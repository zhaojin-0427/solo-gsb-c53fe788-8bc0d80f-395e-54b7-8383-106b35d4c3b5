"""Integration tests for the quota decision API.

Run against a live stack:

    docker compose up --build -d
    pip install -r requirements-dev.txt
    BASE_URL=http://localhost:8000 pytest tests/ -v
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

BASE = os.getenv("BASE_URL", "http://localhost:8000")


@pytest.fixture()
def client():
    with httpx.Client(base_url=BASE, timeout=15) as c:
        yield c


def tid() -> str:
    return "t-" + uuid.uuid4().hex[:12]


def put_policy(client, level, tenant, capacity, rate, subject="", operation=""):
    r = client.put(
        "/v1/policies",
        json={
            "level": level,
            "tenant": tenant,
            "subject": subject,
            "operation": operation,
            "capacity": capacity,
            "refill_rate": rate,
        },
    )
    assert r.status_code == 200, r.text
    return r.json()


def bucket_tokens(client, tenant, level="tenant", subject="", operation=""):
    r = client.get("/v1/buckets", params={"tenant": tenant})
    assert r.status_code == 200, r.text
    for b in r.json()["buckets"]:
        if (
            b["level"] == level
            and b["subject"] == subject
            and b["operation"] == operation
        ):
            return b["tokens"]
    raise AssertionError(f"bucket {level}/{tenant}/{subject}/{operation} not found")


def reserve(client, tenant, cost, request_id=None, subject="s", operation="op", ttl=None):
    body = {
        "tenant": tenant,
        "subject": subject,
        "operation": operation,
        "cost": cost,
        "request_id": request_id or ("req-" + uuid.uuid4().hex),
    }
    if ttl is not None:
        body["ttl_seconds"] = ttl
    r = client.post("/v1/quota/reserve", json=body)
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------------------------------- #
# basic flow
# --------------------------------------------------------------------------- #

def test_health(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_check_does_not_deduct(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)
    body = {
        "tenant": tenant,
        "subject": "s",
        "operation": "op",
        "cost": 4,
        "request_id": "chk-" + uuid.uuid4().hex,
    }
    for _ in range(2):
        r = client.post("/v1/quota/check", json=body)
        assert r.status_code == 200
        assert r.json()["decision"] == "approved"
    assert bucket_tokens(client, tenant) == pytest.approx(10.0)


def test_reserve_deducts_commit_does_not(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)

    res = reserve(client, tenant, cost=4)
    assert res["decision"] == "approved"
    assert bucket_tokens(client, tenant) == pytest.approx(6.0)

    rid = res["reservation_id"]
    r1 = client.post("/v1/quota/commit", json={"reservation_id": rid})
    assert r1.status_code == 200 and r1.json()["status"] == "committed"
    assert bucket_tokens(client, tenant) == pytest.approx(6.0)  # no second deduction

    # duplicate commit is idempotent
    r2 = client.post("/v1/quota/commit", json={"reservation_id": rid})
    assert r2.status_code == 200 and r2.json()["status"] == "committed"
    assert bucket_tokens(client, tenant) == pytest.approx(6.0)

    # cancel after commit is rejected
    r3 = client.post("/v1/quota/cancel", json={"reservation_id": rid})
    assert r3.status_code == 409


def test_reject_reports_all_insufficient_levels_and_longest_retry(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=1)
    put_policy(client, "subject", tenant, capacity=10, rate=2, subject="s")
    put_policy(client, "operation", tenant, capacity=10, rate=4, subject="s", operation="op")

    # drain all three levels to 3 tokens each
    drain = reserve(client, tenant, cost=7)
    assert drain["decision"] == "approved"

    body = {
        "tenant": tenant,
        "subject": "s",
        "operation": "op",
        "cost": 5,
        "request_id": "rej-" + uuid.uuid4().hex,
    }
    r = client.post("/v1/quota/check", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["decision"] == "rejected"
    assert data["insufficient"] == ["tenant", "subject", "operation"]
    # per-level waits: (5-3)/1=2, (5-3)/2=1, (5-3)/4=0.5 -> longest is ~2s
    assert data["retry_after"] == pytest.approx(2.0, abs=0.5)
    # the check itself deducted nothing further
    assert bucket_tokens(client, tenant, "operation", "s", "op") == pytest.approx(3.0, abs=0.5)


def test_reject_impossible_when_cost_exceeds_capacity(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=5, rate=0)
    body = {
        "tenant": tenant,
        "subject": "s",
        "operation": "op",
        "cost": 50,
        "request_id": "imp-" + uuid.uuid4().hex,
    }
    r = client.post("/v1/quota/reserve", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["decision"] == "rejected"
    assert data["retry_after"] is None  # can never succeed under current policy


# --------------------------------------------------------------------------- #
# idempotency
# --------------------------------------------------------------------------- #

def test_same_request_id_same_params_replays(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)
    request_id = "idem-" + uuid.uuid4().hex

    r1 = reserve(client, tenant, cost=4, request_id=request_id)
    r2 = reserve(client, tenant, cost=4, request_id=request_id)
    assert r1 == r2  # identical response, same reservation_id
    assert bucket_tokens(client, tenant) == pytest.approx(6.0)  # deducted once


def test_same_request_id_different_params_conflicts(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)
    request_id = "conf-" + uuid.uuid4().hex

    reserve(client, tenant, cost=4, request_id=request_id)
    r = client.post(
        "/v1/quota/reserve",
        json={
            "tenant": tenant,
            "subject": "s",
            "operation": "op",
            "cost": 5,  # changed parameter
            "request_id": request_id,
        },
    )
    assert r.status_code == 409


def test_rejected_decision_is_replayed(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=5, rate=0)
    request_id = "rejr-" + uuid.uuid4().hex

    r1 = reserve(client, tenant, cost=50, request_id=request_id)
    assert r1["decision"] == "rejected"

    # capacity raised afterwards: replay must still return the stored decision
    put_policy(client, "tenant", tenant, capacity=1000, rate=0)
    r2 = reserve(client, tenant, cost=50, request_id=request_id)
    assert r2 == r1


def test_request_id_scoped_to_endpoint(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)
    request_id = "scope-" + uuid.uuid4().hex
    body = {
        "tenant": tenant,
        "subject": "s",
        "operation": "op",
        "cost": 4,
        "request_id": request_id,
    }
    r1 = client.post("/v1/quota/check", json=body)
    assert r1.status_code == 200
    # same request_id on a different endpoint is a parameter change -> 409
    r2 = client.post("/v1/quota/reserve", json=body)
    assert r2.status_code == 409


# --------------------------------------------------------------------------- #
# cancel / expiry: refund exactly once
# --------------------------------------------------------------------------- #

def test_cancel_refunds_exactly_once(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)

    res = reserve(client, tenant, cost=6)
    assert bucket_tokens(client, tenant) == pytest.approx(4.0)
    rid = res["reservation_id"]

    r1 = client.post("/v1/quota/cancel", json={"reservation_id": rid})
    assert r1.status_code == 200 and r1.json()["status"] == "cancelled"
    assert bucket_tokens(client, tenant) == pytest.approx(10.0)

    # duplicate cancel: idempotent, no second refund
    r2 = client.post("/v1/quota/cancel", json={"reservation_id": rid})
    assert r2.status_code == 200 and r2.json()["status"] == "cancelled"
    assert bucket_tokens(client, tenant) == pytest.approx(10.0)

    # commit after cancel is rejected
    r3 = client.post("/v1/quota/commit", json={"reservation_id": rid})
    assert r3.status_code == 409


def test_expiry_refunds_exactly_once(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)

    res = reserve(client, tenant, cost=6, ttl=0.3)
    rid = res["reservation_id"]
    assert bucket_tokens(client, tenant) == pytest.approx(4.0)

    # wait for the sweeper to reclaim it
    deadline = time.time() + 10
    status = None
    while time.time() < deadline:
        r = client.get(f"/v1/reservations/{rid}")
        assert r.status_code == 200
        status = r.json()["status"]
        if status == "expired":
            break
        time.sleep(0.2)
    assert status == "expired"
    assert bucket_tokens(client, tenant) == pytest.approx(10.0)

    # cancel/commit after expiry: conflict, and no double refund
    assert client.post("/v1/quota/cancel", json={"reservation_id": rid}).status_code == 409
    assert client.post("/v1/quota/commit", json={"reservation_id": rid}).status_code == 409
    assert bucket_tokens(client, tenant) == pytest.approx(10.0)


def test_commit_cancel_race_settles_exactly_once(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=10, rate=0)
    res = reserve(client, tenant, cost=6)
    rid = res["reservation_id"]

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_commit = ex.submit(client.post, "/v1/quota/commit", json={"reservation_id": rid})
        f_cancel = ex.submit(client.post, "/v1/quota/cancel", json={"reservation_id": rid})
        results = [f_commit.result(), f_cancel.result()]

    assert sorted(r.status_code for r in results) == [200, 409]
    final = client.get(f"/v1/reservations/{rid}").json()["status"]
    if final == "committed":
        assert bucket_tokens(client, tenant) == pytest.approx(4.0)
    else:
        assert final == "cancelled"
        assert bucket_tokens(client, tenant) == pytest.approx(10.0)


# --------------------------------------------------------------------------- #
# concurrency
# --------------------------------------------------------------------------- #

def test_no_oversell_under_concurrency(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=100, rate=0)

    async def blast():
        async with httpx.AsyncClient(base_url=BASE, timeout=30) as ac:
            tasks = [
                ac.post(
                    "/v1/quota/reserve",
                    json={
                        "tenant": tenant,
                        "subject": "s",
                        "operation": "op",
                        "cost": 3,
                        "request_id": f"{tenant}-{i}",
                    },
                )
                for i in range(50)
            ]
            return await asyncio.gather(*tasks)

    responses = asyncio.run(blast())
    assert all(r.status_code == 200 for r in responses)
    approved = [r for r in responses if r.json()["decision"] == "approved"]
    rejected = [r for r in responses if r.json()["decision"] == "rejected"]
    # floor(100 / 3) = 33 approvals, 1 token left, never negative
    assert len(approved) == 33
    assert len(rejected) == 17
    assert bucket_tokens(client, tenant) == pytest.approx(1.0)


def test_concurrent_duplicates_return_same_result(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=100, rate=0)
    request_id = "dup-" + uuid.uuid4().hex
    body = {
        "tenant": tenant,
        "subject": "s",
        "operation": "op",
        "cost": 7,
        "request_id": request_id,
    }

    async def blast():
        async with httpx.AsyncClient(base_url=BASE, timeout=30) as ac:
            return await asyncio.gather(
                *[ac.post("/v1/quota/reserve", json=body) for _ in range(20)]
            )

    responses = asyncio.run(blast())
    payloads = [r.json() for r in responses]
    assert all(r.status_code == 200 for r in responses)
    assert all(p == payloads[0] for p in payloads)  # identical replay
    assert payloads[0]["decision"] == "approved"
    assert bucket_tokens(client, tenant) == pytest.approx(93.0)  # deducted once


# --------------------------------------------------------------------------- #
# policy updates
# --------------------------------------------------------------------------- #

def test_policy_update_affects_new_requests_only(client):
    tenant = tid()
    put_policy(client, "tenant", tenant, capacity=5, rate=0)

    r1 = reserve(client, tenant, cost=3)
    assert r1["decision"] == "approved"

    # impossible under the old policy (cost > capacity)
    too_big = reserve(client, tenant, cost=50)
    assert too_big["decision"] == "rejected"
    assert too_big["retry_after"] is None

    # raise the limits: new requests are evaluated with the new policy
    put_policy(client, "tenant", tenant, capacity=100, rate=50)
    time.sleep(1.0)  # let the bucket refill under the new rate
    r3 = reserve(client, tenant, cost=50)
    assert r3["decision"] == "approved"

    # the pre-existing reservation is untouched and still committable
    r = client.post("/v1/quota/commit", json={"reservation_id": r1["reservation_id"]})
    assert r.status_code == 200 and r.json()["status"] == "committed"
    assert r.json()["cost"] == 3
