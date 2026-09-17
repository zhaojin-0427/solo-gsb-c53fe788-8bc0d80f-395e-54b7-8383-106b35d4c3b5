"""Core quota decision logic.

Concurrency & consistency design (all enforced by PostgreSQL, so the app can
run as multiple replicas behind a load balancer):

* Atomic multi-level check/deduct: every decision runs in one transaction and
  locks the three bucket rows with SELECT ... FOR UPDATE in a fixed hierarchy
  order (tenant -> subject -> operation). Concurrent transactions serialize on
  the row locks; the fixed order prevents deadlocks. Buckets are refilled
  lazily inside the lock, so the check+deduct is atomic across all levels.

* Idempotency: the requests table is claimed with
  INSERT ... ON CONFLICT DO NOTHING ... RETURNING. A concurrent duplicate
  blocks until the in-flight transaction commits, then replays the stored
  response. Same request_id with different parameters -> ConflictError (409).

* Exactly-once refund: reservation status transitions
  (active -> committed/cancelled/expired) happen under the reservation row
  lock (SELECT ... FOR UPDATE, or FOR UPDATE SKIP LOCKED in the sweeper).
  A refund is issued only by the transaction that wins the transition out of
  'active', inside the same transaction, so cancel/commit/timeout races can
  never refund twice.

* Policy updates only rewrite policy rows and bucket limits (capacity /
  refill_rate) used by *new* decisions. Existing reservations keep the cost
  they were created with; refunds are capped at the bucket capacity.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid

from sqlalchemy import select
from sqlalchemy import update as sql_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from . import config
from .models import Bucket, Policy, Request, Reservation
from .schemas import PolicyIn, QuotaRequest

logger = logging.getLogger(__name__)

_EPS = 1e-9  # float tolerance when comparing tokens against cost

LEVEL_TENANT = "tenant"
LEVEL_SUBJECT = "subject"
LEVEL_OPERATION = "operation"

STATUS_ACTIVE = "active"
STATUS_COMMITTED = "committed"
STATUS_CANCELLED = "cancelled"
STATUS_EXPIRED = "expired"


class ConflictError(Exception):
    """409 — request_id reused with different params, or invalid state transition."""


class NotFoundError(Exception):
    """404 — unknown resource."""


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _bucket_keys(tenant: str, subject: str, operation: str) -> list[tuple[str, str, str, str]]:
    """The three bucket keys, in lock-acquisition order (coarse -> fine)."""
    return [
        (LEVEL_TENANT, tenant, "", ""),
        (LEVEL_SUBJECT, tenant, subject, ""),
        (LEVEL_OPERATION, tenant, subject, operation),
    ]


def _fingerprint(endpoint: str, req: QuotaRequest) -> str:
    payload = json.dumps(
        {
            "endpoint": endpoint,
            "tenant": req.tenant,
            "subject": req.subject,
            "operation": req.operation,
            "cost": req.cost,
            "ttl_seconds": req.ttl_seconds,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _refill(bucket: Bucket, now: float) -> None:
    """Lazy token-bucket refill; must be called with the bucket row locked."""
    elapsed = now - bucket.last_refill_at
    if elapsed > 0 and bucket.refill_rate > 0:
        bucket.tokens = min(bucket.capacity, bucket.tokens + elapsed * bucket.refill_rate)
    bucket.last_refill_at = now


async def _ensure_policy(
    session: AsyncSession, level: str, tenant: str, subject: str, operation: str
) -> Policy:
    key = (level, tenant, subject, operation)
    policy = await session.get(Policy, key)
    if policy is None:
        # Auto-provision a default policy; ON CONFLICT DO NOTHING blocks until a
        # concurrent creator commits, so exactly one row ever exists per key.
        await session.execute(
            pg_insert(Policy)
            .values(
                level=level,
                tenant=tenant,
                subject=subject,
                operation=operation,
                capacity=config.DEFAULT_CAPACITY,
                refill_rate=config.DEFAULT_REFILL_RATE,
                version=1,
                updated_at=time.time(),
            )
            .on_conflict_do_nothing()
        )
        policy = await session.get(Policy, key)
    return policy


async def _lock_buckets(
    session: AsyncSession, tenant: str, subject: str, operation: str
) -> list[Bucket]:
    """Return the three bucket rows, locked FOR UPDATE in hierarchy order."""
    buckets: list[Bucket] = []
    for level, t, s, o in _bucket_keys(tenant, subject, operation):
        stmt = (
            select(Bucket)
            .where(
                Bucket.level == level,
                Bucket.tenant == t,
                Bucket.subject == s,
                Bucket.operation == o,
            )
            .with_for_update()
        )
        bucket = (await session.execute(stmt)).scalar_one_or_none()
        if bucket is None:
            policy = await _ensure_policy(session, level, t, s, o)
            await session.execute(
                pg_insert(Bucket)
                .values(
                    level=level,
                    tenant=t,
                    subject=s,
                    operation=o,
                    capacity=policy.capacity,
                    refill_rate=policy.refill_rate,
                    tokens=policy.capacity,  # new buckets start full
                    last_refill_at=time.time(),
                    policy_version=policy.version,
                )
                .on_conflict_do_nothing()
            )
            bucket = (await session.execute(stmt)).scalar_one()
        buckets.append(bucket)
    return buckets


async def _claim_request(
    session: AsyncSession, request_id: str, endpoint: str, fingerprint: str
) -> dict | None:
    """Try to claim request_id. Returns the stored response on idempotent
    replay, None if this transaction now owns the request_id.

    Raises ConflictError if the request_id was used with different parameters.
    """
    result = await session.execute(
        pg_insert(Request)
        .values(
            request_id=request_id,
            endpoint=endpoint,
            fingerprint=fingerprint,
            response=None,
            created_at=time.time(),
        )
        .on_conflict_do_nothing()
        .returning(Request.request_id)
    )
    if result.scalar_one_or_none() is not None:
        return None  # we own the request_id; proceed with the real work

    # A row already exists (the INSERT above blocks until any in-flight
    # transaction holding this key commits or rolls back).
    existing = await session.get(Request, request_id)
    if existing is None or existing.fingerprint != fingerprint:
        raise ConflictError("request_id already used with different parameters")
    if existing.response is None:
        # Unreachable in practice (response is written before commit), kept as
        # a defensive guard.
        raise ConflictError("request is still in progress; retry later")
    return existing.response


def _reservation_view(r: Reservation) -> dict:
    return {
        "reservation_id": r.reservation_id,
        "request_id": r.request_id,
        "tenant": r.tenant,
        "subject": r.subject,
        "operation": r.operation,
        "cost": r.cost,
        "status": r.status,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
        "expires_at": r.expires_at,
    }


async def _refund(session: AsyncSession, reservation: Reservation, now: float) -> None:
    """Return the reservation's cost to all three buckets (capped at capacity).

    Must be called in the same transaction that transitions the reservation
    out of 'active' — that transition is what guarantees exactly-once.
    """
    buckets = await _lock_buckets(
        session, reservation.tenant, reservation.subject, reservation.operation
    )
    for bucket in buckets:
        _refill(bucket, now)
        bucket.tokens = min(bucket.capacity, bucket.tokens + reservation.cost)


# --------------------------------------------------------------------------- #
# check / reserve
# --------------------------------------------------------------------------- #

async def decide(session: AsyncSession, endpoint: str, req: QuotaRequest) -> dict:
    """Atomic three-level token-bucket decision.

    endpoint='check'   -> dry run, never deducts.
    endpoint='reserve' -> deducts immediately and creates a reservation.
    """
    fingerprint = _fingerprint(endpoint, req)
    replayed = await _claim_request(session, req.request_id, endpoint, fingerprint)
    if replayed is not None:
        return replayed

    now = time.time()
    buckets = await _lock_buckets(session, req.tenant, req.subject, req.operation)
    for bucket in buckets:
        _refill(bucket, now)

    insufficient: list[str] = []
    retry_after = 0.0
    impossible = False
    for bucket in buckets:
        if bucket.tokens + _EPS < req.cost:
            insufficient.append(bucket.level)
            if bucket.refill_rate > 0 and req.cost <= bucket.capacity:
                wait = (req.cost - bucket.tokens) / bucket.refill_rate
                retry_after = max(retry_after, wait)
            else:
                # refill disabled, or cost can never fit into the bucket
                impossible = True

    if insufficient:
        response = {
            "request_id": req.request_id,
            "decision": "rejected",
            "insufficient": insufficient,
            # Longest wait across all insufficient levels; null means the
            # request can never succeed under the current policy.
            "retry_after": None if impossible else round(retry_after, 3),
        }
    else:
        response = {"request_id": req.request_id, "decision": "approved"}
        if endpoint == "reserve":
            # Only reserve deducts; check is a pure dry run.
            for bucket in buckets:
                bucket.tokens = max(0.0, bucket.tokens - req.cost)
            ttl = req.ttl_seconds
            if ttl is None:
                ttl = config.RESERVATION_TTL_SECONDS
            ttl = min(ttl, config.RESERVATION_TTL_SECONDS)
            reservation = Reservation(
                reservation_id=str(uuid.uuid4()),
                request_id=req.request_id,
                tenant=req.tenant,
                subject=req.subject,
                operation=req.operation,
                cost=req.cost,
                status=STATUS_ACTIVE,
                created_at=now,
                updated_at=now,
                expires_at=now + ttl,
            )
            session.add(reservation)
            response.update(
                {
                    "reservation_id": reservation.reservation_id,
                    "expires_at": reservation.expires_at,
                    "ttl_seconds": ttl,
                }
            )

    # Persist the response atomically with the deduction, so a replayed
    # request_id always returns the exact same result.
    await session.execute(
        sql_update(Request)
        .where(Request.request_id == req.request_id)
        .values(response=response)
    )
    await session.commit()
    return response


# --------------------------------------------------------------------------- #
# commit / cancel
# --------------------------------------------------------------------------- #

async def _get_reservation_for_update(
    session: AsyncSession, reservation_id: str
) -> Reservation:
    stmt = (
        select(Reservation)
        .where(Reservation.reservation_id == reservation_id)
        .with_for_update()
    )
    reservation = (await session.execute(stmt)).scalar_one_or_none()
    if reservation is None:
        raise NotFoundError(f"reservation {reservation_id} not found")
    return reservation


async def commit_reservation(session: AsyncSession, reservation_id: str) -> dict:
    """Commit a reservation. Never deducts (reserve already did). Idempotent."""
    reservation = await _get_reservation_for_update(session, reservation_id)
    now = time.time()

    if reservation.status == STATUS_COMMITTED:
        return _reservation_view(reservation)  # duplicate commit -> same result

    if reservation.status == STATUS_ACTIVE:
        if reservation.expires_at <= now:
            # Lost the race against the timeout: reclaim instead of committing.
            reservation.status = STATUS_EXPIRED
            reservation.updated_at = now
            await _refund(session, reservation, now)
            await session.commit()
            raise ConflictError("reservation already expired")
        reservation.status = STATUS_COMMITTED
        reservation.updated_at = now
        await session.commit()
        return _reservation_view(reservation)

    # Terminal state (cancelled/expired): invalid transition. Nothing was
    # modified, so no rollback is needed here; the endpoint rolls back.
    raise ConflictError(f"reservation already {reservation.status}")


async def cancel_reservation(session: AsyncSession, reservation_id: str) -> dict:
    """Cancel a reservation and refund once. Idempotent for repeated cancels."""
    reservation = await _get_reservation_for_update(session, reservation_id)
    now = time.time()

    if reservation.status == STATUS_CANCELLED:
        return _reservation_view(reservation)  # duplicate cancel -> no double refund

    if reservation.status == STATUS_ACTIVE:
        # If it is past its TTL but the sweeper has not reclaimed it yet, this
        # transition still wins the row lock and refunds exactly once.
        reservation.status = STATUS_CANCELLED
        reservation.updated_at = now
        await _refund(session, reservation, now)
        await session.commit()
        return _reservation_view(reservation)

    # Terminal state (committed/expired): invalid transition.
    raise ConflictError(f"reservation already {reservation.status}")


async def get_reservation(session: AsyncSession, reservation_id: str) -> dict:
    reservation = await session.get(Reservation, reservation_id)
    if reservation is None:
        raise NotFoundError(f"reservation {reservation_id} not found")
    return _reservation_view(reservation)


# --------------------------------------------------------------------------- #
# expiry sweeper
# --------------------------------------------------------------------------- #

async def sweep_expired(session: AsyncSession) -> int:
    """Reclaim active reservations past their TTL, refunding each exactly once.

    Safe to run concurrently on multiple replicas: SKIP LOCKED makes each
    expired row be processed by exactly one sweeper.
    """
    now = time.time()
    stmt = (
        select(Reservation.reservation_id)
        .where(Reservation.status == STATUS_ACTIVE, Reservation.expires_at <= now)
        .order_by(Reservation.expires_at)
        .limit(config.SWEEP_BATCH_SIZE)
        .with_for_update(skip_locked=True)
    )
    ids = (await session.execute(stmt)).scalars().all()
    for reservation_id in ids:
        reservation = await session.get(Reservation, reservation_id)
        reservation.status = STATUS_EXPIRED
        reservation.updated_at = now
        await _refund(session, reservation, now)
    await session.commit()
    return len(ids)


# --------------------------------------------------------------------------- #
# policies & observability
# --------------------------------------------------------------------------- #

async def upsert_policy(session: AsyncSession, p: PolicyIn) -> dict:
    """Create or replace a policy. Affects only new decisions; existing
    reservations keep the cost they were created with."""
    key_where = (
        Policy.level == p.level,
        Policy.tenant == p.tenant,
        Policy.subject == p.subject,
        Policy.operation == p.operation,
    )
    now = time.time()
    stmt = select(Policy).where(*key_where).with_for_update()
    policy = (await session.execute(stmt)).scalar_one_or_none()
    if policy is None:
        result = await session.execute(
            pg_insert(Policy)
            .values(
                level=p.level,
                tenant=p.tenant,
                subject=p.subject,
                operation=p.operation,
                capacity=p.capacity,
                refill_rate=p.refill_rate,
                version=1,
                updated_at=now,
            )
            .on_conflict_do_nothing()
            .returning(Policy.level)
        )
        inserted = result.scalar_one_or_none() is not None
        policy = (await session.execute(stmt)).scalar_one()
        if not inserted:
            # Lost a concurrent create race: our values win (last writer wins).
            policy.capacity = p.capacity
            policy.refill_rate = p.refill_rate
            policy.version += 1
            policy.updated_at = now
    else:
        policy.capacity = p.capacity
        policy.refill_rate = p.refill_rate
        policy.version += 1
        policy.updated_at = now

    # Propagate limits to the live bucket so NEW decisions use them. Token
    # balance is preserved (clamped to the new capacity on the next refill).
    await session.execute(
        sql_update(Bucket)
        .where(
            Bucket.level == p.level,
            Bucket.tenant == p.tenant,
            Bucket.subject == p.subject,
            Bucket.operation == p.operation,
        )
        .values(
            capacity=p.capacity,
            refill_rate=p.refill_rate,
            policy_version=policy.version,
        )
    )
    await session.commit()
    return {
        "level": p.level,
        "tenant": p.tenant,
        "subject": p.subject,
        "operation": p.operation,
        "capacity": p.capacity,
        "refill_rate": p.refill_rate,
        "version": policy.version,
        "updated_at": policy.updated_at,
    }


async def list_policies(session: AsyncSession) -> list[dict]:
    rows = (await session.execute(select(Policy).order_by(Policy.tenant, Policy.level))).scalars().all()
    return [
        {
            "level": p.level,
            "tenant": p.tenant,
            "subject": p.subject,
            "operation": p.operation,
            "capacity": p.capacity,
            "refill_rate": p.refill_rate,
            "version": p.version,
            "updated_at": p.updated_at,
        }
        for p in rows
    ]


async def list_buckets(session: AsyncSession, tenant: str | None = None) -> list[dict]:
    stmt = select(Bucket).order_by(Bucket.tenant, Bucket.level)
    if tenant is not None:
        stmt = stmt.where(Bucket.tenant == tenant)
    rows = (await session.execute(stmt)).scalars().all()
    now = time.time()
    return [
        {
            "level": b.level,
            "tenant": b.tenant,
            "subject": b.subject,
            "operation": b.operation,
            "capacity": b.capacity,
            "refill_rate": b.refill_rate,
            # Effective balance right now (lazy refill applied read-only).
            "tokens": min(
                b.capacity,
                b.tokens + max(0.0, now - b.last_refill_at) * b.refill_rate,
            ),
            "policy_version": b.policy_version,
        }
        for b in rows
    ]
