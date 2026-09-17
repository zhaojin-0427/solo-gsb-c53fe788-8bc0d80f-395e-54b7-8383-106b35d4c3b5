"""HTTP layer: FastAPI app, schema init on startup, background expiry sweeper."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text

from . import config, service
from .db import Base, SessionLocal, engine
from .schemas import PolicyIn, QuotaRequest, ReservationAction
from .service import ConflictError, NotFoundError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("quota")


async def _init_db() -> None:
    for attempt in range(1, 31):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("database schema ready")
            return
        except Exception:
            logger.warning("database not ready (attempt %d/30)", attempt, exc_info=True)
            await asyncio.sleep(1)
    raise RuntimeError("database is not reachable")


async def _sweeper_loop() -> None:
    """Periodically reclaim expired reservations (timeout -> refund once)."""
    while True:
        try:
            async with SessionLocal() as session:
                expired = await service.sweep_expired(session)
                if expired:
                    logger.info("sweeper reclaimed %d expired reservation(s)", expired)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sweeper iteration failed")
        await asyncio.sleep(config.SWEEP_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _init_db()
    sweeper = asyncio.create_task(_sweeper_loop())
    try:
        yield
    finally:
        sweeper.cancel()
        try:
            await sweeper
        except asyncio.CancelledError:
            pass
        await engine.dispose()


app = FastAPI(
    title="Distributed Quota Decision API",
    version="1.0.0",
    lifespan=lifespan,
)


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, NotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    raise HTTPException(status_code=409, detail=str(exc))


# --------------------------------------------------------------------------- #
# quota decisions
# --------------------------------------------------------------------------- #

@app.post("/v1/quota/check")
async def check_quota(req: QuotaRequest):
    """Dry-run decision: atomically checks the three-level token buckets
    without deducting. Rejections list every insufficient level plus the
    longest retry_after."""
    async with SessionLocal() as session:
        try:
            return await service.decide(session, "check", req)
        except (ConflictError, NotFoundError) as exc:
            await session.rollback()
            _raise_http(exc)


@app.post("/v1/quota/reserve")
async def reserve_quota(req: QuotaRequest):
    """Reserve quota: same atomic check, and on approval deducts immediately
    and returns a reservation that must later be committed or cancelled."""
    async with SessionLocal() as session:
        try:
            return await service.decide(session, "reserve", req)
        except (ConflictError, NotFoundError) as exc:
            await session.rollback()
            _raise_http(exc)


@app.post("/v1/quota/commit")
async def commit_quota(action: ReservationAction):
    """Commit a reservation (no further deduction). Safe to retry."""
    async with SessionLocal() as session:
        try:
            return await service.commit_reservation(session, action.reservation_id)
        except (ConflictError, NotFoundError) as exc:
            await session.rollback()
            _raise_http(exc)


@app.post("/v1/quota/cancel")
async def cancel_quota(action: ReservationAction):
    """Cancel a reservation, refunding its cost exactly once. Safe to retry."""
    async with SessionLocal() as session:
        try:
            return await service.cancel_reservation(session, action.reservation_id)
        except (ConflictError, NotFoundError) as exc:
            await session.rollback()
            _raise_http(exc)


@app.get("/v1/reservations/{reservation_id}")
async def get_reservation(reservation_id: str):
    async with SessionLocal() as session:
        try:
            return await service.get_reservation(session, reservation_id)
        except NotFoundError as exc:
            _raise_http(exc)


# --------------------------------------------------------------------------- #
# policies & observability
# --------------------------------------------------------------------------- #

@app.put("/v1/policies")
async def put_policy(policy: PolicyIn):
    """Create or replace a bucket policy. Applies only to new decisions;
    existing reservations are never rewritten."""
    async with SessionLocal() as session:
        return await service.upsert_policy(session, policy)


@app.get("/v1/policies")
async def get_policies():
    async with SessionLocal() as session:
        return {"policies": await service.list_policies(session)}


@app.get("/v1/buckets")
async def get_buckets(tenant: str | None = Query(default=None)):
    """Current bucket states (observability; lazy refill applied read-only)."""
    async with SessionLocal() as session:
        return {"buckets": await service.list_buckets(session, tenant)}


@app.get("/healthz")
async def healthz():
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover - depends on infra failure
        return JSONResponse(
            status_code=503, content={"status": "unavailable", "detail": str(exc)}
        )
