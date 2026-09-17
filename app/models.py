"""Database schema.

Three-level token buckets are keyed by (level, tenant, subject, operation):

    level='tenant'    -> (tenant, '', '')
    level='subject'   -> (tenant, subject, '')
    level='operation' -> (tenant, subject, operation)

Bucket rows hold both the effective limits (copied from / refreshed by the
policy of the same key) and the mutable token state. Reservations store the
deducted cost so that a later policy change never rewrites them.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import JSON, CheckConstraint, Float, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Bucket(Base):
    __tablename__ = "buckets"

    level: Mapped[str] = mapped_column(String(16), primary_key=True)
    tenant: Mapped[str] = mapped_column(String(128), primary_key=True)
    subject: Mapped[str] = mapped_column(String(128), primary_key=True)
    operation: Mapped[str] = mapped_column(String(128), primary_key=True)

    capacity: Mapped[float] = mapped_column(Float, nullable=False)
    refill_rate: Mapped[float] = mapped_column(Float, nullable=False)  # tokens/sec
    tokens: Mapped[float] = mapped_column(Float, nullable=False)
    last_refill_at: Mapped[float] = mapped_column(Float, nullable=False)  # epoch seconds
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class Policy(Base):
    __tablename__ = "policies"

    level: Mapped[str] = mapped_column(String(16), primary_key=True)
    tenant: Mapped[str] = mapped_column(String(128), primary_key=True)
    subject: Mapped[str] = mapped_column(String(128), primary_key=True)
    operation: Mapped[str] = mapped_column(String(128), primary_key=True)

    capacity: Mapped[float] = mapped_column(Float, nullable=False)
    refill_rate: Mapped[float] = mapped_column(Float, nullable=False)  # tokens/sec
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[float] = mapped_column(Float, nullable=False)


class Request(Base):
    """Idempotency record: one row per request_id, storing the final response."""

    __tablename__ = "requests"

    request_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(16), nullable=False)  # check | reserve
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    response: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[float] = mapped_column(Float, nullable=False)


class Reservation(Base):
    __tablename__ = "reservations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active','committed','cancelled','expired')",
            name="ck_reservations_status",
        ),
        Index("ix_reservations_status_expires", "status", "expires_at"),
    )

    reservation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    tenant: Mapped[str] = mapped_column(String(128), nullable=False)
    subject: Mapped[str] = mapped_column(String(128), nullable=False)
    operation: Mapped[str] = mapped_column(String(128), nullable=False)
    cost: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[float] = mapped_column(Float, nullable=False)
    updated_at: Mapped[float] = mapped_column(Float, nullable=False)
    expires_at: Mapped[float] = mapped_column(Float, nullable=False)
