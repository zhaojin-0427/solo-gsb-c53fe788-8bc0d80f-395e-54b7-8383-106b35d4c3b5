"""Request/response schemas for the HTTP API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class QuotaRequest(BaseModel):
    """Body of POST /v1/quota/check and POST /v1/quota/reserve."""

    tenant: str = Field(min_length=1, max_length=128)
    subject: str = Field(min_length=1, max_length=128)
    operation: str = Field(min_length=1, max_length=128)
    cost: float = Field(gt=0)
    request_id: str = Field(min_length=1, max_length=128)
    # Optional per-reservation TTL (seconds). Capped at the server-side
    # RESERVATION_TTL_SECONDS. Only meaningful for /v1/quota/reserve.
    ttl_seconds: float | None = Field(default=None, gt=0)


class ReservationAction(BaseModel):
    """Body of POST /v1/quota/commit and POST /v1/quota/cancel."""

    reservation_id: str = Field(min_length=1, max_length=36)


class PolicyIn(BaseModel):
    """Body of PUT /v1/policies."""

    level: Literal["tenant", "subject", "operation"]
    tenant: str = Field(min_length=1, max_length=128)
    subject: str = Field(default="", max_length=128)
    operation: str = Field(default="", max_length=128)
    capacity: float = Field(gt=0)
    refill_rate: float = Field(ge=0)  # tokens/sec; 0 means "never refills"

    @model_validator(mode="after")
    def _check_key_shape(self) -> "PolicyIn":
        if self.level == "tenant":
            if self.subject or self.operation:
                raise ValueError("tenant-level policy must not set subject/operation")
        elif self.level == "subject":
            if not self.subject:
                raise ValueError("subject-level policy requires subject")
            if self.operation:
                raise ValueError("subject-level policy must not set operation")
        else:  # operation
            if not self.subject or not self.operation:
                raise ValueError("operation-level policy requires subject and operation")
        return self
