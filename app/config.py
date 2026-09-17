"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://quota:quota@localhost:5432/quota",
)

# Default token-bucket parameters applied to levels that have no explicit policy.
DEFAULT_CAPACITY = float(os.getenv("DEFAULT_CAPACITY", "1000"))
DEFAULT_REFILL_RATE = float(os.getenv("DEFAULT_REFILL_RATE", "100"))  # tokens / second

# Reservation TTL: an active reservation that is not committed within this
# window is reclaimed and its cost refunded exactly once. Also the upper bound
# for the per-request ttl_seconds override.
RESERVATION_TTL_SECONDS = float(os.getenv("RESERVATION_TTL_SECONDS", "60"))

# How often the background sweeper reclaims expired reservations.
SWEEP_INTERVAL_SECONDS = float(os.getenv("SWEEP_INTERVAL_SECONDS", "0.5"))
SWEEP_BATCH_SIZE = int(os.getenv("SWEEP_BATCH_SIZE", "100"))
