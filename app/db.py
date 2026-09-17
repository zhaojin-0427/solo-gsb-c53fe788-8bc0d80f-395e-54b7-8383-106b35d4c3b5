"""Async SQLAlchemy engine / session factory."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from . import config
from .models import Base

engine = create_async_engine(
    config.DATABASE_URL,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

__all__ = ["engine", "SessionLocal", "Base"]
