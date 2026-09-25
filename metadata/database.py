"""Database configuration and schema helpers for Vault metadata."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Callable, TypeVar

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from common.settings import settings

T = TypeVar("T")


class Base(DeclarativeBase):
    """Base class for all control-plane SQLAlchemy models."""


def normalize_database_url(url: str) -> str:
    """Normalize common PostgreSQL URLs to the psycopg SQLAlchemy dialect."""
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def build_engine(database_url: str, *, echo: bool = False):
    """Create a SQLAlchemy engine with connection liveness checks."""
    return create_engine(
        normalize_database_url(database_url),
        echo=echo,
        pool_pre_ping=True,
    )


DATABASE_URL = normalize_database_url(settings.database_url)
engine = build_engine(DATABASE_URL, echo=os.getenv("SQL_ECHO", "0") == "1")
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


@contextmanager
def session_scope(
    session_factory: sessionmaker[Session] = SessionLocal,
) -> Iterator[Session]:
    """Yield a session and guarantee rollback on exceptions."""
    with session_factory() as session:
        try:
            yield session
        except Exception:
            session.rollback()
            raise


def create_schema(db_engine=engine) -> None:
    """Create all metadata tables for local development and tests."""
    from . import models  # noqa: F401

    Base.metadata.create_all(db_engine)


def drop_schema(db_engine=engine) -> None:
    """Drop all metadata tables for isolated local tests."""
    from . import models  # noqa: F401

    Base.metadata.drop_all(db_engine)
