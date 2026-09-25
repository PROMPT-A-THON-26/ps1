"""Stable identifier helpers."""

from __future__ import annotations

from uuid import UUID, uuid4


def new_uuid() -> UUID:
    """Return a new UUID suitable for object/version/replica/job identity."""
    return uuid4()


def new_request_id() -> str:
    """Return a traceable request identifier for gateway/internal calls."""
    return f"req_{uuid4().hex}"
