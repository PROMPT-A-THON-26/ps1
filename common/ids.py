"""Stable identifier helpers."""

from __future__ import annotations

import re
from uuid import UUID, uuid4

REQUEST_ID_PATTERN = r"^[A-Za-z0-9_.:-]{1,64}$"
_REQUEST_ID_RE = re.compile(REQUEST_ID_PATTERN)


def new_uuid() -> UUID:
    """Return a new UUID suitable for object/version/replica/job identity."""
    return uuid4()


def new_request_id() -> str:
    """Return a traceable request identifier for gateway/internal calls."""
    return f"req_{uuid4().hex}"


def normalize_request_id(value: str | None) -> str:
    """Accept only short, log-safe request IDs; otherwise create a fresh ID."""
    candidate = value.strip() if isinstance(value, str) else ""
    return candidate if _REQUEST_ID_RE.fullmatch(candidate) else new_request_id()
