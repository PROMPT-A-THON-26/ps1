"""Shared input validation rules for public domain identifiers."""
from __future__ import annotations

MAX_OBJECT_NAME_LENGTH = 255


def normalize_object_name(value: str) -> str:
    """Return a safe canonical object name for metadata operations."""
    if not isinstance(value, str):
        raise ValueError("object name must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError("object name must not be empty")
    if len(normalized) > MAX_OBJECT_NAME_LENGTH:
        raise ValueError(
            f"object name must not exceed {MAX_OBJECT_NAME_LENGTH} characters"
        )
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in normalized):
        raise ValueError("object name must not contain control characters")
    return normalized
