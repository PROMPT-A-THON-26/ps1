"""Domain exceptions used by the control plane."""

from __future__ import annotations

from dataclasses import dataclass

from .constants import ErrorCode


@dataclass(slots=True)
class VaultError(Exception):
    code: ErrorCode
    message: str
    status_code: int = 500

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class ObjectNotFound(VaultError):
    def __init__(self, name: str) -> None:
        super().__init__(
            code=ErrorCode.OBJECT_NOT_FOUND,
            message=f"Object '{name}' was not found.",
            status_code=404,
        )


class ObjectAlreadyExists(VaultError):
    def __init__(self, name: str) -> None:
        super().__init__(
            code=ErrorCode.OBJECT_ALREADY_EXISTS,
            message=f"Object '{name}' already exists.",
            status_code=409,
        )


class VersionConflict(VaultError):
    def __init__(self, expected: int | None, actual: int | None) -> None:
        expected_text = "none" if expected is None else str(expected)
        actual_text = "none" if actual is None else str(actual)
        super().__init__(
            code=ErrorCode.VERSION_CONFLICT,
            message=(
                "Expected current version "
                f"{expected_text}, but current version is {actual_text}."
            ),
            status_code=409,
        )


class InvalidState(VaultError):
    def __init__(self, message: str) -> None:
        super().__init__(code=ErrorCode.INVALID_REQUEST, message=message, status_code=409)


class ChecksumMismatch(VaultError):
    def __init__(self, expected: str, actual: str) -> None:
        super().__init__(
            code=ErrorCode.CHECKSUM_MISMATCH,
            message=f"Checksum mismatch: expected {expected}, got {actual}.",
            status_code=409,
        )
