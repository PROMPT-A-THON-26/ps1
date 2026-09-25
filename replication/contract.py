"""Canonical Part A <-> Part B storage-node HTTP contract."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StorageNodeContract:
    put: str = "/internal/v1/objects/{object_id}/{version_id}"
    get: str = "/internal/v1/objects/{object_id}/{version_id}"
    head: str = "/internal/v1/objects/{object_id}/{version_id}"
    delete: str = "/internal/v1/objects/{object_id}/{version_id}"
    verify: str = "/internal/v1/objects/{object_id}/{version_id}/verify"
    health: str = "/internal/v1/health"
    stats: str = "/internal/v1/stats"


CANONICAL_STORAGE_NODE_CONTRACT = StorageNodeContract()
