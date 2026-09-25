"""Node registry, heartbeat processing, and failure detection for Vault."""

from .failure_detector import FailureDetector, NodeTransition
from .heartbeat import HeartbeatPayload, HeartbeatResult, HeartbeatService
from .node_registry import NodeRegistry
from .scheduler import HealthScheduler

__all__ = [
    "FailureDetector",
    "HeartbeatPayload",
    "HeartbeatResult",
    "HeartbeatService",
    "HealthScheduler",
    "NodeRegistry",
    "NodeTransition",
]
