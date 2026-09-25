"""Durable automatic repair services for Vault replicas."""

from .manager import RepairManager, RepairResult

__all__ = ["RepairManager", "RepairResult"]
