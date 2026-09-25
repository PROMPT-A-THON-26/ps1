"""Integrity scanning and corruption-triggered repair orchestration."""

from .manager import IntegrityManager, IntegrityResult

__all__ = ["IntegrityManager", "IntegrityResult"]
