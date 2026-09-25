"""Celery background tasks for Vault control-plane work."""

from .celery_app import celery_app

__all__ = ["celery_app"]
