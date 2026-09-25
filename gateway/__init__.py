"""Public HTTP gateway for the Vault control plane."""

from .api import build_gateway_router
from .service import GatewayService

__all__ = ["GatewayService", "build_gateway_router"]
