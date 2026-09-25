from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from .app_factory import create_storage_node_app
from .config import StorageNodeConfig
from .node_lifecycle import NodeLifecycle
from .node_state import SQLiteNodeStateStore
from .storage_engine import StorageEngine

config = StorageNodeConfig.from_env()
engine = StorageEngine(config.data_dir, config.capacity_bytes, config.chunk_size_bytes)
lifecycle = NodeLifecycle()
node_state_store: SQLiteNodeStateStore | None = None


@asynccontextmanager
async def lifespan(_: object) -> AsyncIterator[None]:
    global node_state_store

    config.data_dir.mkdir(parents=True, exist_ok=True)
    node_state_store = SQLiteNodeStateStore(config.sqlite_path)
    lifecycle.attach_store(node_state_store, restore=True)
    try:
        yield
    finally:
        lifecycle.detach_store()
        node_state_store.close()
        node_state_store = None


app = create_storage_node_app(engine, config.node_id, lifecycle)
app.router.lifespan_context = lifespan
