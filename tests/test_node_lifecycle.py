from pathlib import Path

from storage.node_lifecycle import NodeLifecycle, NodeLifecycleState
from storage.node_state import SQLiteNodeStateStore


def test_lifecycle_starts_healthy():
    lifecycle = NodeLifecycle()
    assert lifecycle.state is NodeLifecycleState.HEALTHY
    assert lifecycle.accepting_writes is True


def test_lifecycle_drain_and_resume_are_idempotent():
    lifecycle = NodeLifecycle()

    lifecycle.drain()
    lifecycle.drain()
    assert lifecycle.state is NodeLifecycleState.DRAINING
    assert lifecycle.accepting_writes is False

    lifecycle.resume()
    lifecycle.resume()
    assert lifecycle.state is NodeLifecycleState.HEALTHY
    assert lifecycle.accepting_writes is True


def test_lifecycle_state_persists_in_sqlite(tmp_path: Path):
    db_path = tmp_path / "node_state.sqlite3"

    first = NodeLifecycle()
    store = SQLiteNodeStateStore(db_path)
    first.attach_store(store)
    first.drain()
    first.detach_store()
    store.close()

    second = NodeLifecycle()
    store = SQLiteNodeStateStore(db_path)
    second.attach_store(store)

    assert second.state is NodeLifecycleState.DRAINING
    assert second.accepting_writes is False

    second.resume()
    assert second.state is NodeLifecycleState.HEALTHY
    assert store.get_state() == NodeLifecycleState.HEALTHY.value

    second.detach_store()
    store.close()

def test_invalid_persisted_lifecycle_state_fails_closed(tmp_path: Path):
    db_path = tmp_path / "node_state.sqlite3"
    store = SQLiteNodeStateStore(db_path)
    store.set_state("invalid-state")
    store.close()

    lifecycle = NodeLifecycle()
    store = SQLiteNodeStateStore(db_path)
    lifecycle.attach_store(store, restore=True)

    assert lifecycle.state is NodeLifecycleState.DRAINING
    assert lifecycle.accepting_writes is False
    assert store.get_state() == NodeLifecycleState.DRAINING.value

    lifecycle.detach_store()
    store.close()
