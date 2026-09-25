from storage.node_lifecycle import NodeLifecycle, NodeLifecycleState


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
