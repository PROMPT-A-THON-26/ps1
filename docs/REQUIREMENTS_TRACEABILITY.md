# Vault Part B Requirements Traceability

This document ties the implemented control-plane behaviors to the project specification so reviewers can verify the engineering evidence directly.

| Required capability | Implementation | Verification |
|---|---|---|
| Store object/version metadata | `metadata/models.py`, `metadata/manager.py` | `tests/test_metadata.py`, `tests/test_gateway.py` |
| Replicate with configurable RF/WQ/RQ | `replication/manager.py`, `common/settings.py` | `tests/test_replication_manager.py`, `tests/test_part_a_integration.py` |
| Verify replicas before HEALTHY | `replication/manager.py`, `metadata/manager.py` | checksum-mismatch and direct-transition tests |
| Stream/chunk object data | `gateway/service.py`, `storage/storage_engine.py`, `storage/node_server.py` | `tests/test_storage_engine.py`, `tests/test_gateway.py` |
| Checksums and integrity verification | `storage/storage_engine.py`, `integrity/manager.py` | `tests/test_integrity.py`, `tests/test_failure_chaos.py` |
| Expected-version concurrency protection | `metadata/manager.py`, `gateway/service.py` | `tests/test_gateway.py`, `tests/test_failure_chaos.py` |
| Failure detection and recovery | `health/failure_detector.py`, `health/heartbeat.py`, `recovery/manager.py` | `tests/test_health.py`, `tests/test_recovery.py`, `tests/test_failure_chaos.py` |
| Repair and under-replication healing | `repair/manager.py`, `worker/tasks.py` | `tests/test_repair.py`, `tests/test_worker.py` |
| Rebalancing and draining | `rebalance/manager.py`, `worker/tasks.py` | `tests/test_rebalance.py`, `tests/test_failure_chaos.py` |
| Durable background jobs | `metadata/models.py`, `worker/tasks.py` | worker/restart-safe tests |
| Capacity safety | `storage/storage_engine.py`, database check constraint, migration 0003 | `tests/test_storage_engine.py`, `tests/test_migrations.py` |
| Bounded upload ingress | `common/settings.py`, `gateway/api.py`, `gateway/service.py` | `tests/test_gateway.py` |
| Administrative control protection | `gateway/api.py` with `VAULT_ADMIN_API_KEY` | `tests/test_admin.py` |
| Network/API hardening | `gateway/app.py`, `storage/node_server.py`, `replication/node_client.py` | CI Bandit/Ruff plus security tests |
| Failure/partition regression coverage | `tests/test_failure_chaos.py` | CI |
| Frontend observability and safe demo mode | `frontend/index.html`, `frontend/js/app.js`, `frontend/js/config.js` | `frontend/tests/verify_frontend.py` |
| Accessibility requirements | semantic landmarks, labels, progress semantics, focus management, reduced motion, contrast | `frontend/tests/verify_frontend.py`, manual keyboard review gate |

## Release evidence

- Backend CI runs compile checks, Ruff static analysis, Bandit security analysis, and the complete pytest suite on Python 3.11 and 3.12.
- MASTER end-to-end CI builds the complete Docker stack, waits for all services, probes the public API, and exercises object write/read/delete.
- Frontend CI runs the dedicated accessibility/security contract verifier.
- Docker services use non-root containers, dropped capabilities, read-only roots, and no-new-privileges.
