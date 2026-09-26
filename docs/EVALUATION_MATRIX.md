# Vault Evaluation Matrix

This document is an internal engineering map from the Vault implementation to the six evaluation dimensions used by the PromptWars/Hack2Skill-style evaluator. It is evidence-oriented: every claim below points to implementation or tests that should be inspectable in the repository.

## Code Quality

| Evidence | Implementation |
|---|---|
| Layered architecture | Gateway routes delegate to `GatewayService`; placement, replication, repair, integrity, recovery, rebalance, health, and workers remain separate modules. |
| Canonical contracts | Request IDs, error codes, replication policy, storage-node client errors, and object-name validation are centralized. |
| Deterministic behavior | Placement sorts by free capacity and `node_id`; lifecycle/state changes are explicit. |
| Failure handling | Storage/network failures are mapped to typed exceptions; cleanup paths avoid silently swallowing normal control-flow errors. |
| Maintainability | Frontend routes/constants/status mapping are centralized; rendering is isolated by active view. |

## Security

| Evidence | Implementation |
|---|---|
| Input validation | Object names, request IDs, storage-node identifiers, upload size/type metadata, and API base URLs are validated. |
| Browser hardening | CSP metadata, strict referrer policy, no inline JavaScript, safe output escaping, redirect blocking, and security headers are present. |
| API hardening | Restricted CORS, bounded request rate limiting, `nosniff`, frame-denial, no-store responses, browser isolation headers, explicit HTTP(S) URL validation, and authenticated storage-node control traffic are used. |
| Container hardening | Gateway, worker, frontend, and storage nodes run as non-root with dropped capabilities, no-new-privileges, read-only root filesystems, and hardened temporary filesystems. |
| Secret handling | Compose service credentials are supplied at runtime rather than committed as fixed database/Redis passwords; internal node and admin keys are runtime-only. |
| Safe retries | Mutating PUT is not blindly retried after ambiguous network failure; safe/idempotent operations use bounded retry policies. |

## Efficiency

| Evidence | Implementation |
|---|---|
| Streaming | Gateway stages request bodies incrementally and streams stored objects to storage nodes. |
| Bounded placement queries | Placement and repair target selection filter/order/limit at the SQL layer. |
| Query batching | Version replica counts and under-replication counts are grouped instead of issuing one count query per version. |
| SQL aggregation | Cluster health is aggregated in SQL rather than materializing all nodes. |
| Cached storage accounting | Storage usage is cached and updated under a lock instead of scanning the filesystem for every statistics request. |
| Concurrency | Independent replica network writes are performed concurrently while metadata transitions remain centralized; delete traffic is parallelized while reusing node clients. |
| Reduced client overhead | Delete paths reuse node clients and close them together. |

## Testing

| Evidence | Implementation |
|---|---|
| Control-plane test suite | `pytest -q` runs backend unit/integration tests in CI on supported Python versions. |
| Storage contract tests | Tests cover streaming, protocol validation, corruption, retries, timeouts, traversal protection, and request tracing. |
| Gateway contract tests | Tests cover replication/commit, version conflicts, read failover, delete failure states, and public API behavior. |
| Frontend contract tests | `frontend/tests/verify_frontend.py` statically checks API contract, security, accessibility, rendering, and demo/live separation. |
| CI gates | Backend, frontend, and full-stack end-to-end workflows run on the submitted `main` branch; dependency auditing, Docker Compose validation, API smoke tests, and object write/read/delete smoke tests are included. |
| Negative paths | The suite intentionally covers insufficient replicas, unreachable nodes, corruption, malformed responses, stale versions, and failed deletion/repair paths. |

## Accessibility

| Evidence | Implementation |
|---|---|
| Keyboard access | Skip link, visible focus indicators, keyboard command palette controls, Escape handling, and modal focus trapping are implemented. |
| Focus context | Dialog focus is restored to the invoking element and the application shell is marked inert while an overlay is open. |
| Semantics | Landmarks, named navigation/search controls, table captions/headers, live regions, and progressbar values are present. |
| Motion | `prefers-reduced-motion` removes non-essential animation/scroll motion. |
| Readability | Dense operational text has readable minimum sizes and strengthened contrast colors. |
| Dynamic state | Filters use `aria-pressed`; status/toast/progress updates expose programmatic state. |

## Problem Statement Alignment

Vault is implemented around the distributed object-storage control-plane requirements:

- **Store/replicate/retrieve:** gateway + replication manager + storage-node client.
- **Quorum durability:** configurable RF/WQ/RQ with commit only after verified healthy replicas satisfy write quorum.
- **Verification before HEALTHY:** a replica is marked healthy only after storage-node verification, checksum, and size validation; fresh storage nodes are verified before gateway bootstrap can mark them HEALTHY.
- **Self-healing:** health/failure detection, durable repair jobs, integrity checks, and recovery workflows.
- **Network partition/recovery:** node health state transitions and recovery reconciliation.
- **Rebalancing:** verified replica migration away from draining/high-pressure nodes.
- **Integrity:** SHA-256 object/chunk verification and corruption isolation.
- **Concurrency/version safety:** expected-version conflicts, explicit locks, bounded worker concurrency, and idempotent state transitions.
- **Durable background work:** Celery task routing, retries with backoff, and persisted repair/integrity/rebalance job state.

## Release Gate

Before a scoring submission, verify:

1. The intended submission branch contains the latest code and documentation.
2. Backend tests are green.
3. Frontend verification is green.
4. End-to-end stack build/compose validation is green.
5. The live preview is reachable and clearly identifies demo vs live API behavior.
6. No secrets, credentials, or private endpoints are committed.
