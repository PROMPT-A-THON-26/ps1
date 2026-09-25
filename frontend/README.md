# Vault Part C — Frontend

Part C is a dependency-light, judge-facing resilience cockpit built with plain HTML, CSS and vanilla JavaScript.

## Why it stands out

The first screen explains a distributed-storage failure story instead of presenting a generic CRUD dashboard:

- replica topology and capacity pressure are visible immediately
- resilience score, durability policy and failure-domain health share one state model
- the Resilience Drill demonstrates detect → isolate → repair → verify as a safe local simulation
- the command palette gives judges a fast way to move through the system
- live mode uses the documented Part B public REST contract and never fabricates unsupported backend behavior

## Views

Overview, Nodes, Objects, Repairs, Integrity, Rebalance, Events and Policies.

## Local preview

From repository root:

    python -m http.server 5173 --directory frontend

Open:

    http://localhost:5173

No npm install and no framework are required.

## Live Part B integration

Default mode is safe mock mode:

    const CONFIG = window.VAULT_CONFIG || { mode: "mock", baseUrl: "/api/v1" };

For a quick live preview without editing the bundle, open:

    http://localhost:5173/?mode=api&baseUrl=http%3A%2F%2Flocalhost%3A8000%2Fapi%2Fv1

Live mode uses:

- GET /health
- GET /nodes
- GET /objects
- GET /objects/{name}/metadata
- GET /objects/{name}/versions
- PUT /objects/{name}
- POST /admin/repair
- POST /admin/integrity/check
- POST /admin/rebalance
- GET /admin/repair/{repair_id}
- GET /admin/integrity/check/{job_id}
- GET /admin/rebalance/{job_id}

The frontend polls accepted admin jobs and refreshes normalized state when a job reaches a terminal state.

Node drain/resume is intentionally demo-only because the documented Part B public contract does not expose a corresponding endpoint.

## Error handling

API errors use the Part B shape:

    error.code
    error.message
    error.request_id

The frontend adds an X-Request-ID to API calls and surfaces failures through live toasts.

## Accessibility

The layout uses semantic sections, labeled navigation, labeled search fields, live status regions, keyboard-focus styles, a modal dialog and an interactive command palette.

## Verification

Run:

    python frontend/tests/verify_frontend.py

The repository CI also runs:

    node --check frontend/js/app.js

alongside the existing Python and integration test suite.
