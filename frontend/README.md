# VaultGuard Part C — Frontend

Part C is the judge-facing resilience cockpit built with plain HTML, CSS and vanilla JavaScript.

## End-to-end mode

On MASTER, live API mode is the default:

    http://localhost:5173

The frontend connects automatically to:

    http://localhost:8000/api/v1

## One-command full stack

From repository root:

    docker compose up --build

Then open:

    http://localhost:5173

The full stack is:

    browser
      ↓
    Part C frontend
      ↓
    FastAPI gateway (:8000)
      ↓
    PostgreSQL + Redis
      ↓
    Celery control-plane worker
      ↓
    four Vault storage nodes (:9001–:9004)

## Live API contract

The frontend consumes:

- GET /api/v1/health
- GET /api/v1/nodes
- GET /api/v1/objects
- GET /api/v1/objects/{name}/metadata
- GET /api/v1/objects/{name}/versions
- PUT /api/v1/objects/{name}
- POST /api/v1/admin/repair
- GET /api/v1/admin/repair/{repair_id}
- POST /api/v1/admin/integrity/check
- GET /api/v1/admin/integrity/check/{job_id}
- POST /api/v1/admin/rebalance
- GET /api/v1/admin/rebalance/{job_id}

Node lifecycle changes are intentionally not exposed in the UI because Part B does not publish a corresponding public endpoint.

## Verification

Frontend-only:

    python frontend/tests/verify_frontend.py
    node --check frontend/js/app.js

Full stack:

    docker compose config --quiet
    docker compose up --build
