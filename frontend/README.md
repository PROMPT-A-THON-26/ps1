# Vault Part C — Frontend

Part C is a dependency-light judge-facing operations cockpit built with plain HTML, CSS and vanilla JavaScript.

## What makes the UI different

The first screen is intentionally not a generic CRUD dashboard. It treats Vault as a distributed-systems product and puts resilience on the first glance:

- replica topology is visible instead of hidden in a detail page
- a resilience score summarizes the operational state
- failure-domain health and storage pressure are visible together
- recovery, integrity and rebalance actions are one click away
- object details show the replica spread and checksum state
- all screens use the same state model and navigation
- demo mode works without Part B, so the UI can be reviewed independently

## Views

Overview, Nodes, Objects, Repairs, Integrity, Rebalance, Events and Policies.

## Local preview

From repository root:

    python -m http.server 5173 --directory frontend

Open:

    http://localhost:5173

No npm install and no framework are required.

## Part B integration boundary

The production adapter is intentionally isolated in frontend/js/app.js under the API object.

Default mode:

    const CONFIG = window.VAULT_CONFIG || { mode: "mock", baseUrl: "/api/v1" };

Before app.js is loaded, Part B can supply:

    window.VAULT_CONFIG = {
      mode: "api",
      baseUrl: "http://localhost:8000/api/v1"
    };

The UI uses the documented Part B public REST contract. In live mode it synchronizes health, nodes and object catalog data at startup/refresh, while administrative actions are sent through the same centralized adapter. The UI expects these logical operations:

- GET health summary
- GET nodes
- GET objects
- GET repairs
- GET integrity jobs/results
- GET rebalance jobs
- GET events
- PUT object upload (`/objects/{name}`)
- POST repair
- POST integrity check
- POST rebalance
- POST node drain/resume

If the final Part B route names differ, translate them only in the API object. Do not spread backend-specific paths through the UI.

## Error contract

The adapter is designed for:

    error.code
    error.message
    error.request_id

Request IDs are generated for API calls so Part C can surface failures without inventing backend behavior.

## Accessibility

The layout uses semantic sections, labeled navigation, labeled search fields, keyboard-focus styles, a modal dialog, and live toast announcements.

## Scope boundary

Part C does not change Part A or Part B implementation. It is safe to build and visually review while another developer works on the control plane.