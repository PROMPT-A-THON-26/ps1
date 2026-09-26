# Vault Security Notes

## Frontend controls

- Dynamic API responses rendered into the UI are passed through HTML escaping before insertion.
- API request identifiers are generated client-side and forwarded as `X-Request-ID` for traceability.
- Uploads are validated for type/size metadata before the request is sent. The demo frontend caps uploads at 100 MB; the backend remains authoritative.
- API base URLs are parsed with the platform URL parser and restricted to HTTP(S) schemes; credentials and fragments are removed.
- External links should use `rel="noopener noreferrer"` when opened in a new tab.
- The hosted static frontend defaults to a non-destructive mock mode so a public demo cannot accidentally issue control-plane mutations.

## Backend responsibility

Client-side checks are defense-in-depth only. The server must independently enforce authentication, authorization, input validation, upload limits, CORS, rate limiting, and durable metadata invariants before accepting production traffic.
