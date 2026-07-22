# Build Plan #1: App Scaffold

## Summary
Implement the first PRD build step inside `fees-api`: replace the Hello World app with a single Encore Go `fees` service that boots, exposes a placeholder `GET /v1/bills`, provisions a Postgres database, and starts a Temporal worker polling task queue `fees`.

## Key Changes
- Replace the generated `hello` service with `fees/`.
- Add a service struct using Encore’s `//encore:service` init pattern:
  - Create a Temporal client at startup.
  - Start a Temporal worker in the background on task queue `fees`.
  - Register placeholder workflow/activity names needed for scaffold smoke only.
  - Stop the worker and close the Temporal client in `Shutdown`.
- Add Encore Postgres declaration in the `fees` service:
  - Database name: `feesdb`.
  - Migrations path: `./migrations`.
  - No schema tables yet; Build Plan #3 owns the ledger migration.
- Add public placeholder endpoint:
  - `GET /v1/bills`
  - Response: `{ "bills": [], "nextCursor": "", "hasMore": false }`
  - This exists only to satisfy step #1 smoke; full filters/cursor behavior lands in Build Plan #10.
- Keep module `encore.app`, Go `1.26`, and current `encore.dev v1.57.5`; add `go.temporal.io/sdk` without downgrading the generated app.

## Temporal Config
- Use local defaults for step #1 smoke:
  - Target: `127.0.0.1:7233`
  - Namespace: `default`
  - Task queue: `fees`
- Do not declare Encore secret fields yet, because Encore requires declared secrets to be set before running. Secret-backed config will be introduced when targeting non-local environments.
- Document future secret names in README comments or scaffold code:
  - `TemporalTarget`
  - `TemporalNamespace`
  - later Cloud-only TLS secrets if needed.

## Test Plan
- Update/remove Hello World tests and add scaffold smoke tests for:
  - `ListBills` returns 200-compatible empty response shape.
  - Temporal config resolves to local defaults.
  - Worker setup registers successfully when Temporal dev server is available.
- Manual smoke:
  - Terminal 1: `temporal server start-dev`
  - Terminal 2: `encore run`
  - Call `GET /v1/bills` and expect empty list.
  - Confirm worker polls task queue `fees` in Temporal UI or logs.

## Assumptions
- Scope is strictly Build Plan #1; no ledger tables, open/add/close APIs, workflow state machine, or real line-item persistence yet.
- The drafted `docs/workflow.go` remains authoritative for later workflow implementation, but step #1 should only add enough workflow/activity stubs to prove worker registration and polling.
- Source used for Encore secret behavior: https://encore.dev/docs/go/primitives/secrets
