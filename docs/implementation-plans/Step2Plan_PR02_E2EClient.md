# Build Plan #2: E2E Client + README Skeleton

## Summary
Add an opt-in black-box E2E dashboard for the Fees API under `fees-api/e2e`, plus expand the README with cold-start, architecture, and test instructions. This step does not implement ledger tables, workflow logic, or new endpoints; it encodes the PRD Section 9.4 lifecycle expectations so later build steps turn the dashboard green.

## Key Changes
- Add `e2e/client.go` using only `net/http` and `encoding/json`.
  - `NewClient(baseURL string)` with default base URL from `PAVEBANK_API_BASE_URL`, falling back to `http://localhost:4000`.
  - Typed methods for `POST /v1/bills`, `POST /v1/bills/{billId}/line-items`, `POST /v1/bills/{billId}/close`, `GET /v1/bills/{billId}`, and `GET /v1/bills`.
  - Return a typed response wrapper carrying status code, headers, decoded success body, decoded problem body, and raw body for clear failures.
- Add `e2e/e2e_test.go` as an opt-in live suite.
  - Guard with `PAVEBANK_E2E=1`; otherwise skip under normal `encore test ./...`.
  - Preflight `GET /v1/bills` so an opted-in run fails clearly if the app is not running.
  - Use unique client IDs per run and a far-future period such as `2099-07` to avoid elapsed-period flakiness.
- Lock expected wire shapes from the PRD/contracts:
  - Add-line-item success body: `{ "reference": "...", "applied": true|false }`.
  - Bill body includes computed `totalMinorAmount`, `itemCount`, `openedAt`, `closedAt`.
  - Close/get-with-items body includes `lineItems`.
  - Error bodies are parsed as RFC 9457 problem JSON, but this step primarily asserts HTTP status until endpoint-specific error taxonomy lands later.
- Update `fees-api/README.md`.
  - Preserve existing scaffold smoke instructions.
  - Add two-terminal local startup: `temporal server start-dev` and `encore run`.
  - Add E2E command: `PAVEBANK_E2E=1 PAVEBANK_API_BASE_URL=http://localhost:4000 go test -v ./e2e`.
  - Add architecture map: callers → Encore API → Temporal client/workflow → activities → Postgres ledger → read endpoints.
  - Add build progress section showing Step #1 complete, Step #2 dashboard, Steps #3-#12 upcoming.

## E2E Scenarios
- Open bill: expect `201`, `Location`, `OPEN`, total `0`.
- Add three distinct items: expect `201` each; `GET` shows running total.
- Re-add one reference: expect `200`, `applied=false`, total unchanged.
- Add mismatched-currency item: expect `400`.
- Close bill: expect `200`, correct total, itemized list present.
- Add after close: expect `409`.
- Re-close: expect `200` with the same sealed invoice facts.
- Get and list: bill appears with computed total under `clientId/status/currency/period` filters.

## Test Plan
- Run from `fees-api/`: `encore test -v ./...`
  - Expected after Step #2: existing tests pass; E2E package skips without `PAVEBANK_E2E=1`.
- With Temporal and Encore running: `PAVEBANK_E2E=1 PAVEBANK_API_BASE_URL=http://localhost:4000 go test -v ./e2e`
  - Expected immediately after Step #2: suite runs and fails cleanly because most endpoints are not implemented yet.
  - Expected across later steps: individual E2E cases go green as Steps #7-#10 land.

## Assumptions
- E2E is opt-in live testing, not failing by default.
- The E2E client stays black-box and does not import the `fees` service package or generated Encore client.
- No new dependencies are needed for Step #2.
- Scope is strictly Build Plan #2: client, E2E tests, and README skeleton only.
