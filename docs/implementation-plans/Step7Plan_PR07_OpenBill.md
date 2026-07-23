# Build Plan #7: F1 Open Endpoint + `ActivityPersistBill`

## Summary
Implement only PRD Step #7: opening a bill, persisting the parent ledger row from inside the workflow, and making `201 Created` mean the bill row is already committed. Keep add-line-item, close, GET-by-id, full LIST filtering, and line-item reads scoped to later steps.

## Key Changes
- Add `ActivityPersistBill` to `fees/activities.go`.
  - Input: existing `BillInput`.
  - SQL: `INSERT INTO bills (bill_id, client_id, currency, period, status) VALUES (...) ON CONFLICT (bill_id) DO NOTHING`.
  - Return no persisted aggregate; idempotency is the point. Tests assert rerun leaves exactly one bill row.

- Update `BillWorkflow` startup ordering.
  - First execute `ActivityPersistBill`.
  - Then register new `UpdateAwaitOpen = "awaitOpen"` returning `state.toView()`.
  - Then register `QueryGetBill`, `addLineItem`, close signal, and timer.
  - Register/mock `ActivityPersistBill` in workflow tests and add a startup-ordering test proving an early add-line-item Update is buffered until the bill row exists, not rejected as `BillNotOpen`.

- Add `POST /v1/bills`.
  - Request: `{ "clientId": "...", "currency": "USD", "period": "YYYY-MM" }`.
  - Validate before Temporal: non-empty `clientId`, `currency` matches `^[A-Z]{3}$`, `period` matches `^\d{4}-(0[1-9]|1[0-2])$`.
  - Reject elapsed periods when `time.Now().UTC() >= resolvePeriodEnd(period)` with `422 period-elapsed`.
  - Start using `UpdateWithStartWorkflow` with `WorkflowIDConflictPolicy: FAIL`, `WaitForStage: client.WorkflowUpdateStageCompleted`, workflow ID `bill-{clientId}-{currency}-{period}`, task queue `fees`, update name `awaitOpen`.
  - Return `201`, `Location: /v1/bills/{billId}`, and a bill resource read back from the ledger after `awaitOpen` completes.

- Use exact HTTP behavior.
  - Because `422` and RFC 9457 problem JSON are required, implement `POST /v1/bills` as an Encore raw endpoint if typed endpoint errors cannot express the exact status/body cleanly.
  - Keep typed response/header support available for future endpoints where Encore’s normal `encore:"httpstatus"` and `header` tags are sufficient; official Encore docs confirm response header tags and custom success status support.

## Error Mapping
- Malformed JSON, empty `clientId`, malformed `currency`, malformed `period`: `400 invalid-request`.
- Elapsed period: `422 period-elapsed`.
- Temporal workflow already running for the bill key: `409 bill-already-open`.
- `ActivityPersistBill` / update completion cannot be confirmed due timeout or Temporal/service unavailability: `503 open-unavailable`.
- Unexpected internal error: `500 internal-error`.

## Test Plan
- Add activity integration tests:
  - Fresh `ActivityPersistBill` inserts one `OPEN` row.
  - Re-running it is a no-op and still leaves one row.
- Update workflow tests:
  - Existing Step #6 tests mock the startup persist activity.
  - `UpdateAwaitOpen` only completes after `ActivityPersistBill`.
  - Add-line-item Update racing workflow startup is not rejected due missing bill row.
- Add API tests with fake Temporal client:
  - Success captures `UpdateWithStartWorkflow` options and returns `201 + Location`.
  - Bad period/currency/client validation does not call Temporal.
  - Past period returns `422`.
  - already-started maps to `409`.
  - update/persist confirmation failure maps to `503`.
- Run `encore test ./...` and then optional live E2E with `PAVEBANK_E2E=1` once Temporal and Encore are running.

## Assumptions
- PRD v3 is authoritative over earlier step plans for startup ordering.
- Step #7 may add a narrow read-after-open SQL helper only to build the open response; it must not implement full F6/F7 read behavior.
- The current Step #6 `DRAINING` behavior stays unchanged even though the v3 PRD narrative has some drift there.
- No `currencies` table validation is added yet; supported-currency validation remains deferred per the PRD.
- Sources checked: Encore typed API docs (`https://encore.dev/docs/go/primitives/defining-apis`) and raw HTTP endpoint docs (`https://encore.dev/docs/go/how-to/http-requests`).
