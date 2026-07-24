# Build Plan #9: F3/F10 Close Endpoint

## Summary
Implement `POST /v1/bills/:billId/close` for the Encore Go service. The endpoint will close an open bill through Temporal, then return the sealed invoice facts from Postgres: status, computed total, item count, close timestamp, and full line-item list. It will also satisfy F10 by returning `200` with the existing sealed invoice when the bill is already closed, even if the workflow has completed or aged out.

Baseline verified: `encore test -v ./...` currently passes, with E2E skipped unless `PAVEBANK_E2E=1`.

## Public API And Interfaces
- Add raw endpoint: `POST /v1/bills/:billId/close`.
- Request body: `{ "reason": "<string>" }`; `reason` is optional/informational. Empty body or `{}` is accepted; malformed JSON or unknown fields returns `400 invalid-request`.
- Success response: `200 OK` invoice-shaped JSON with:
  - existing bill fields: `billId`, `clientId`, `currency`, `period`, `status`, `totalMinorAmount`, `itemCount`, `openedAt`, `closedAt`
  - `lineItems`: always present, ordered by ledger insertion order, with `reference`, `minorAmount`, `currency`, `feeType`, `description`, `appliedAt`
- Extend the internal `temporalClient` abstraction with:
  - `SignalWorkflow(ctx, workflowID, runID, signalName string, arg interface{}) error`
  - `GetWorkflow(ctx, workflowID, runID string) client.WorkflowRun` or a narrow local workflow-run interface with `Get(ctx, valuePtr)`.

## Key Changes
- Add close handler in `fees/api.go`:
  - Extract `billId` using the existing Encore path-param approach, with a direct-test fallback generalized for `/close`.
  - If the ledger already has the bill as `CLOSED`, return the invoice immediately. This is the durable F10 path and works after Temporal history retention.
  - If the bill does not exist in the ledger, return `404 bill-not-found`.
  - If the bill is `OPEN`, send `SignalCloseBill` with `CloseSignal{Reason: input.Reason}`, then wait on `GetWorkflow(...).Get(ctx, nil)`.
  - After Temporal completion, read the sealed invoice from the ledger and require `status == "CLOSED"` before returning `200`.
  - Map missing Temporal workflow during signal/wait to a ledger fallback: return closed invoice if now `CLOSED`, `404` if no bill exists, otherwise `503 close-unavailable`.
  - Map generic Temporal/ledger failures to `503 close-unavailable` with internal details redacted.

- Add ledger read helpers:
  - Factor the current aggregate bill read so close can compute `COALESCE(SUM(amount_minor),0)` and `COUNT(li.id)` consistently with open/get/list plans.
  - Add a close-invoice read that loads line items separately from `line_items`, orders by `id`, formats `amount_minor` as string `minorAmount`, and returns `lineItems: []` for zero-item bills.

- Keep scope tight:
  - Do not implement `GET /v1/bills/{id}` or full `LIST` filtering in this step.
  - Do not change workflow state semantics or the current `DRAINING` behavior.
  - Do not add an invoices table or stored totals.

- Update docs:
  - Mark README Step #9 complete after implementation.
  - Note that close/re-close behavior is implemented, while full black-box E2E remains partially blocked until Step #10 because the current E2E calls GET before close.

## Test Plan
- Add API tests for close success:
  - Seed an `OPEN` bill with multiple line items.
  - Fake Temporal signal/wait and have the fake wait mark the bill `CLOSED`.
  - Assert `200`, `status=CLOSED`, computed total, item count, `closedAt`, and itemized `lineItems`.

- Add F10 tests:
  - Seed an already `CLOSED` bill with line items.
  - Assert re-close returns `200` with the same invoice facts and does not call Temporal.
  - Assert zero-item closed bill returns `lineItems: []` and total `"0"`.

- Add error-mapping tests:
  - Missing `billId` or malformed/unknown JSON -> `400 invalid-request`.
  - No ledger bill -> `404 bill-not-found`.
  - Open ledger bill with nil Temporal client -> `503 close-unavailable`.
  - Signal/get `serviceerror.NotFound` plus closed ledger fallback -> `200`.
  - Signal/get `serviceerror.NotFound` while ledger remains `OPEN` -> `503 close-unavailable`.
  - Generic signal/get errors -> `503`, with raw internal details redacted.

- Update fakes:
  - Extend `openTemporalClient` and other fake Temporal clients to satisfy the new interface.
  - Add fake workflow-run `Get` support for handler tests.

- Run verification:
  - `gofmt` touched Go files.
  - `encore test -v ./...`.
  - Do not require `PAVEBANK_E2E=1 go test -v ./e2e` to pass until Step #10 implements GET/LIST; existing E2E remains the later full-lifecycle gate.

## Assumptions
- `PaveBank_Fees_API_PRD_v3.docx`, `API_CONTRACTS.md`, `LEDGER.md`, and `docs/workflow.go` are the intent sources; current repo behavior wins where implementation drift is already covered by passing tests.
- The F10 durable re-close behavior must work even when a completed workflow can no longer be signaled.
- Step #9 should stay focused on close; GET/LIST and full E2E completion remain Step #10.
