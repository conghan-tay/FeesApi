# Build Plan #6: Workflow

## Summary
Implement the real Temporal bill workflow in `fees-api/fees`, replacing the current scaffold-only workflow path while preserving the existing activities, Encore `sqldb` setup, and green `encore test ./...` baseline.

## Key Changes
- Add `fees/workflow.go` with `BillWorkflow`, using the v2 PRD contract:
  - Register `QueryGetBill` to return `BillView` identity plus live workflow status only.
  - Register `UpdateAddLineItem` with a validator that rejects currency mismatch as `"CurrencyMismatch"` and non-accrual states as `"BillNotOpen"`.
  - In the Update handler, call `ActivityPersistLineItem` with `ledgerRow(state, li)` and return `LineItemResult{Reference, Applied}`.
  - Use `resolvePeriodEnd(input.Period)` plus `workflow.NewTimer` for auto-close.
  - Use `SignalCloseBill` for explicit close; explicit close cancels the auto-close timer.
  - Use `workflow.Await(ctx, workflow.AllHandlersFinished)` before sealing.
  - Seal through `ActivityPersistInvoice`, then move status to `CLOSED`.
  - On `workflow.GetInfo(ctx).GetContinueAsNewSuggested()`, wait for handlers, cancel the timer, and continue as new with `state.carryForward()`.

- Update worker registration:
  - Add stable workflow constants for `BillWorkflow`, `UpdateAddLineItem`, `SignalCloseBill`, `QueryGetBill` if needed to avoid scattering names.
  - Register `BillWorkflow` on the `"fees"` task queue worker.
  - Keep scaffold workflow registration only if existing smoke tests still rely on it; otherwise remove scaffold tests and update them to assert `BillWorkflow` registration.

- Keep scope limited to Workflow:
  - Do not implement Open/Add/Close HTTP endpoints yet.
  - Do not add ledger read/store code.
  - Do not change schema or activity semantics unless required by workflow compilation.

## Test Plan
- Add `fees/workflow_test.go` using Temporal `TestWorkflowEnvironment` with mocked activities.
- Cover:
  - Open workflow starts in `OPEN`; `QueryGetBill` returns identity/status.
  - Add-line-item Update calls `ActivityPersistLineItem` and returns `Applied=true`.
  - Duplicate add returns `Applied=false` when mocked activity returns false.
  - Currency mismatch is rejected by the Update validator with `"CurrencyMismatch"`.
  - Add after close is rejected with `"BillNotOpen"`.
  - Explicit close signal drains handlers, calls `ActivityPersistInvoice`, and completes.
  - Auto-close timer fires at `resolvePeriodEnd(period)` and seals.
  - Continue-As-New path carries only lifecycle status when suggested.
- Update worker/service tests to expect the real workflow registration.
- Verification command: `encore test ./...`.
- Note: plain `go test ./...` is not the acceptance command because Encore `sqldb.NewDatabase` panics outside the Encore runner.

## Assumptions
- Use the current `sqldb`-backed Activities implementation from Step #5; do not switch to `pgxpool`.
- The current `BillInput.HasCarry` field remains as the explicit carry marker, even though the companion draft omits it; it avoids confusing a legitimate zero-valued `OPEN` status with “no carry”.
- Workflow-owned state remains lifecycle-only; totals, item count, and line-item references stay ledger-owned.
- API status-code mapping is deferred to Build Plan steps #7-#9.
