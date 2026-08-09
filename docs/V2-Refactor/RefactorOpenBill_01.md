# Refactor OpenBill Persistence Before Temporal Start

## Summary

- Refactor [api.go](/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/fees-api/fees/api.go) so OpenBill persists or recovers the bill row, starts `BillWorkflow` with `ExecuteWorkflow`, and returns the persisted database snapshot after Temporal accepts the start.
- Preserve the existing HTTP response fields, `201 + Location`, normal duplicate `409`, and current `period-elapsed` status behavior.
- Remove `ActivityPersistBill`, `UpdateAwaitOpen`, and their workflow synchronization logic entirely. No replay migration is required because there are no live workflows.
- Do not change schemas or documentation in this PR.

## Implementation Changes

- Add a persistence helper that returns the stored `BillResource` and whether the row was newly inserted. Use `INSERT ... ON CONFLICT DO NOTHING RETURNING`; on conflict, load the exact existing row and verify its identity.
- OpenBill ordering:
  1. Validate request grammar and supported currency.
  2. Confirm the Temporal client is configured before writing.
  3. Load an existing bill by the deterministic ID.
  4. For a new bill, reject an elapsed period, then insert it as `OPEN`.
  5. For an existing `CLOSED` bill, return `409 bill-already-open` without calling Temporal.
  6. For an existing `OPEN` bill, attempt workflow start even if the period has since elapsed, allowing orphan recovery.
  7. Call `ExecuteWorkflow` with the current task queue, workflow ID, conflict policy `FAIL`, and reuse policy `REJECT_DUPLICATE`.
  8. Return `201` using the stored row; do not wait for workflow code or re-read through `readOpenedBillResource`.
- Map `WorkflowExecutionAlreadyStarted` to the existing duplicate `409`. Map other start failures to `503 open-unavailable`, deliberately retaining the OPEN row so the same request can heal it later. Do not perform a compensating delete.
- Update the Temporal client abstraction and test fakes: add `ExecuteWorkflow`; remove `NewWithStartWorkflowOperation` and `UpdateWithStartWorkflow`.
- Simplify [workflow.go](/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/fees-api/fees/workflow.go): register Query, add-line-item Update, close Signal, and timer without `billPersisted`; preserve close, auto-close, and Continue-As-New behavior.
- Remove the `ActivityPersistBill` constant and method from [activities.go](/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/fees-api/fees/activities.go). Keep line-item and invoice activities registered normally.

## Compatibility and Failure Effects

- The external OpenBill JSON contract remains unchanged.
- The internal Temporal contract breaks: `awaitOpen` disappears, and old workflow histories cannot safely replay. Deployment must verify that no existing `BillWorkflow` executions or pending `ActivityPersistBill` tasks remain.
- A non-duplicate `503` may now leave a database-visible OPEN bill without a workflow. Until OpenBill is retried successfully, GET/LIST will expose it while AddLineItem and CloseBill return unavailable and auto-close cannot occur.
- A Temporal timeout is ambiguous: the workflow may have started despite the 503. A retry will then receive the normal duplicate 409.
- Database persistence no longer benefits from Temporal Activity retries; transient database failures return 503 without starting a workflow.
- A successful 201 guarantees that Postgres committed the row and Temporal accepted the start. It does not guarantee the worker has initialized handlers yet; Temporal will buffer subsequent Updates and Signals.
- PRD v3 and its companion documents will intentionally remain inconsistent with the new implementation and must be identified as stale in the PR description.

## Test Plan

- API/database tests:
  - Fresh open proves the row exists before the fake Temporal client receives `ExecuteWorkflow`.
  - Response identity, `openedAt`, zero total/count, Location, and workflow options remain correct.
  - Start failure returns 503 and leaves one OPEN row; repeating the request successfully starts the workflow and returns 201 without another row.
  - Existing OPEN bill with an active workflow returns 409.
  - Existing CLOSED bill returns 409 without calling Temporal.
  - Existing OPEN orphan can be recovered after period end; a new elapsed bill remains rejected.
  - Nil Temporal client and database-write failures do not start workflows.
  - Concurrent identical opens leave one row and produce one successful start plus one duplicate response.
- Workflow tests:
  - Remove await-open/startup-persistence expectations and activity mocks.
  - Verify query and add-line-item handlers operate without `ActivityPersistBill`.
  - Retain lifecycle, close, timer, draining, and Continue-As-New coverage.
- Activity/worker/service tests:
  - Remove `ActivityPersistBill` integration coverage.
  - Update client fakes and ensure only the remaining activity methods are available.
- Run `encore test ./...`; the current pre-change baseline is green.
- Keep the existing optional full-stack lifecycle E2E unchanged because the public success contract is unchanged.

## Assumptions

- There are no active or retained workflow executions requiring replay compatibility.
- Retry healing is caller-driven; no outbox, reconciler, rollback, or new lifecycle status will be added.
- Existing OPEN rows are recoverable; existing CLOSED rows and active workflows remain duplicate conflicts.
- The current implementation’s HTTP status mappings remain unchanged, including its existing divergence from PRD v3 for elapsed periods.
- Documentation changes are explicitly out of scope.
