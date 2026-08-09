<proposed_plan>
# Extract AddLineItem into the Charge Service and Use Temporal Signals

## Summary

- Create a new Encore `charge` service owning `POST /v1/bills/:billId/line-items`, its DTOs, syntactic validation, Temporal client, and HTTP error mapping.
- Preserve the HTTP path and JSON fields, but change successful calls to `202 Accepted` with `{reference, applied:true}` once Temporal accepts the Signal.
- Keep `BillWorkflow`, `ActivityPersistLineItem`, and ledger ownership in `fees`.
- Hard-cut from the AddLineItem Workflow Update to a Signal; no compatibility for existing Update histories.
- Make no schema or documentation changes.

## Implementation Changes

- Add a shared non-service package containing `SignalAddLineItem = "addLineItem"` and the JSON-free Temporal `LineItem` payload.
- In `charge/`:
  - Define the existing public route and request/response fields.
  - Retain syntactic validation for required fields, uppercase three-letter currency, and signed `int64` amount parsing.
  - Initialize a Temporal client without a worker.
  - Call `SignalWorkflow(billID, "", SignalAddLineItem, lineItem)`.
  - Return `202` with the request reference and `Applied: true` when Temporal accepts the Signal.
  - Map Temporal `NotFound`, nil clients, and all other Signal failures directly to redacted `503 add-line-item-unavailable`.
  - Do not access `feesdb` or perform ledger fallback/preflight queries.
- In `fees/`:
  - Remove the AddLineItem endpoint, DTOs, validator, Update-specific error handling, and `UpdateWorkflow` client dependency.
  - Replace the Update handler with an AddLineItem Signal channel.
  - Recheck currency and workflow status before launching `ActivityPersistLineItem`.
  - Process valid Signals concurrently and explicitly track in-flight activities.
  - Log and discard currency mismatches, post-draining Signals, and non-retryable persistence rejections.
  - Wait for in-flight Signal activities before close, auto-close, or Continue-As-New.
  - Continue using the current Activity retry policy and database idempotency constraint.

## API and Failure Effects

- The URL and JSON field names remain unchanged.
- Fresh and duplicate requests both return `202 {applied:true}`; `applied` now means Signal acceptance, not ledger insertion.
- Temporal `NotFound` always returns `503 add-line-item-unavailable`:
  - Unknown bill IDs no longer return `404`.
  - Requests against completed CLOSED workflows no longer return `409`.
  - OPEN bills with missing workflows also return `503`.
- Currency mismatch and close-race failures can return `202` before being rejected asynchronously; their outcomes are visible only through structured logs.
- Immediate GET/LIST calls may not include an accepted item yet.
- Database uniqueness still prevents double accrual, but callers cannot distinguish a fresh insertion from a duplicate.
- In-process Encore callers must migrate from `fees.AddLineItem` to `charge.AddLineItem`; external HTTP routing remains unchanged.
- Existing workflow histories containing AddLineItem Updates are unsupported and must not require replay.
- PRD v3 and companion documentation remain intentionally stale and must be identified as such in the PR description.

## Test Plan

- Charge API tests:
  - Verify Signal workflow ID, empty run ID, Signal name, and payload.
  - Verify every accepted Signal returns exact `202 {reference, applied:true}`.
  - Retain all syntactic validation tests and assert invalid requests do not call Temporal.
  - Verify Temporal `NotFound`, generic errors, and a nil client all return redacted `503 add-line-item-unavailable`.
  - Confirm the Charge service declares no database and starts no worker.
- Fees workflow tests:
  - Valid Signals invoke `ActivityPersistLineItem`.
  - Duplicate Activity results remain successful no-ops.
  - Currency-mismatched and draining-state Signals invoke no persistence Activity.
  - Multiple Signal activities may overlap, while close waits for all in-flight work.
  - Non-retryable Activity rejection is logged without failing the workflow.
  - Auto-close and Continue-As-New account for buffered and in-flight Signals.
  - Remove Update callbacks, handles, validators, and assertions.
- E2E tests:
  - Expect `202/applied:true` for fresh and duplicate adds.
  - Poll GET until accepted valid charges become visible before checking totals or closing.
  - Expect mismatched currency to return `202`, then verify it does not affect the ledger.
  - Expect AddLineItem against a completed CLOSED workflow to return `503`, not the previous `409`.
- Run `encore test ./...` and the optional full-stack lifecycle E2E.

## Assumptions

- Charge owns only API ingress and Temporal signaling; Fees continues owning orchestration and persistence.
- No database access, durable command records, failed-charge rows, reconciliation process, or result-status API will be added to Charge.
- No live or retained workflow execution requires AddLineItem Update replay compatibility.
- Documentation changes are outside this PR.
</proposed_plan>