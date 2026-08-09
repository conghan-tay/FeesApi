<proposed_plan>
# Publish PENDING Line-Item Status Before Persistence

## Summary

- Add a public validation-only `POST /v1/line-item-status` endpoint to the `charge` service.
- Add `ActivityPublishPending` to the `fees` worker and execute it before `ActivityPersistLineItem` for every accepted `addLineItem` Signal.
- Keep `minorAmount` as a decimal string on both Charge HTTP request DTOs while retaining `int64` inside the Temporal and ledger boundaries.
- Publish only `PENDING` in this PR; successfully persisted rows continue to be written directly as `FINALIZED`.
- Make no database migration or generated-file edits.

## API and Validation Changes

- Define `PublishLineItemStatusRequest` with `billId`, `reference`, `minorAmount`, `currency`, `feeType`, `description`, and `status` fields.
- Define `MinorAmount` as `string`, identical to `AddLineItemRequest.MinorAmount`. Callback JSON uses a quoted decimal value:

  ```json
  {
    "billId": "bill-acme-USD-2099-01",
    "reference": "pay-svc-evt-98213",
    "minorAmount": "1500",
    "currency": "USD",
    "feeType": "wire_transfer",
    "description": "Outbound USD wire",
    "status": "PENDING"
  }
  ```

- Add a dedicated `validatePublishLineItemStatusRequest` validator:
  - Require the request body, bill ID, reference, minor amount, currency, fee type, and status.
  - Parse `minorAmount` with `strconv.ParseInt(value, 10, 64)` and reject malformed, fractional, or overflowing values.
  - Accept zero and negative signed amounts.
  - Require a three-letter uppercase currency.
  - Accept only `PENDING`, `FINALIZED`, or `FAILED` as exact status values.
- Return an empty `200 OK` response when validation succeeds and the existing `400 invalid-request` error shape when it fails.
- Keep the endpoint validation-only: it does not call Temporal, access Postgres, or mutate application state.
- Let Encore regenerate its service-to-service wrapper and `charge.Interface`; do not edit `encore.gen.go` manually.

## Activity and Workflow Changes

- Add `ActivityPublishPending(ctx, LedgerRow) error` to the registered `Activities` method set.
- Use an injectable adapter around Encore's generated `charge.PublishLineItemStatus` service call.
- Build the callback DTO from `LedgerRow`, formatting `AmountMinor` with `strconv.FormatInt(row.AmountMinor, 10)` only at the HTTP boundary and setting status to `PENDING`.
- In the line-item `workflow.Go` handler:
  1. Derive the existing `LedgerRow` once.
  2. Execute `ActivityPublishPending` with a 30-second start-to-close timeout, exponential backoff, and five total attempts.
  3. Execute `ActivityPersistLineItem` with its existing retry policy only after publication succeeds.
  4. If publication exhausts its attempts, log the failure, skip persistence, and release the in-flight handler so bill closure can continue.
- Invoke the PENDING callback for every accepted Signal, including duplicate references; the database uniqueness constraint remains the persistence idempotency boundary.
- Continue rejecting currency mismatches and Signals received after the workflow stops accepting accruals before either Activity runs.

## Compatibility and Failure Effects

- The new HTTP route is additive, but the generated `charge.Interface` gains a method and external mocks may need updating.
- This is a deliberate Temporal hard cut. Active workflow histories that previously scheduled `ActivityPersistLineItem` can fail replay after the new preceding Activity is introduced; deployment must confirm that no affected histories remain.
- `AddLineItem` still returns `202` after Temporal accepts the Signal. A later PENDING callback failure can therefore cause the item to be dropped without persistence; the outcome is visible only through workflow logs.
- Publication retries keep the handler in flight and delay bill closure until publication succeeds or reaches its fifth failed attempt.
- PENDING is not stored in the Fees ledger or exposed by GET/LIST. A later persistence failure does not publish `FAILED`, and successful persistence does not publish a separate `FINALIZED` callback in this PR.
- The endpoint is public and unauthenticated but has no side effects beyond validation and success logging.

## Test Plan

- Charge API tests:
  - Accept `PENDING`, `FINALIZED`, and `FAILED` with positive, zero, and negative decimal strings.
  - Reject missing, fractional, non-integer, and overflowing `minorAmount` values with `400 invalid-request`.
  - Verify JSON encodes `minorAmount` as a quoted string and rejects numeric JSON values.
  - Cover required fields, uppercase currency, exact status casing, empty 200 responses, and absence of Temporal calls.
- Activity tests:
  - Verify positive, zero, and negative `int64` values are formatted into exact decimal strings.
  - Verify all DTO fields and fixed `PENDING` status, publisher error propagation, and missing-client handling.
- Workflow tests:
  - Verify publication completes before persistence starts.
  - Verify five failed publication attempts skip persistence and allow close to continue.
  - Verify every duplicate Signal publishes PENDING and valid concurrent Signals still drain before sealing.
  - Preserve mismatch, closing, auto-close, persistence-rejection, and Continue-As-New coverage.
- Worker/service tests:
  - Verify `ActivityPublishPending` is registered and the production Charge adapter is configured.
- Run `encore test ./...` and `git diff --check`.

## Assumptions

- Charge owns the callback HTTP contract; Fees owns workflow orchestration and ledger persistence.
- No active or retained workflow execution requires replay compatibility.
- No durable status store, terminal status callback, authentication, reconciliation process, or schema change is included.
</proposed_plan>
