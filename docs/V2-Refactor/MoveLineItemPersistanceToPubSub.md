# Move Line-Item Persistence to Ordered Pub/Sub

## Summary

- Replace `ActivityPersistLineItem` in the workflow pipeline with `ActivityLongRunning`: `PublishPending → LongRunning → PublishFinalized`.
- Persist PENDING and FINALIZED status changes asynchronously in the Fees Pub/Sub subscriber.
- Order events per `(billId, reference)` using a derived `OrderingID`.
- Keep `/v1/line-item-status` public and accept eventual ledger consistency, including writes after bill closure.

## Implementation Changes

- Extend `LineItemEvent` with idiomatic Go field `OrderingID`, serialized as `orderingId` and tagged `pubsub-attr:"ordering-id"`.
- Populate it exclusively in `PublishLineItemStatus` as `BillID + "-" + Reference`; callers do not supply it.
- Configure `update-line-items` with `OrderingAttribute: "ordering-id"` while retaining `AtLeastOnce`.
- Remove `ActivityPersistLineItem` and add `ActivityLongRunning(context.Context, LedgerRow) error`.
  - Run a context-cancellable simulated external transaction for a uniformly random continuous duration from zero through two seconds.
  - Keep the operation injectable so tests do not sleep and can verify input, cancellation, and failure propagation.
  - Retain the existing Activity timeout/retry policy and skip FINALIZED publication if the simulation ultimately fails.
- Update the subscriber:
  - PENDING parses `MinorAmount` to `int64` and inserts a PENDING row with `ON CONFLICT (bill_id, reference) DO NOTHING`.
  - Permit PENDING insertion for OPEN or CLOSED bills; the existing foreign key still rejects missing bills.
  - Duplicate/conflicting PENDING messages are successful no-ops, preserving the first stored payload and never regressing FINALIZED.
  - FINALIZED updates only the status for the matching `(bill_id, reference)` and leaves `applied_at` unchanged; repeated updates succeed.
  - Missing-row or database failures return errors for Pub/Sub retry; FAILED and unexpected statuses are logged and acknowledged without mutation.
- Change GET/LIST/detail aggregation to count and sum only FINALIZED rows while continuing to return PENDING, FINALIZED, and FAILED rows in itemized responses.

## Compatibility and Failure Effects

- This is a Temporal replay hard cut: existing histories referencing `ActivityPersistLineItem` or expecting the former Activity sequence can become non-deterministic, and outstanding old Activity tasks cannot run.
- Closed invoices are no longer immutable snapshots. Close may return before Pub/Sub inserts or finalizes rows; later GET or repeated close responses can contain different items and totals.
- `itemCount` now means finalized-item count and may differ from `len(lineItems)`, since itemized responses retain non-finalized rows.
- The public unauthenticated status endpoint can now cause ledger mutations without workflow bill-state or currency checks.
- A missing bill or missing PENDING predecessor causes retries and blocks later events for that ordering key until success or dead-lettering.
- At-least-once delivery still permits duplicates; ordering is only per `OrderingID`, and Encore ordering is not enforced in local development.
- Changing an already-deployed unordered topic to ordered may require provider-side resource replacement. Existing queued messages lack `OrderingID`, so deployment assumes no backlog or retained topic state requiring compatibility.
- Concatenating unrestricted hyphenated IDs can create ordering-key collisions or exceed provider limits; collisions only reduce concurrency, while excessive length can cause publication failure.

## Test Plan

- Workflow tests assert strict `PENDING → LongRunning → FINALIZED` ordering, replacement of all old Activity mocks, failure behavior, concurrent simulations, drain-before-close, and updated worker registration.
- Activity tests verify the simulated operation receives the exact row, uses a continuous 0–2 second delay, responds to context cancellation, and propagates injected failures without real sleeps.
- Subscriber/database tests cover fresh and duplicate PENDING inserts, conflicting duplicates, negative amounts, late inserts after closure, missing bills, FINALIZED transitions and redelivery, missing predecessors, and log-only FAILED handling.
- API/Pub/Sub tests verify exact `OrderingID` derivation, JSON and Pub/Sub attribute tags, at-least-once ordered topic metadata, validation-before-publish, and complete event mapping.
- Read-path tests verify finalized-only totals/counts while all statuses remain visible.
- Run `encore test ./...`, `encore check`, `git diff --check`, and the full Temporal-backed E2E suite with polling for eventual finalized ledger state.

## Assumptions

- No active or retained Temporal execution requires replay compatibility.
- No schema migration is needed; existing status, foreign-key, and unique constraints remain authoritative.
- First PENDING payload wins for a duplicate `(bill_id, reference)`.
- Event publication completion does not imply subscriber completion, by explicit design choice.
