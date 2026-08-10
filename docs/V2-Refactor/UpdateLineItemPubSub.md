# Add Line-Item Status Pub/Sub Fan-Out

## Summary

- Keep the existing HTTP and Temporal status-publication flow, but have `charge.PublishLineItemStatus` publish every validated request to Encore Pub/Sub.
- Create topic `update-line-items` with `pubsub.AtLeastOnce` and no ordering attribute.
- Subscribe from `fees` using `update-line-item-ledger`; the handler only writes a structured log and acknowledges the event.
- No database, workflow, API request, or successful response changes.

## Implementation Changes

- In `charge`, add an explicit `LineItemEvent` struct duplicating all seven fields, Go types, and JSON tags from `PublishLineItemStatusRequest`.
- Declare the topic at package level and expose it for the cross-service subscription.
- Add a publisher dependency to the Charge service, initialized using a publisher-only topic reference so publish failures can be tested.
- After request validation:
  - Map every request field into a new `LineItemEvent`.
  - Publish using the request context.
  - On success, retain the empty `200 OK` response and log the message ID, bill ID, reference, and status.
  - On failure or a missing publisher, log the internal error and return redacted `503 line-item-status-unavailable` with a retry-safe message.
- In `fees`, declare the subscription at package level. Its handler logs all event fields—including description—and returns `nil`; it must not call Temporal, write to PostgreSQL, or republish.
- Do not edit Encore-generated files, migrations, the PRD, or existing V2 refactor documents.

## Compatibility and Failure Effects

- The API remains source- and wire-compatible on successful requests, but valid requests can now return `503` and incur broker latency where they previously always returned `200`.
- This affects the existing workflow:
  - A Pub/Sub outage during `PENDING` publication triggers the current five Temporal Activity attempts; exhaustion skips line-item persistence even though `AddLineItem` previously returned `202`.
  - An outage during `FINALIZED` publication triggers five attempts and delays closure; the already-persisted ledger row remains intact.
- Publish errors are ambiguous: the broker may have accepted an event even when the caller receives `503`, so retries can produce duplicates.
- At-least-once delivery can also redeliver events. This is harmless for the log-only handler, but any future ledger mutation must be idempotent.
- Because the topic is deliberately unordered, `FINALIZED` can be observed before `PENDING`. A future state-writing handler must enforce monotonic status transitions rather than applying arrival order.
- No import cycle, database migration, generated API-interface change, or event-processing feedback loop is introduced.

## Test Plan

- Charge tests:
  - Verify the event and request structs have identical field names, field types, and JSON tags to prevent duplicated-contract drift.
  - Verify each accepted status and signed amount publishes exactly one fully mapped event and returns an empty `200`.
  - Verify every validation failure publishes nothing.
  - Verify publisher errors and a missing publisher return redacted `503 line-item-status-unavailable`.
  - Assert topic metadata is `update-line-items`, at-least-once, with an empty ordering attribute.
- Fees tests:
  - Assert subscription metadata identifies `update-line-item-ledger` and the correct topic.
  - Invoke the handler with a representative event and verify it returns `nil`.
- Runtime verification:
  - Call `POST /v1/line-item-status`, wait for `update-line-item-ledger` to process the matching event, and verify the exact payload and successful handler outcome.
  - Run `encore check`, `encore test ./...`, and `git diff --check`.
- Current baseline note: `encore test ./...` is presently blocked because the local Docker daemon is not running; plain `go test` is not a valid substitute for Encore resource tests.

## Assumptions

- Topic and subscription names are permanent once deployed.
- Default subscription retry and retention settings are acceptable because the current handler always acknowledges successfully.
- Logging the complete event, including `description`, is acceptable for this PR.
