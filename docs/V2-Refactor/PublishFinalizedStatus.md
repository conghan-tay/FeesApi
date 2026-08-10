# Publish FINALIZED Status After Line-Item Persistence

## Summary

- Add `ActivityPublishFinalized` and execute it in the existing `workflow.Go` sequence: `PENDING → persist → FINALIZED`.
- Publish `FINALIZED` after every successful persistence activity, including idempotent duplicates returning `applied=false`.
- Preserve all HTTP contracts, ledger behavior, validation, and existing PENDING publication behavior.
- Treat the V2 refactor documents and current code as authoritative where they supersede the original PRD. :codex-file-citation{path="/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/docs/PaveBank_Fees_API_PRD_v3.docx" purpose="source" artifact_kind="document"}

## Implementation Changes

- In [activities.go](/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/fees-api-V2/fees/activities.go):
  - Add the exported activity name `ActivityPublishFinalized`.
  - Add `Activities.ActivityPublishFinalized(context.Context, LedgerRow) error`.
  - Preserve the exact `LedgerRow` mapping and signed `int64` string formatting used by `ActivityPublishPending`, changing only the status to `charge.LineItemStatusFinalized` and using finalized-specific error context.
  - Leave `ActivityPublishPending` unchanged to minimize regression risk.
- In [workflow.go](/Users/conghantay/Desktop/PaveBankCodingChallenge/FeesApi/fees-api-V2/fees/workflow.go):
  - Execute `ActivityPublishFinalized` only after `ActivityPersistLineItem` returns successfully.
  - Use the same 30-second timeout and five-attempt exponential retry policy as PENDING publication.
  - Run it for both fresh inserts and duplicate no-ops.
  - Do not run it when PENDING publication or persistence fails.
  - If FINALIZED publication exhausts retries, log the failure, retain the persisted row, release the in-flight handler, and allow closing to proceed.
  - Keep the handler in flight through FINALIZED publication so invoice sealing waits for the terminal callback attempt.
- No HTTP DTO, Charge endpoint, database migration, Encore-generated file, or external E2E-client change is required.

## Compatibility and Failure Effects

- External HTTP behavior remains unchanged; `PublishLineItemStatus` already accepts `FINALIZED`.
- This is a Temporal replay hard cut. Histories that already traversed the persistence path without the new Activity may become non-deterministic on replay. Deployment therefore assumes no active or retained affected histories require compatibility.
- The callback is at-least-once: retries or duplicate add signals can publish repeated `FINALIZED` notifications.
- A permanent callback failure leaves a durable `FINALIZED` ledger row without a successful terminal notification; there is no rollback, outbox, or reconciliation mechanism in this PR.
- FINALIZED retries add latency and can delay bill closure until their five attempts are exhausted.
- Existing workflow tests will initially fail unless the new activity is registered and given a default mock.

## Test Plan

- Activity tests:
  - Verify positive, zero, and negative amounts are formatted as decimal strings.
  - Verify every payload field is preserved and status is exactly `FINALIZED`.
  - Verify publisher errors are wrapped and missing publishers are rejected.
- Workflow tests:
  - Assert strict ordering: PENDING completes before persistence, and persistence completes before FINALIZED.
  - Assert fresh and duplicate successful persistence both publish FINALIZED.
  - Assert PENDING or persistence failure prevents FINALIZED publication.
  - Assert FINALIZED failure is attempted five times, does not repeat persistence, and still permits closure.
  - Assert close waits for in-flight FINALIZED publication.
  - Preserve mismatch, draining, auto-close, concurrency, and Continue-As-New coverage.
- Worker tests:
  - Verify `ActivityPublishFinalized` is available through the registered `Activities` method set.
- Verification:
  - Run `encore test ./...`.
  - Run `git diff --check`.

## Assumptions

- Duplicate signals intentionally republish both PENDING and FINALIZED.
- FINALIZED publication failure is logged and tolerated after retries.
- No affected Temporal history requires replay compatibility.
- The current green baseline is preserved: `encore test ./...` presently passes for all packages.
