# Move Bill Sealing to the API

## Summary

- Remove `ActivityPersistInvoice` and make explicit close persist the CLOSED ledger state after Temporal workflow completion.
- Change `POST /v1/bills/:billId/close` to return `200 {"success":true}` instead of invoice facts.
- Centralize the idempotent `OPEN -> CLOSED` SQL in the private `SealBill` Encore API.
- Preserve period-end auto-close with timer-only `ActivityAutoCloseBill`, which calls `SealBill` and retries transient failures.
- Make a Temporal replay hard cut; no existing `BillWorkflow` history compatibility is retained.

## Behavior

- Explicit close performs ledger preflight, signals `closeBill`, waits for the workflow to complete, calls `SealBill`, and reports success only after Postgres confirms CLOSED.
- Re-closing an already-CLOSED bill returns success without calling Temporal and does not change `closed_at`.
- Temporal `NotFound` during Signal or result wait falls through to `SealBill`. This heals a completed-workflow/OPEN-ledger split, but can also seal an orphan OPEN row without proving workflow draining occurred.
- Other Temporal and database failures remain redacted `503 close-unavailable` responses. Missing ledger bills remain `404 bill-not-found`.
- The workflow records whether close was explicit or timer-driven, drains in-flight line-item pipelines, and enters CLOSING. Only the timer path invokes `ActivityAutoCloseBill`; CLOSED is set after that Activity succeeds.

## Compatibility and Failure Effects

- Close clients and generated Encore interfaces must migrate from `InvoiceResource` to `CloseBillResponse{Success bool}`. Callers needing totals or items must use `GET /v1/bills/:billId?includeLineItems=true`.
- PRD v3 F3 and the former F10 identical-invoice-body response are superseded. Close remains idempotent at the lifecycle level.
- Explicit close has a request-bound persistence window: Temporal can complete before the API seals Postgres. A canceled request or transient database failure can leave the row OPEN until close is retried.
- The existing Pub/Sub subscriber may persist or finalize line items after closure, so itemized GET remains eventually consistent.
- Active or retained histories that expect `ActivityPersistInvoice` are unsupported and must be discarded before deployment.
- No schema migration is required.

## Verification

- Private seal tests cover fresh, repeated, missing, concurrent, and database-failure cases.
- API tests cover the exact success body, already-CLOSED behavior, Temporal failures, and NotFound recovery.
- Workflow tests distinguish explicit from timer close, verify drain ordering, Activity retries, and the CLOSING state while sealing is in flight.
- Activity and worker tests verify the typed `SealBill` call, error classification, registration of `ActivityAutoCloseBill`, and removal of `ActivityPersistInvoice`.
- The E2E lifecycle verifies close/re-close success and reads invoice facts through GET.

