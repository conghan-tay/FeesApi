# Build Plan #8: F2 Add-Line-Item Endpoint

## Summary
Add `POST /v1/bills/:billId/line-items` as the public API shell over the already-implemented `UpdateAddLineItem` workflow update. Current baseline is clean: `encore test ./...` passes. No schema, workflow, or activity behavior changes are needed for F2 unless compilation exposes a narrow interface issue.

## Key Changes
- Add wire DTOs in `fees/api.go`:
  - Request: `{ "reference", "minorAmount", "currency", "feeType", "description" }`.
  - Response: `{ "reference", "applied" }`.
  - Keep domain structs JSON-tag-free; translate `minorAmount` string to `LineItem.AmountMinor int64`.

- Add raw Encore endpoint:
  - Annotation: `//encore:api public raw method=POST path=/v1/bills/:billId/line-items`.
  - Read `billId` from `encore.CurrentRequest().PathParams.Get("billId")`.
  - Decode JSON with unknown fields rejected.
  - Validate: non-empty `billId`, `reference`, `minorAmount`, `currency`, `feeType`; `currency` matches `^[A-Z]{3}$`; `minorAmount` parses as base-10 `int64`; negative amounts are allowed; `description` defaults to `""`.
  - Do not validate supported currencies from the `currencies` table in this step.

- Extend Temporal client abstraction:
  - Add `UpdateWorkflow(ctx, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)` to `temporalClient`.
  - Call with `WorkflowID: billId`, `UpdateName: UpdateAddLineItem`, `Args: []interface{}{lineItem}`, `WaitForStage: client.WorkflowUpdateStageCompleted`.
  - Leave `UpdateID` blank; idempotency remains `(bill_id, reference)` in the ledger.

- Implement exact HTTP mapping:
  - `Applied=true` -> `201 Created` with response body.
  - `Applied=false` -> `200 OK` with response body.
  - malformed/invalid request -> `400 invalid-request`.
  - `CurrencyMismatch` Temporal application error -> `400 currency-mismatch`.
  - `BillNotOpen` Temporal application error -> `409 bill-not-open`.
  - `serviceerror.NotFound` from `UpdateWorkflow` or `handle.Get` -> `404 no-open-bill`.
  - nil/missing Temporal client or other Temporal/update failures -> `503 add-line-item-unavailable`, with raw internal error details redacted.
  - Use existing RFC 9457 `writeProblem` helper and JSON content-type behavior.

- Update support files only where directly relevant:
  - Update fake Temporal clients in existing tests to satisfy the extended interface.
  - Update README build progress to mark Step #8 complete and note add-line-item status behavior.

## Test Plan
- Add API tests for fresh and duplicate additions:
  - Assert `201` vs `200`, lowercase JSON response fields, and captured `UpdateWorkflowOptions`.
  - Assert `LineItem` argument has parsed `AmountMinor`, request currency, fee type, description, and reference.

- Add API tests for validation:
  - malformed JSON, unknown field, missing reference, missing/invalid/overflow `minorAmount`, lowercase currency, missing fee type.
  - Assert no Temporal call for invalid input.

- Add API tests for error mapping:
  - direct `serviceerror.NotFound` -> `404 no-open-bill`.
  - `CurrencyMismatch` application error from `handle.Get` -> `400`.
  - `BillNotOpen` application error from `handle.Get` -> `409`.
  - generic direct/update-handle errors -> `503` with redacted details.
  - nil Temporal client -> `503`.

- Run verification:
  - `gofmt` on touched Go files.
  - `encore test ./...`.
  - Optional live check after close/read endpoints exist: `PAVEBANK_E2E=1 go test -v ./e2e`.

## Assumptions
- `PaveBank_Fees_API_PRD_v3.docx` and `API_CONTRACTS.md` are authoritative for F2.
- The current workflow/activity implementation already satisfies F2’s Temporal/ledger contract.
- Existing current-code behavior that rejects new adds during `DRAINING` remains unchanged for this step despite PRD narrative drift; the endpoint maps all `BillNotOpen` outcomes to `409`.
- No `Location` header is added for `201` line-item creation because there is no standalone line-item GET endpoint in the current contract.
