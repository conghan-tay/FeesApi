# Refactor Fees API To Idiomatic Encore Typed Endpoints

## Summary
Convert the public Fees API from raw `net/http` handlers to Encore typed endpoints so the Encore dashboard can generate schemas and enable `Call API`. Drop RFC 9457 behavior in code, use Encore `errs.Error`, and map elapsed-period from `422` to `400 InvalidArgument`. Do not update docs in this pass.

## Public API And Interface Changes
- Replace all `//encore:api public raw ...` handlers with typed Encore handlers:
  - `OpenBill(ctx, req *OpenBillRequest) (*OpenBillResponse, error)`
  - `AddLineItem(ctx, billId string, req *AddLineItemRequest) (*AddLineItemResponse, error)`
  - `CloseBill(ctx, billId string, req *CloseBillRequest) (*InvoiceResource, error)`
  - `GetBill(ctx, billId string, req *GetBillRequest) (*GetBillResponse, error)`
  - `ListBills(ctx, req *ListBillsRequest) (*ListBillsResponse, error)`
- Add typed response support:
  - `OpenBillResponse` includes bill fields plus `Location string 'header:"Location"'` and `HTTPStatus int 'encore:"httpstatus"'` set to `201`.
  - `AddLineItemResponse` keeps `reference`/`applied` plus `HTTPStatus int 'encore:"httpstatus"'`, set to `201` for fresh rows and `200` for duplicates.
  - `GetBillRequest` uses `IncludeLineItems bool 'query:"includeLineItems"'`.
  - `ListBillsRequest` uses query tags for `clientId`, `status`, `currency`, `period`, `cursor`, and `limit`.
- Remove `problemResponse`, `writeProblem`, `writeJSON`, `currentBillID`, raw request parsing, and direct JSON decoding from endpoint logic.

## Error Model
- Add `APIErrorDetails` with `Type string 'json:"type"'` and an `ErrDetails()` marker method.
- Add `apiError(code errs.ErrCode, typ, message string, metaPairs ...any) error` for Encore-native errors.
- Map domain failures to Encore codes:
  - `invalid-request`, `period-elapsed`, `unsupported-currency`, `currency-mismatch` -> `errs.InvalidArgument` / HTTP `400`.
  - `bill-already-open` -> `errs.AlreadyExists` / HTTP `409`.
  - `bill-not-open` and `bill-closed` -> `errs.Aborted` / HTTP `409`.
  - `bill-not-found`, `no-bill`, `no-open-bill` -> `errs.NotFound` / HTTP `404`.
  - `open-unavailable`, `add-line-item-unavailable`, `close-unavailable`, `read-unavailable` -> `errs.Unavailable` / HTTP `503`.
  - unexpected internal invariant failures -> `errs.Internal` / HTTP `500`.
- Keep raw upstream errors in logs or `Meta`, not external `Message`/`Details`.

## Implementation Changes
- Refactor `fees/api.go` handlers to typed Encore signatures and use `ctx` directly.
- Keep business behavior unchanged:
  - open validates input, checks supported currency, rejects elapsed periods as `400`, starts `UpdateWithStartWorkflow`, returns `201` with `Location`.
  - add-line-item calls `UpdateWorkflow` and returns `201` or `200` from `Applied`.
  - close reads ledger first, signals/waits when open, returns ledger invoice.
  - get/list read from Postgres only.
- Update tests and E2E client code only as required for the new Encore error shape.
- Do not update `docs/API_CONTRACTS.md`, README, or docs-only E2E wording in this pass.

## Test Plan
- Convert API tests from `httptest.ResponseRecorder` helpers to direct typed endpoint calls.
- Replace `assertProblem` helpers with `assertEncoreError(t, err, wantCode, wantType)` using `errors.As`, `errs.Code(err)`, and `errs.Details(err)`.
- Update success tests for `OpenBillResponse.HTTPStatus`, `Location`, and add-line-item `HTTPStatus`.
- Update failure tests so elapsed period asserts `errs.InvalidArgument` and details type `period-elapsed`.
- Remove raw malformed JSON / unknown-field handler tests; Encore owns typed request decoding.
- Run:
  - `encore check`
  - `encore test ./...`
  - mandatory live E2E after implementation: start/confirm Temporal and `encore run`, then run `PAVEBANK_E2E=1 PAVEBANK_API_BASE_URL=http://localhost:4000 go test -v ./e2e -count=1`
- Escalate permissions for live E2E commands that need unsandboxed networking/process access, including `encore run`, Temporal startup if needed, and the E2E test command.

## Assumptions
- Idiomatic Encore compatibility is now more important than the old exact wire contract.
- `400` is acceptable for elapsed periods.
- Unknown JSON field handling can follow Encore’s typed endpoint behavior.
- Clients branch on Encore `code` plus `details.type`, not RFC 9457 `problem.type`.
