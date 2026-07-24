# Step 10: F6/F7 Ledger Reads

## Summary
Implement public ledger-backed reads for `GET /v1/bills/:billId` and `GET /v1/bills`. Reads bypass Temporal entirely, compute `totalMinorAmount` and `itemCount` from `line_items`, and work for both `OPEN` and `CLOSED` bills.

## Key Changes
- Add raw `GET /v1/bills/:billId` endpoint:
  - `includeLineItems=false` by default returns `BillResource`.
  - `includeLineItems=true` returns the same bill fields plus `lineItems`, ordered by `line_items.id`.
  - Missing bill returns `404 bill-not-found`; invalid `includeLineItems` returns `400 invalid-request`.

- Replace scaffold `GET /v1/bills` with real ledger list:
  - Support optional filters: `clientId`, `status`, `currency`, `period`.
  - Validate `status` as `OPEN|CLOSED`, `currency` as uppercase 3-letter code, and `period` as `YYYY-MM`.
  - Never inline line items in list responses.
  - Default `limit=50`, cap at `200`; invalid limit/cursor returns `400 invalid-request`.

- Add cursor pagination:
  - Sort by `opened_at DESC, bill_id DESC`.
  - Fetch `limit+1` rows to compute `hasMore`.
  - Cursor is opaque base64url JSON containing `{ "openedAt": "<RFC3339Nano>", "billId": "<id>" }`.
  - Apply cursor with keyset condition `(opened_at, bill_id) < (cursor.openedAt, cursor.billId)`.

- Refactor read helpers out of `api.go` into a ledger-read helper area, preferably `fees/store.go`:
  - Keep existing `readBillResource` behavior for close/open reuse.
  - Add reusable `readBillLineItems`, `readBillWithLineItemsResource`, and `listBillResources`.
  - Preserve current close semantics: `readClosedInvoiceResource` must still require `CLOSED`.

## Tests
- Replace `TestListBillsReturnsEmptyScaffoldResponse` with real list tests covering:
  - Empty list.
  - Combined filters by `clientId`, `status`, `currency`, and `period`.
  - Computed total/count from multiple line items, including negative credit rows.
  - Pagination limit, `hasMore`, `nextCursor`, and second-page retrieval.
  - Bad filter/limit/cursor returns `400`.

- Add GET tests covering:
  - Missing bill returns `404`.
  - Open bill returns computed running total without Temporal.
  - `includeLineItems=true` includes ordered line items.
  - `includeLineItems=false` omits `lineItems`.
  - Closed bill still reads correctly with no Temporal client, simulating post-retention.

- Run verification:
  - `encore test -v ./...`
  - Optional after local Temporal + Encore are running: `PAVEBANK_E2E=1 go test -v ./e2e -run TestFeesLifecycleE2E -count=1`

## Assumptions
- Cursor ordering is newest-opened first: `opened_at DESC, bill_id DESC`.
- Cursor format is intentionally opaque; clients must only echo `nextCursor`.
- List responses do not include `lineItems`, matching `API_CONTRACTS.md`.
- No schema migration is needed; existing tables and indexes already support Step 10.
