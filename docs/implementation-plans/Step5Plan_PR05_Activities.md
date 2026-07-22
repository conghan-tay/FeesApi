# Build Plan #5: Activities

## Summary
Implement the write-side ledger Activities for the Fees API: idempotent line-item persistence, idempotent bill sealing, and worker registration. Keep scope limited to Build Plan #5; no API endpoints or real `BillWorkflow` implementation yet.

## Key Changes
- Add `fees/activities.go` with:
  - `const ActivityPersistLineItem = "ActivityPersistLineItem"` and `ActivityPersistInvoice = "ActivityPersistInvoice"` for the upcoming workflow to call by stable Temporal activity type name.
  - `type Activities struct { db *sqldb.Database }` and `NewActivities(db *sqldb.Database) *Activities`, matching the project’s existing Encore `sqldb` setup.
  - `temporalNonRetryable(err error) error`, using Temporal non-retryable application errors with type `"BillNotOpen"`.
- Implement `(*Activities).ActivityPersistLineItem(ctx, LedgerRow) (bool, error)`:
  - Run one conditional insert into `line_items` using `WHERE EXISTS (SELECT 1 FROM bills WHERE bill_id=$1 AND status='OPEN')`.
  - Use `ON CONFLICT (bill_id, reference) DO NOTHING`.
  - Return `true` when a row inserts.
  - On zero rows, query whether `(bill_id, reference)` already exists; return `false, nil` for duplicate idempotent replay.
  - If no duplicate exists, return non-retryable `"BillNotOpen"` for closed or missing bill.
- Implement `(*Activities).ActivityPersistInvoice(ctx, billID string) (BillView, error)`:
  - `UPDATE bills SET status='CLOSED', closed_at=now() WHERE bill_id=$1 AND status='OPEN' RETURNING client_id, currency, period, status`.
  - If no row updates, read the existing bill; return it for already-closed idempotent close.
  - If no bill exists, return non-retryable `"BillNotOpen"`.
  - Do not compute or store totals; totals remain read-side `SUM(line_items)` work for later steps.
- Update worker registration:
  - Keep scaffold registration intact unless removing it is already part of a later plan.
  - Register `NewActivities(db)` with empty `activity.RegisterOptions{}` so Temporal registers method names exactly as `ActivityPersistLineItem` and `ActivityPersistInvoice`.

## Test Plan
- Add DB-backed activity tests under `fees` and run with `encore test ./...`.
- Cover `ActivityPersistLineItem`:
  - Fresh insert against an `OPEN` bill returns `applied=true` and creates exactly one row.
  - Duplicate reference returns `applied=false` and does not add another row.
  - Closed bill returns a non-retryable `"BillNotOpen"` error and inserts nothing.
  - Missing bill follows the same non-retryable zero-row branch.
  - Negative `amount_minor` is accepted to preserve append-only credit behavior.
- Cover `ActivityPersistInvoice`:
  - Open bill seals to `CLOSED`, sets `closed_at`, and returns identity/status.
  - Re-closing an already closed bill returns the existing bill without changing `closed_at`.
  - Missing bill returns non-retryable `"BillNotOpen"`.
- Update worker tests to assert the real `Activities` struct is registered in addition to the scaffold activity.
- Verification command: `encore test ./...`. Plain `go test ./...` is not the acceptance command because Encore `sqldb.NewDatabase` panics outside the Encore runner.

## Assumptions
- Use Encore’s existing package-level `db` from `fees/db.go`; do not introduce `pgxpool` or a new dependency.
- Activity type names are the unprefixed exported method names, per Temporal Go SDK struct registration behavior.
- Step #5 does not implement ledger reads, API handlers, or `BillWorkflow`; it only creates the Activity layer those later steps consume.
