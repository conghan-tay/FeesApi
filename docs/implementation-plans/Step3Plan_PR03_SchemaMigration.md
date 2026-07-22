# Build Plan #3: Schema Migration

## Summary
Add the first Encore Postgres migration for the fees ledger schema, matching the PRD/LEDGER contract exactly: `bills`, `line_items`, indexes, and seeded `currencies` reference rows. Do not implement activities, workflow logic, read queries, or new API endpoints in this step.

## Key Changes
- Add `fees/migrations/1_create_ledger_schema.up.sql`.
- Create `bills` with:
  - `bill_id TEXT PRIMARY KEY`
  - `client_id TEXT NOT NULL`
  - `currency CHAR(3) NOT NULL`
  - `period TEXT NOT NULL`
  - `status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED'))`
  - `opened_at TIMESTAMPTZ NOT NULL DEFAULT now()`
  - `closed_at TIMESTAMPTZ`
  - `UNIQUE (client_id, currency, period)`
- Create `line_items` with:
  - `id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`
  - `bill_id TEXT NOT NULL REFERENCES bills(bill_id)`
  - `reference TEXT NOT NULL`
  - `amount_minor BIGINT NOT NULL`
  - `currency CHAR(3) NOT NULL`
  - `fee_type TEXT NOT NULL`
  - `description TEXT NOT NULL DEFAULT ''`
  - `applied_at TIMESTAMPTZ NOT NULL DEFAULT now()`
  - `UNIQUE (bill_id, reference)`
- Create `currencies` with:
  - `code CHAR(3) PRIMARY KEY`
  - `exponent SMALLINT NOT NULL`
  - `display_name TEXT NOT NULL DEFAULT ''`
  - Seed exactly `GEL` and `USD`, both exponent `2`.

## Indexes
- Add `idx_bills_client_status ON bills (client_id, status)`.
- Add `idx_bills_period ON bills (period)`.
- Add `idx_bills_currency ON bills (currency)`.
- Add `idx_line_items_bill ON line_items (bill_id)`.
- Do not add an `invoices` table.

## Tests
- Add `fees/schema_test.go` with catalog-level tests run by `encore test`.
- Assert all three tables exist.
- Assert required columns exist with expected nullability/default-sensitive behavior where practical.
- Assert primary keys, foreign key, status check, and both unique constraints exist.
- Assert expected indexes exist by name.
- Assert `currencies` contains `GEL` and `USD` with exponent `2`.
- Run verification from `fees-api/`:
  - `encore test -v ./...`
  - Expected: default unit suite passes; E2E and live Temporal tests remain skipped unless their env vars are set.

## Assumptions
- Scope is strictly schema migration and schema tests.
- Currency reference data is not yet used for validation; that remains Build Plan #13.
- Ledger reads, computed totals, activity persistence, and endpoint behavior land in later build steps.
