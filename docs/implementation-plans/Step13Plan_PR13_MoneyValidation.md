# Build Plan #13: Money-Table-Driven Currency Validation

## Summary
Implement supported-currency validation from the existing `currencies` reference table for `POST /v1/bills` only. Keep add-line-item behavior as-is: it validates currency shape at the API boundary, then the workflow validator enforces item currency equals bill currency.

## Key Changes
- Add a ledger helper that checks `currencies` membership:
  - Query: `SELECT EXISTS (SELECT 1 FROM currencies WHERE code = $1)`.
  - Use it after `validateOpenBillRequest` passes regex/period/client validation.
  - If no row exists, return `400 unsupported-currency` and do not call Temporal.
  - If the lookup itself fails, return `503 open-unavailable` with internal details redacted and logged.

- Update `OpenBill` flow:
  - Preserve current order for syntactic validation first.
  - Run supported-currency lookup before elapsed-period validation and before `UpdateWithStartWorkflow`.
  - Leave supported set table-driven from migration seed rows, currently `GEL` and `USD`.

- Do not add supported-currency lookup to `AddLineItem`.
  - Keep `validateAddLineItemRequest` checking only uppercase three-letter format.
  - Keep unsupported-but-uppercase item currencies reaching the workflow, where mismatch against the bill currency returns existing `400 currency-mismatch`.
  - No schema changes, no FK from `bills.currency` to `currencies.code`, and no arithmetic/display use of `exponent`.

## Test Plan
- Add API test: opening with uppercase unsupported currency, e.g. `EUR`, returns `400 unsupported-currency` and makes zero Temporal calls.
- Extend existing open validation coverage to distinguish malformed currency (`usd` -> `400 invalid-request`) from unsupported currency (`EUR` -> `400 unsupported-currency`).
- Add helper-level or integration coverage that confirms `USD` and `GEL` are supported and an unseeded code is not.
- Keep add-line-item tests unchanged, except optionally add an explicit uppercase unsupported item currency case against a USD bill and assert it maps through existing workflow mismatch behavior.
- Verify with `encore test ./fees -run 'TestOpenBill|TestAddLineItemValidation|TestCurrenciesSeedRows'`, then `encore test ./...`.

## Assumptions
- PRD v3 is authoritative, but your scope refinement overrides Step #13’s older “gate open/add” wording.
- `currencies` remains a reference table seeded by migrations; this change consumes it but does not expand the seed set.
- Public error type for this feature is `unsupported-currency`, matching the PRD wording “return 400 unsupported-currency.”
