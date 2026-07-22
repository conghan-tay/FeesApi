# Build Plan #4: Domain Vocabulary And Pure Helpers

## Summary
Add the replay-safe domain vocabulary for the Fees workflow without implementing workflow handlers, activities, API behavior, or ledger reads yet. This step creates the types and deterministic helpers that later steps will consume.

## Key Changes
- Add `fees/types.go` with package-local Temporal/domain types only:
  - `BillStatus` enum: `OPEN`, `DRAINING`, `CLOSING`, `CLOSED`.
  - `String()` returns `"OPEN"`, `"DRAINING"`, `"CLOSING"`, `"CLOSED"`, and `"UNKNOWN"` for invalid values.
  - `acceptsAccruals()` returns `true` for `OPEN` and `DRAINING`; `false` for `CLOSING`, `CLOSED`, and unknown statuses.
  - `LineItem`, `LineItemResult`, `CloseSignal`, `BillInput`, `BillView`, `BillState`, and `LedgerRow` exactly as Temporal-boundary/domain structs, with no JSON tags and `AmountMinor int64`.
- Add `fees/helpers.go` with deterministic pure helpers:
  - `billID(clientID, currency, period)` returns `bill-{clientID}-{currency}-{period}`.
  - `resolvePeriodEnd(period)` parses strict `YYYY-MM`, returns first instant of next month at `00:00:00 UTC`, and panics on malformed input because API validation is the precondition.
  - `newBillState(input)` defaults to `OPEN` unless `CarriedStatus` is non-zero.
  - `(*BillState).toView()` returns identity plus `Status.String()`, with no totals or item counts.
  - `(*BillState).carryForward()` preserves identity and lifecycle status only.
  - `ledgerRow(state, item)` derives `BillID` from state and copies line-item fields through without validation.
- Keep existing scaffold API, worker, service, and schema files unchanged except for compile interactions if needed.

## Tests
- Add `fees/helpers_test.go` using standard Go tests.
- Cover `resolvePeriodEnd` for normal month rollover, December-to-January rollover, leap-year February, UTC location, and malformed-period panic.
- Cover `billID` formatting.
- Cover `BillStatus.String()` and `acceptsAccruals()` for every enum value plus an unknown value.
- Cover `newBillState`, `toView`, `carryForward`, and `ledgerRow` field mapping.
- Verify from `fees-api/` with `encore test ./...`; expected baseline remains green, with E2E still skipped unless `PAVEBANK_E2E=1`.

## Assumptions
- The v2 PRD and `docs/workflow.go` are authoritative for this step over the current scaffolded implementation.
- `DRAINING` accepts accruals for this step, matching the PRD state-machine text and helper comment, even though one later close-path comment in `docs/workflow.go` appears to drift.
- No currency validation, amount parsing, Temporal registration, Activity logic, API DTOs, or ledger read behavior is added in Build Plan #4.
