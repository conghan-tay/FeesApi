# Pave Bank Fees API — Activities & Postgres Ledger

The persistence layer behind `workflow.go`. This is the concrete realization of decision **D6**: *Temporal orchestrates; the ledger remembers.* The workflow owns live orchestration state during the bill's open month; the Activities in this document write the permanent, queryable system-of-record to Postgres.

Everything here honors the contract `workflow.go` already commits to:

1. **The workflow accumulates nothing.** It owns lifecycle status and delegates every fact to the ledger. The add-line-item Update validator enforces the currency + lifecycle guards, and its handler calls one idempotent Activity — there is no in-memory total, item count, or seen-reference set to advance afterward. The ledger is the sole home of those facts.
2. **Idempotency lives entirely in the DB.** The `(bill_id, reference)` unique constraint plus `INSERT ... ON CONFLICT DO NOTHING` makes a duplicate delivery a no-op. There is no workflow-side fast-path check to keep in sync (and nothing to carry across a Continue-As-New boundary), so there's no check-then-act window and no drift risk.
3. **Total and item count are computed on read.** `SUM(amount_minor)` / `COUNT(*)` over the append-only `line_items`. No stored aggregate exists to double-count or drift; a duplicate insert literally cannot affect the total because the total isn't stored.
4. **Close seals lifecycle only.** `closeBill` calls a single `ActivityPersistInvoice` that flips `OPEN → CLOSED` and stamps `closed_at` — it does *not* freeze a total, because there is no total column. The immutable, tamper-evident total is the `SUM` over the (now historical) line items.
5. **Reads survive Temporal retention.** `GET`/`LIST` (FR6/FR7) hit the ledger, not the workflow, so the schema answers every read path without the workflow alive.

---

## 1. Schema

Two tables. There is no separate `invoices` table — a closed `bills` row *is* the invoice (see §4.1).

```sql
-- ────────────────────────────────────────────────────────────
-- bills: one row per (client, currency, period). The bill key is
-- the natural PK — the same tuple that forms the Temporal workflow
-- ID — so there is exactly one ledger row per workflow.
-- ────────────────────────────────────────────────────────────
CREATE TABLE bills (
    bill_id       TEXT PRIMARY KEY,            -- "bill-acme-USD-2026-07"
    client_id     TEXT        NOT NULL,
    currency      CHAR(3)     NOT NULL,        -- ISO-4217 alpha; bill is single-currency (D4)
    period        TEXT        NOT NULL,        -- calendar-month identifier "2026-07" (D2)
    status        TEXT        NOT NULL         -- 'OPEN' | 'CLOSED' (observable states only)
                  DEFAULT 'OPEN'
                  CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ,                 -- NULL until sealed

    UNIQUE (client_id, currency, period)       -- redundant with the PK's components,
                                               -- but makes LIST filters index-friendly
);

-- NOTE: there is deliberately no total_minor or item_count column. Both are
-- COMPUTED ON READ from line_items (SUM(amount_minor), COUNT(*)) — see §2.1.
-- The bills row carries only identity + lifecycle, so a stored aggregate can
-- never drift from the append-only rows it would summarize.

CREATE INDEX idx_bills_client_status ON bills (client_id, status);
CREATE INDEX idx_bills_period        ON bills (period);
CREATE INDEX idx_bills_currency      ON bills (currency);
```

```sql
-- ────────────────────────────────────────────────────────────
-- line_items: append-only. One row per accepted fee event. The
-- (bill_id, reference) unique constraint IS the idempotency backstop
-- the workflow relies on.
-- ────────────────────────────────────────────────────────────
CREATE TABLE line_items (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bill_id       TEXT        NOT NULL REFERENCES bills(bill_id),
    reference     TEXT        NOT NULL,        -- caller-supplied idempotency key (FR8)
    amount_minor  BIGINT      NOT NULL,        -- integer minor units (D5); may be negative for credits
    currency      CHAR(3)     NOT NULL,        -- must equal bills.currency; stored for audit legibility
    fee_type      TEXT        NOT NULL,
    description   TEXT        NOT NULL DEFAULT '',
    applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (bill_id, reference)                -- ← the real idempotency guard
);

CREATE INDEX idx_line_items_bill ON line_items (bill_id);
```

### Schema notes

**`status` is two states, not four.** `workflow.go` distinguishes `OPEN / DRAINING / CLOSING / CLOSED`, but `DRAINING` and `CLOSING` are orchestration-internal — they sequence the drain-then-seal handoff inside the workflow. The ledger, as the external system-of-record, persists only the two states a caller can observe: accepting items, or sealed. The bill row goes `OPEN` → `CLOSED` in one statement at seal time and never sits between.

**`status` is the one field the workflow owns, not a projection.** The removed columns (`total_minor`, `item_count`) were *projections* of the line-item rows — the ledger held the underlying facts and memory merely mirrored them, so mirroring risked drift and the fix was to delete the mirror. Lifecycle status is the opposite: the running **workflow** is the single writer of lifecycle transitions (it is the only thing that closes a bill), so its in-memory `status` is the *source of truth* and `bills.status` is the durable *projection* of it, written at seal time. That inversion is why `status` stays in workflow memory while the aggregates left: for the aggregates, memory was the stale copy; for status, memory is the authority and the ledger is the copy.

**Transient status-visibility window (by design).** Because the workflow flips its in-memory status to closed *before* `ActivityPersistInvoice` commits `bills.status = 'CLOSED'`, there is a brief interval where the workflow authoritatively rejects new line items (in-memory guard) while a `GET` hitting the ledger still reads `OPEN`. This is correct, not a bug: only the workflow's decision gates writes, so no item is wrongly accepted; the readable status simply catches up the instant the seal commits. The *authoritative* accept/reject state and the *readable* state momentarily disagree, and only the former is load-bearing.

**`line_items.currency` duplicates `bills.currency` deliberately.** The workflow already rejects mismatched-currency items, so this is never the enforcement point — it's audit legibility, so a line-item row is readable in isolation during a dispute without joining back to `bills`. (If you want DB-level enforcement too, a composite FK `(bill_id, currency) REFERENCES bills(bill_id, currency)` does it, but needs a `UNIQUE (bill_id, currency)` on `bills` to support the FK. Left as documentation-grade; the workflow is the enforcement point where the reject-loudly semantics already live.)

**`amount_minor` has no `CHECK (>= 0)`.** Corrections are modeled as separate *credit* line items with negative amounts (append-only; the ledger is never mutated). A non-negativity check would block that correction mechanism.

**No `status` on line items.** They're append-only facts — a persisted row *is* an accepted accrual. Nothing to mutate.

### Index rationale (LIST — FR7: filter by client, status, currency, period)

| Index | Serves | Notes |
|---|---|---|
| `idx_bills_client_status (client_id, status)` | `client_id` alone; `client_id + status` | Composite led by the **high-cardinality/selective** column. `status` alone can't use it (second column of a composite is unreachable without the leading one) — but `status` alone is a near-worthless filter anyway (~half the table). |
| `idx_bills_period (period)` | `period`, alone or combined | Planner picks the most selective available index and applies the rest as filters (or bitmap-ANDs). |
| `idx_bills_currency (currency)` | `currency`, any value | Standalone B-tree. The planner consults per-value statistics and uses it for **selective** values (rare coins, once the currency set grows to include crypto) while ignoring it for high-volume majors (USD) on a query-by-query basis. At today's GEL/USD only, it's low-value, but it's a deliberate, defensible call to carry it for the crypto-expansion roadmap. |

Guiding principle: index columns that are *selective* and *filtered on independently*. A low-cardinality column earns an index only as the refining second column of a composite whose leading column is selective, or when heavily skewed and you query the rare value. If a "show me every OPEN bill across all clients" backoffice sweep ever becomes a real query, prefer a **partial index** `... WHERE status = 'OPEN'` (tiny once most periods have closed) over widening the composite.

---

## 2. Activities

Two Activities, one per `ExecuteActivity` call site in the workflow. Both must be idempotent — Temporal Activities are at-least-once, so a worker can crash after the DB commit but before Temporal records completion, and the Activity re-runs on retry.

```go
package fees

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.temporal.io/sdk/temporal"
)

// Activities holds the dependencies. Registered with the worker as a struct so
// the pool is injected once rather than being package-global.
type Activities struct {
	db *pgxpool.Pool
}

func NewActivities(db *pgxpool.Pool) *Activities {
	return &Activities{db: db}
}

// temporalNonRetryable wraps an error so Temporal will not retry it. Used for
// logic violations that retrying cannot cure (e.g. a line item on a sealed bill).
func temporalNonRetryable(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "BillNotOpen", err)
}
```

### 2.1 `ActivityPersistLineItem`

Insert one line item. That's the whole write — there is no bill-row total to bump, because the total is computed on read (`SUM` over `line_items`). So this Activity is a **single idempotent `INSERT`**, not a two-statement transaction. A duplicate `(bill_id, reference)` hits `ON CONFLICT DO NOTHING` and is a no-op success, so at-least-once retries never double-insert — and because nothing is summed at write time, a duplicate cannot double-count a total either.

**The closed-bill guard, and the ambiguity it introduces.** "Don't accept a new item on a sealed bill" is a lifecycle rule, not something a unique constraint expresses. The workflow's `acceptsAccruals()` check is the authoritative guard (and the workflow is single-writer on close, so it can't race itself). We *also* enforce it at the DB as a backstop, by making the insert conditional on the bill being `OPEN`:

```sql
INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, description)
SELECT $1, $2, $3, $4, $5, $6
 WHERE EXISTS (SELECT 1 FROM bills WHERE bill_id = $1 AND status = 'OPEN')
ON CONFLICT (bill_id, reference) DO NOTHING
```

This is atomic — a concurrent close either commits before the insert (the `EXISTS` fails, item rejected) or after (the item lands, then the seal freezes history). Postgres serializes them; no `SELECT ... FOR UPDATE` needed.

The cost is that **zero rows inserted is ambiguous**: it means *either* the reference already existed (idempotent no-op → success) *or* the bill wasn't OPEN (reject → non-retryable error). `ON CONFLICT DO NOTHING` and a failed `WHERE EXISTS` both yield `RowsAffected() == 0`, and the two demand opposite outcomes. So on a 0-row result the Activity must disambiguate with one follow-up read: does the row exist (→ it was a duplicate, succeed) or not (→ was the bill non-OPEN or absent, fail non-retryably). This extra read happens only on the 0-row path — the common case (a fresh item on an open bill) inserts one row and returns immediately.

```go
// LedgerRow is what workflow.go's ledgerRow(state, li) helper produces.
type LedgerRow struct {
	BillID      string
	Reference   string
	AmountMinor int64
	Currency    string
	FeeType     string
	Description string
}

// ActivityPersistLineItem inserts one line item, conditional on the bill being
// OPEN. There is no bill-row aggregate to update — the total and item count are
// computed on read — so this is a single idempotent statement, not a transaction.
//
// Returns (applied bool, err error): applied=true when a new row was inserted,
// applied=false on an idempotent duplicate no-op. The bool lets the caller (and the
// API layer above it) distinguish 201-created from 200-noop without a second query.
//
// Idempotent: a duplicate (bill_id, reference) hits ON CONFLICT DO NOTHING, and
// nothing is summed at write time, so at-least-once retries never double-count.
func (a *Activities) ActivityPersistLineItem(ctx context.Context, row LedgerRow) (bool, error) {
	// Conditional insert: only lands if the bill is OPEN. Atomic with any concurrent
	// close — Postgres serializes the EXISTS test against the seal UPDATE. The
	// workflow's acceptsAccruals() check is the authoritative guard; this WHERE EXISTS
	// is the durable backstop for the same rule (a rule a unique constraint can't express).
	tag, err := a.db.Exec(ctx, `
		INSERT INTO line_items
			(bill_id, reference, amount_minor, currency, fee_type, description)
		SELECT $1, $2, $3, $4, $5, $6
		 WHERE EXISTS (SELECT 1 FROM bills WHERE bill_id = $1 AND status = 'OPEN')
		ON CONFLICT (bill_id, reference) DO NOTHING`,
		row.BillID, row.Reference, row.AmountMinor,
		row.Currency, row.FeeType, row.Description,
	)
	if err != nil {
		return false, fmt.Errorf("insert line item: %w", err)
	}

	if tag.RowsAffected() == 1 {
		return true, nil // fresh item, bill was OPEN — the common path
	}

	// Zero rows is AMBIGUOUS: either the reference already existed (ON CONFLICT — an
	// idempotent no-op we must treat as success) or the bill was not OPEN / absent
	// (the WHERE EXISTS failed — a real reject). Disambiguate with one existence read.
	// This runs only on the 0-row path, never on the hot fresh-insert path.
	var exists bool
	if err := a.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM line_items WHERE bill_id = $1 AND reference = $2)`,
		row.BillID, row.Reference,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("disambiguate zero-row insert: %w", err)
	}
	if exists {
		return false, nil // duplicate delivery of an already-persisted item — idempotent success
	}

	// Row absent AND not inserted ⇒ the bill was not OPEN (or doesn't exist). A line
	// item on a sealed/missing bill is an integrity violation, not a transient error.
	return false, temporalNonRetryable(
		fmt.Errorf("bill %s not OPEN — cannot apply line item %s",
			row.BillID, row.Reference))
}
```

**Concurrency note (why no row lock, and where safety actually comes from).** There is no application-level read-modify-write here at all, so there is nothing to lock. The previous design bumped `bills.total_minor` and relied on the claim that "Temporal serializes signals per workflow" to avoid a read-modify-write race. That claim is imprecise — a signal *or* Update handler that awaits an Activity yields at the await point and can interleave with another handler; per-workflow ordering is about delivery, not about atomic execution of handler bodies. The current design sidesteps the whole question: safety comes from the **database**, not from Temporal's scheduling.

- **Idempotency / no double-insert:** the `(bill_id, reference)` unique constraint. Two concurrent inserts of the same reference — however the handlers interleave — resolve at the constraint; one wins, one is a no-op.
- **No lost/duplicated total:** there is no stored total to lose or duplicate. It's `SUM(amount_minor)` over immutable rows at read time.
- **Closed-bill rejection under a concurrent close:** the `WHERE EXISTS (... status='OPEN')` and the seal `UPDATE` are serialized by Postgres.

Because none of these depend on line-item handlers running non-interleaved, the design is robust whether persistence is driven by a Signal, an Update handler that yields, or (hypothetically) a caller outside the workflow entirely. That's a strictly stronger guarantee than the old per-signal-serialization argument, which quietly assumed handlers never interleave.

**On the `bool` return.** The activity returns `applied bool` (`true` = inserted, `false` = idempotent duplicate). The add-line-item Update handler propagates it back to the caller as `LineItemResult.Applied`, so the API answers `201 Created` vs `200 OK` from the Update result alone — no follow-up query to tell "created" from "already there."

### 2.2 `ActivityPersistInvoice`

Seal the bill **in place**: flip `OPEN → CLOSED` and stamp `closed_at` in one atomic `UPDATE`. There is no total to freeze — the total is `SUM` over the line items, which become immutable-in-effect the moment the bill is CLOSED (no new items can be inserted against a non-OPEN bill, per §2.1's `WHERE EXISTS`). So the seal is purely a lifecycle flip. The `bills` row is the invoice; one statement, inherently atomic. Idempotent for FR10: re-closing matches 0 rows and returns the already-sealed row rather than erroring.

The Activity returns a `BillView` (identity + status) for logging/testing convenience. It deliberately does not compute the total here — the close *endpoint* reads total and items back from the ledger for its response (FR3); the seal Activity's job is only to make the bill CLOSED.

```go
// ActivityPersistInvoice seals the bill in place: flips OPEN → CLOSED and stamps
// closed_at in a single atomic UPDATE. There is no total to freeze — it's SUM over
// the line items, which no new insert can extend once the bill is non-OPEN. The
// bills row IS the invoice; there is no separate invoice artifact. Takes just the
// bill_id. Idempotent (FR10): a second close touches 0 rows and returns the
// already-sealed row.
func (a *Activities) ActivityPersistInvoice(ctx context.Context, billID string) (BillView, error) {
	// Seal: flip to CLOSED only if still OPEN. RETURNING gives us the sealed row.
	// On a re-close the UPDATE matches 0 rows, so we fall through to a plain read.
	var out BillView
	err := a.db.QueryRow(ctx, `
		UPDATE bills
		   SET status = 'CLOSED', closed_at = now()
		 WHERE bill_id = $1 AND status = 'OPEN'
		RETURNING client_id, currency, period, status`,
		billID,
	).Scan(&out.ClientID, &out.Currency, &out.Period, &out.Status)

	if errors.Is(err, pgx.ErrNoRows) {
		// Already CLOSED (or never existed): read the authoritative row back.
		// This is the idempotent-close path (FR10).
		err = a.db.QueryRow(ctx, `
			SELECT client_id, currency, period, status
			  FROM bills WHERE bill_id = $1`,
			billID,
		).Scan(&out.ClientID, &out.Currency, &out.Period, &out.Status)
		if errors.Is(err, pgx.ErrNoRows) {
			return BillView{}, temporalNonRetryable(
				fmt.Errorf("bill %s does not exist", billID))
		}
	}
	if err != nil {
		return BillView{}, fmt.Errorf("seal bill: %w", err)
	}
	return out, nil
}
```

`closeBill` in `workflow.go` discards the return via `.Get(ao, nil)` — the sealed state is observable through the ledger, not the workflow result. The Activity still returns `BillView` so the seal has an observable result for logging/testing; drop it to a bare `error` if you'd rather not carry the return.

> **Note — no seal-total to reconcile.** An earlier design froze `bills.total_minor` at seal time and carried an optional hardening step to recompute it from `SUM(line_items)` as a drift guard. Compute-on-read makes that whole concern vanish: there is no stored total to freeze, so there is nothing that *can* drift from the line items — the total simply *is* the `SUM`, always, by construction. The `WHERE EXISTS (... status='OPEN')` guard on inserts (§2.1) is what makes that `SUM` stable after close: once CLOSED, no new item can be added, so the sum over the (now historical) rows is fixed without needing to be snapshotted.

---

## 3. Helpers

Pure, deterministic functions — safe to call directly in workflow code (no Activity needed). `resolvePeriodEnd` and `billID` are replay-safe because they're deterministic and timezone-fixed; `ledgerRow` is a struct mapper.

```go
package fees

import (
	"fmt"
	"time"
)

// billID builds the bill key / Temporal workflow ID (D1).
func billID(clientID, currency, period string) string {
	return fmt.Sprintf("bill-%s-%s-%s", clientID, currency, period)
}

// resolvePeriodEnd maps a calendar-month identifier "2006-01" to the instant the
// period closes: 00:00:00 UTC on the first day of the FOLLOWING month (D3).
// Deterministic and timezone-fixed (UTC), so it's replay-safe in workflow code.
//
//	"2026-07" → 2026-08-01T00:00:00Z
//	"2026-12" → 2027-01-01T00:00:00Z  (year rollover handled by AddDate)
func resolvePeriodEnd(period string) time.Time {
	start, err := time.ParseInLocation("2006-01", period, time.UTC)
	if err != nil {
		// A malformed period is an upstream programming error (the API validates the
		// grammar before StartWorkflow), not a runtime condition to recover from.
		// PRECONDITION: the API layer guarantees a well-formed, zero-padded period.
		panic(fmt.Sprintf("invalid period identifier %q: %v", period, err))
	}
	// First day of the month + 1 month = first instant of the next month.
	// AddDate normalizes Dec→Jan and variable month lengths / leap years (D3).
	return start.AddDate(0, 1, 0)
}

// ledgerRow projects live workflow state + an incoming line item into the flat row
// the persistence Activity writes. Pure mapping, no I/O.
func ledgerRow(s *BillState, li LineItem) LedgerRow {
	return LedgerRow{
		BillID:      billID(s.clientID, s.currency, s.period),
		Reference:   li.Reference,
		AmountMinor: li.AmountMinor,
		Currency:    li.Currency,
		FeeType:     li.FeeType,
		Description: li.Description,
	}
}
```

**On the `panic` in `resolvePeriodEnd`.** It's load-bearing but safe *only because* the API validates the period grammar before `StartWorkflow`, so by the time the workflow runs the period is well-formed by construction. A panic in workflow code becomes a workflow-task failure that Temporal retries indefinitely — correct for a genuine bug (it pauses and alerts without corrupting) but an infinite-retry trap if malformed periods could actually reach here. Keep the API-side validation as the guard. The alternative — a `(time.Time, error)` signature — means handling an error that "can't happen," which is its own smell.

---

## 4. Design decisions carried into this layer

### 4.1 Why no `invoices` table

An earlier draft had a third table. It was dropped because:

- **The append-only `line_items` table is already the immutable audit record.** The invoice total is derivable — `SELECT SUM(amount_minor) FROM line_items WHERE bill_id = $1`. No second table is needed to protect a number recomputable from rows that are already append-only. (This is the same reasoning that led to dropping the stored `total_minor`/`item_count` columns entirely — if the `SUM` is trustworthy enough to be the invoice total, it's trustworthy enough to be *the* total, full stop.)
- **No API returns an invoice resource.** FR3 (close) returns the total and items in the *response*, computed at close time, but there's no persistent invoice resource to `GET` later. FR6/FR7 are Get Bill and List Bills. A `bills` row with `status='CLOSED'` and `closed_at`, joined to `SUM`/`COUNT` over `line_items`, answers every read path actually exposed. An `invoices` table would be a resource with no endpoint.

So the seal collapses from "write invoice row + freeze total + flip bill status" into just "flip bill status." **A closed `bills` row is the invoice**, and its total is the sum of its (now frozen) line items.

*When it would come back:* the moment there's an endpoint that issues/retrieves an invoice as a resource, or a regulatory requirement to store the issued document verbatim (e.g. close must freeze a fee schedule or tax breakdown not reconstructable from line items). Until then, `SUM` over append-only items is the tamper-evident total.

### 4.2 Correction flow stays append-only

If an "amend a closed bill" flow is added later, the correction lands as a **new credit line item on the next period's open bill**, not a mutation of the closed one — a closed bill rejects new inserts (§2.1's OPEN guard) and its `line_items` are historical, so its `SUM` is fixed. This keeps the append-only story coherent.

### 4.3 Retry story

| Failure | Behavior |
|---|---|
| Transient DB error in `PersistLineItem` | Retried per the workflow's `RetryPolicy` (no `MaximumAttempts`) — "a ledger write must eventually land or the bill stays visibly stuck." Safe under retry because the insert is idempotent on `(bill_id, reference)`. |
| Line item on a sealed/missing bill | **Non-retryable** (`temporalNonRetryable`) — a logic violation retrying can't cure. Distinguished from an idempotent duplicate by the zero-row disambiguation read in §2.1. |
| `PersistInvoice` fails | Workflow surfaces the error and stays alive; Temporal won't mark it complete. The bill is visibly un-sealed rather than silently half-closed. |

---

## 5. Build checklist

- [ ] Apply schema migration (two tables + four indexes). Note `bills` has **no** `total_minor`/`item_count` columns — total and count are computed on read.
- [ ] Implement GET/LIST reads to compute total and item count via `SUM(amount_minor)` / `COUNT(*)` over `line_items` (LEFT JOIN so a zero-item bill reports 0, not NULL — `COALESCE(SUM(...),0)`).
- [ ] Confirm `ActivityPersistLineItem` is the single conditional insert with the zero-row disambiguation read — no bill-row UPDATE, no surrounding transaction.
- [ ] Confirm `ActivityPersistInvoice`'s signature matches the workflow call — takes a single `string` (bill_id), returns `(BillView, error)` (identity + status only).
- [ ] Register both Activities on the worker via `NewActivities(db)`.
- [ ] Ensure the API validates the period grammar (zero-padded `YYYY-MM`) before `StartWorkflow` — precondition for `resolvePeriodEnd`'s panic.
- [ ] Close-endpoint flow: `SignalCloseBill` → `.Get(ctx, nil)` to confirm the seal → read the bill (+ computed total and items) from Postgres → return it. The close response comes from the ledger, not Temporal's result channel.
- [ ] If read latency ever forces denormalizing total back onto `bills`, do the increment in the *same* statement as the insert and gate it on the insert actually adding a row — see the tradeoff note in §2.1's history. Default stays compute-on-read.
