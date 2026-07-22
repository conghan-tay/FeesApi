package fees

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ─────────────────────────────────────────────────────────────
// Message names
//
// Line items arrive as a Workflow UPDATE (synchronous, can be rejected, returns a
// result). Close is a Signal (fire-and-forget lifecycle trigger; the sealed result
// is read back from the ledger). Get is a Query.
// ─────────────────────────────────────────────────────────────

const (
	UpdateAddLineItem = "addLineItem"
	SignalCloseBill   = "closeBill"
	QueryGetBill      = "getBill"
)

// ─────────────────────────────────────────────────────────────
// State machine
// ─────────────────────────────────────────────────────────────

type BillStatus int

const (
	OPEN     BillStatus = iota // accepting new line items
	DRAINING                   // close triggered; waiting for in-flight handlers to finish
	CLOSING                    // handlers drained; sealing the invoice, accruals rejected
	CLOSED                     // invoice sealed, immutable
)

func (s BillStatus) String() string {
	switch s {
	case OPEN:
		return "OPEN"
	case DRAINING:
		return "DRAINING"
	case CLOSING:
		return "CLOSING"
	case CLOSED:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// acceptsAccruals is the single source of truth for whether a line item may be
// applied. OPEN and DRAINING accept; CLOSING and CLOSED reject.
func (s BillStatus) acceptsAccruals() bool {
	return s == OPEN || s == DRAINING
}

// ─────────────────────────────────────────────────────────────
// Messages
// ─────────────────────────────────────────────────────────────

type LineItem struct {
	Reference   string // caller-supplied idempotency key
	AmountMinor int64  // integer minor units (D5)
	Currency    string // must match the bill's currency (D4)
	FeeType     string
	Description string
}

// LineItemResult is returned by the add-line-item Update. Applied distinguishes a
// fresh application from an idempotent replay, so the API layer answers 201 vs 200
// without a second query.
type LineItemResult struct {
	Reference string
	Applied   bool
}

type CloseSignal struct {
	Reason string // e.g. "explicit-early-close"; informational only
}

type BillInput struct {
	ClientID string
	Currency string // bill is single-currency (D4)
	Period   string // calendar-month identifier, e.g. "2026-07" (D2)

	// Populated only on a Continue-As-New continuation; zero-valued on first run.
	// Only lifecycle status carries forward — the total, item count, and seen
	// references are ledger-owned, not workflow memory. Status is the one piece of
	// genuinely workflow-owned state: the workflow is the single writer of lifecycle
	// transitions, so it must carry its own OPEN/DRAINING position across the boundary.
	CarriedStatus BillStatus
}

// BillView is the workflow's live self-report via the QueryGetBill handler: identity
// + lifecycle status only. It deliberately carries no total or item count — those are
// computed on read from the ledger (SUM/COUNT over line_items), so the query can't
// report a number that has drifted from the durable rows. Callers needing the total
// read the ledger (GET /bills), which works during the open month AND after the
// workflow ages out of Temporal retention.
type BillView struct {
	ClientID string
	Currency string
	Period   string
	Status   string
}

// ─────────────────────────────────────────────────────────────
// In-memory workflow state
//
// The workflow's entire footprint: bill identity plus one mutable field, lifecycle
// status. Everything the bill "contains" — line items, running total, item count,
// seen references — lives in the ledger. The workflow mirrors none of it. This is
// the payoff of the Temporal-owns-lifecycle / Postgres-owns-facts split: there is no
// shared mutable aggregate state for concurrent add-line-item handlers to corrupt.
// ─────────────────────────────────────────────────────────────

type BillState struct {
	clientID string
	currency string
	period   string
	status   BillStatus
}

func newBillState(in BillInput) *BillState {
	status := OPEN
	if in.CarriedStatus != 0 { // continued run carries status forward
		status = in.CarriedStatus
	}
	return &BillState{
		clientID: in.ClientID,
		currency: in.Currency,
		period:   in.Period,
		status:   status,
	}
}

func (s *BillState) toView() BillView {
	return BillView{
		ClientID: s.clientID,
		Currency: s.currency,
		Period:   s.period,
		Status:   s.status.String(),
	}
}

func (s *BillState) carryForward() BillInput {
	return BillInput{
		ClientID:      s.clientID,
		Currency:      s.currency,
		Period:        s.period,
		CarriedStatus: s.status,
	}
}

// ─────────────────────────────────────────────────────────────
// Workflow
// ─────────────────────────────────────────────────────────────

func BillWorkflow(ctx workflow.Context, input BillInput) error {
	log := workflow.GetLogger(ctx)
	state := newBillState(input)

	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)

	// Read-only query handler: no side effects; safe during replay.
	if err := workflow.SetQueryHandler(ctx, QueryGetBill, func() (BillView, error) {
		return state.toView(), nil
	}); err != nil {
		return err
	}

	// Add-line-item is an Update: validator rejects bad input synchronously without
	// writing to history; handler runs the idempotent persist Activity and returns a
	// result. Registered once; lives for the run. No Selector arm — Updates are
	// delivered straight to the handler.
	if err := registerAddLineItem(ctx, state); err != nil {
		return err
	}

	// D3: close time derived from the PERIOD, not from open time.
	closeTime := resolvePeriodEnd(input.Period)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	autoCloseTimer := workflow.NewTimer(timerCtx, closeTime.Sub(workflow.Now(ctx)))

	// ── Main loop: wait for a close trigger ───────────────────
	// Line items no longer flow through here (they're Updates), so the loop only
	// hosts the close trigger, the auto-close timer, and the Continue-As-New check.
	for state.status == OPEN {

		// Scale escape hatch: if event history is growing large, seal this run and
		// continue as new. Wait for in-flight Update handlers to finish first, or an
		// awaiting caller gets a NotFound on their line-item Update across the boundary.
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
			cancelTimer()
			return workflow.NewContinueAsNewError(ctx, BillWorkflow, state.carryForward())
		}

		selector := workflow.NewSelector(ctx)

		selector.AddReceive(closeCh, func(c workflow.ReceiveChannel, _ bool) {
			var req CloseSignal
			c.Receive(ctx, &req)
			log.Info("explicit close requested", "clientID", state.clientID, "period", state.period, "reason", req.Reason)
			state.status = DRAINING
			cancelTimer()
		})

		selector.AddFuture(autoCloseTimer, func(_ workflow.Future) {
			// Fires at period end (D9). Not invoked if cancelled (explicit close won).
			log.Info("auto-close timer fired", "clientID", state.clientID, "period", state.period)
			state.status = DRAINING
		})

		selector.Select(ctx)
	}

	// ── Close path (shared by explicit close AND timer) ──────
	return closeBill(ctx, state)
}

// ─────────────────────────────────────────────────────────────
// Add-line-item Update: validator + handler.
//
// The workflow accumulates nothing. The handler enforces no aggregate state — it
// runs one idempotent Activity whose only effect is at the DB, where the
// (bill_id, reference) unique constraint and the WHERE EXISTS(status='OPEN') guard
// serialize concurrent calls. So two Updates may interleave freely: no workflow.Mutex,
// no enqueue-to-main-loop indirection.
// ─────────────────────────────────────────────────────────────

func registerAddLineItem(ctx workflow.Context, state *BillState) error {
	return workflow.SetUpdateHandlerWithOptions(
		ctx,
		UpdateAddLineItem,
		func(hctx workflow.Context, li LineItem) (LineItemResult, error) {
			// No workflow-state mutation — nothing to guard. The Activity is the only
			// effect, idempotent and status-guarded at the DB.
			ao := workflow.WithActivityOptions(hctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    time.Minute,
				},
			})

			var applied bool
			if err := workflow.ExecuteActivity(ao, ActivityPersistLineItem, ledgerRow(state, li)).
				Get(hctx, &applied); err != nil {
				// A non-retryable BillNotOpen here is the close-race the validator
				// couldn't catch (bill sealed after validation). Surface it → API maps 409.
				workflow.GetLogger(hctx).Error("PersistLineItem failed", "ref", li.Reference, "err", err)
				return LineItemResult{Reference: li.Reference}, err
			}
			return LineItemResult{Reference: li.Reference, Applied: applied}, nil
		},
		workflow.UpdateHandlerOptions{
			// Validator: synchronous, side-effect-free, no awaits. Rejects the checks
			// makeable from in-memory state; a reject writes nothing to history.
			//
			// The status check is a FAST-PATH reject, not the authority. It can't be
			// race-proof against a close that commits while this Update is in flight
			// (validator sees OPEN → close commits → handler's Activity runs). The
			// authoritative open/closed decision is the Activity's WHERE EXISTS, atomic
			// with the seal. Validator catches the easy already-closed case cleanly;
			// the DB catches the racy one. Don't try to make the in-memory check
			// race-proof — it can't be, and doesn't need to be.
			Validator: func(_ workflow.Context, li LineItem) error {
				if li.Currency != state.currency {
					return temporal.NewApplicationError(
						"currency mismatch: item "+li.Currency+" != bill "+state.currency,
						"CurrencyMismatch",
					)
				}
				if !state.status.acceptsAccruals() {
					return temporal.NewApplicationError(
						"bill not accepting accruals (status "+state.status.String()+")",
						"BillNotOpen",
					)
				}
				return nil
			},
		},
	)
}

// ─────────────────────────────────────────────────────────────
// Close: drain in-flight handlers (DRAINING), seal the invoice (CLOSING → CLOSED).
// ─────────────────────────────────────────────────────────────

func closeBill(ctx workflow.Context, state *BillState) error {
	log := workflow.GetLogger(ctx)

	// Status is DRAINING: the validator now rejects new items, so no fresh Update
	// enters a long await. Wait for any handler already mid-persist to finish before
	// sealing, so no caller waiting on a line-item Update result is orphaned. "Drain"
	// here means "let in-flight handlers complete," not "empty a channel buffer" —
	// there is no buffer, Updates go straight to the handler.
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}

	// Close the accrual window. From here, new items are rejected.
	state.status = CLOSING

	// Seal: one Activity flips the ledger bill row OPEN → CLOSED and stamps closed_at
	// in a single atomic UPDATE. There is no total to freeze — it's SUM over the line
	// items, which no new insert can extend once the bill is non-OPEN. The bills row
	// IS the invoice. The API reads the sealed bill back from the ledger, so the
	// workflow returns nothing but success/failure.
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    2 * time.Minute,
		},
	})

	id := billID(state.clientID, state.currency, state.period)
	if err := workflow.ExecuteActivity(ao, ActivityPersistInvoice, id).Get(ao, nil); err != nil {
		// Keep the workflow alive and surface the error; Temporal won't mark it
		// complete. The bill stays visibly un-sealed rather than silently half-closed.
		log.Error("PersistInvoice failed", "clientID", state.clientID, "period", state.period, "err", err)
		return err
	}

	state.status = CLOSED
	log.Info("bill closed", "clientID", state.clientID, "period", state.period)
	return nil
}
