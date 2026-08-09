package fees

import (
	"time"

	"encore.app/internal/chargecontract"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	BillWorkflowName = "BillWorkflow"

	SignalCloseBill = "closeBill"
	QueryGetBill    = "getBill"
)

type lineItemSignalHandler struct {
	signals  workflow.ReceiveChannel
	done     workflow.Channel
	inFlight int
}

func BillWorkflow(ctx workflow.Context, input BillInput) error {
	log := workflow.GetLogger(ctx)
	state := newBillState(input)

	lineItems := &lineItemSignalHandler{
		signals: workflow.GetSignalChannel(ctx, chargecontract.SignalAddLineItem),
		done:    workflow.NewBufferedChannel(ctx, 1),
	}

	if err := workflow.SetQueryHandler(ctx, QueryGetBill, func() (BillView, error) {
		return state.toView(), nil
	}); err != nil {
		return err
	}

	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	autoCloseTimer := workflow.NewTimer(timerCtx, resolvePeriodEnd(input.Period).Sub(workflow.Now(ctx)))

	for state.status == OPEN {
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() &&
			lineItems.inFlight == 0 &&
			len(workflow.GetUnhandledSignalNames(ctx)) == 0 {
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
		selector.AddFuture(autoCloseTimer, func(workflow.Future) {
			log.Info("auto-close timer fired", "clientID", state.clientID, "period", state.period)
			state.status = DRAINING
		})
		selector.AddReceive(lineItems.signals, func(c workflow.ReceiveChannel, _ bool) {
			var lineItem chargecontract.LineItem
			c.Receive(ctx, &lineItem)
			lineItems.start(ctx, state, lineItem)
		})
		selector.AddReceive(lineItems.done, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
		})
		selector.Select(ctx)
	}

	return closeBill(ctx, state, lineItems)
}

func (h *lineItemSignalHandler) start(ctx workflow.Context, state *BillState, li chargecontract.LineItem) {
	log := workflow.GetLogger(ctx)
	id := billID(state.clientID, state.currency, state.period)
	if li.Currency != state.currency {
		log.Error(
			"add line item signal rejected",
			"billID", id,
			"reference", li.Reference,
			"reason", "currency-mismatch",
			"itemCurrency", li.Currency,
			"billCurrency", state.currency,
		)
		return
	}
	if !state.status.acceptsAccruals() {
		log.Error(
			"add line item signal rejected",
			"billID", id,
			"reference", li.Reference,
			"reason", "bill-not-open",
			"status", state.status.String(),
		)
		return
	}

	h.inFlight++
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer func() {
			h.inFlight--
			h.done.SendAsync(struct{}{})
		}()

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    time.Minute,
			},
		})

		var applied bool
		if err := workflow.ExecuteActivity(activityCtx, ActivityPersistLineItem, ledgerRow(state, li)).
			Get(activityCtx, &applied); err != nil {
			workflow.GetLogger(ctx).Error(
				"add line item signal failed",
				"billID", id,
				"reference", li.Reference,
				"err", err,
			)
			return
		}

		workflow.GetLogger(ctx).Info(
			"add line item signal processed",
			"billID", id,
			"reference", li.Reference,
			"applied", applied,
		)
	})
}

func closeBill(ctx workflow.Context, state *BillState, lineItems *lineItemSignalHandler) error {
	log := workflow.GetLogger(ctx)

	for {
		var lineItem chargecontract.LineItem
		if lineItems.signals.ReceiveAsync(&lineItem) {
			log.Error(
				"add line item signal rejected",
				"billID", billID(state.clientID, state.currency, state.period),
				"reference", lineItem.Reference,
				"reason", "bill-not-open",
				"status", state.status.String(),
			)
			continue
		}
		if lineItems.inFlight == 0 {
			break
		}

		selector := workflow.NewSelector(ctx)
		selector.AddReceive(lineItems.signals, func(c workflow.ReceiveChannel, _ bool) {
			var rejected chargecontract.LineItem
			c.Receive(ctx, &rejected)
			log.Error(
				"add line item signal rejected",
				"billID", billID(state.clientID, state.currency, state.period),
				"reference", rejected.Reference,
				"reason", "bill-not-open",
				"status", state.status.String(),
			)
		})
		selector.AddReceive(lineItems.done, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
		})
		selector.Select(ctx)
	}

	state.status = CLOSING

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    2 * time.Minute,
		},
	})

	id := billID(state.clientID, state.currency, state.period)
	if err := workflow.ExecuteActivity(activityCtx, ActivityPersistInvoice, id).
		Get(activityCtx, nil); err != nil {
		log.Error("PersistInvoice failed", "clientID", state.clientID, "period", state.period, "err", err)
		return err
	}

	state.status = CLOSED
	log.Info("bill closed", "clientID", state.clientID, "period", state.period)
	return nil
}
