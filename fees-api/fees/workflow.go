package fees

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	BillWorkflowName = "BillWorkflow"

	UpdateAwaitOpen   = "awaitOpen"
	UpdateAddLineItem = "addLineItem"
	SignalCloseBill   = "closeBill"
	QueryGetBill      = "getBill"
)

func BillWorkflow(ctx workflow.Context, input BillInput) error {
	log := workflow.GetLogger(ctx)
	state := newBillState(input)
	billPersisted := false

	if err := workflow.SetUpdateHandler(ctx, UpdateAwaitOpen, func(hctx workflow.Context) (BillView, error) {
		if err := workflow.Await(hctx, func() bool { return billPersisted }); err != nil {
			return BillView{}, err
		}
		return state.toView(), nil
	}); err != nil {
		return err
	}

	canCheckCh := workflow.NewBufferedChannel(ctx, 1)
	if err := registerAddLineItem(ctx, state, canCheckCh, func() bool { return billPersisted }); err != nil {
		return err
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
		},
	})
	if err := workflow.ExecuteActivity(activityCtx, ActivityPersistBill, input).Get(activityCtx, nil); err != nil {
		log.Error("PersistBill failed", "clientID", state.clientID, "period", state.period, "err", err)
		return err
	}
	billPersisted = true

	if err := workflow.SetQueryHandler(ctx, QueryGetBill, func() (BillView, error) {
		return state.toView(), nil
	}); err != nil {
		return err
	}

	closeCh := workflow.GetSignalChannel(ctx, SignalCloseBill)
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	autoCloseTimer := workflow.NewTimer(timerCtx, resolvePeriodEnd(input.Period).Sub(workflow.Now(ctx)))

	for state.status == OPEN {
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return err
			}
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
		selector.AddReceive(canCheckCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
		})
		selector.Select(ctx)
	}

	return closeBill(ctx, state)
}

func registerAddLineItem(ctx workflow.Context, state *BillState, canCheckCh workflow.Channel, billPersisted func() bool) error {
	return workflow.SetUpdateHandlerWithOptions(
		ctx,
		UpdateAddLineItem,
		func(hctx workflow.Context, li LineItem) (LineItemResult, error) {
			if err := workflow.Await(hctx, billPersisted); err != nil {
				return LineItemResult{Reference: li.Reference}, err
			}

			activityCtx := workflow.WithActivityOptions(hctx, workflow.ActivityOptions{
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
				workflow.GetLogger(hctx).Error("PersistLineItem failed", "reference", li.Reference, "err", err)
				return LineItemResult{Reference: li.Reference}, err
			}

			canCheckCh.SendAsync(struct{}{})
			return LineItemResult{Reference: li.Reference, Applied: applied}, nil
		},
		workflow.UpdateHandlerOptions{
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

func closeBill(ctx workflow.Context, state *BillState) error {
	log := workflow.GetLogger(ctx)

	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
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
