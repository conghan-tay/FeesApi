package fees

import (
	"context"

	"encore.app/charge"
	"encore.dev/pubsub"
	"encore.dev/rlog"
)

var updateLineItemLedgerSubscription = pubsub.NewSubscription(
	charge.UpdateLineItems,
	"update-line-item-ledger",
	pubsub.SubscriptionConfig[*charge.LineItemEvent]{
		Handler: handleLineItemEvent,
	},
)

func handleLineItemEvent(_ context.Context, event *charge.LineItemEvent) error {
	if event == nil {
		rlog.Info("update line item ledger: event received", "event", nil)
		return nil
	}

	rlog.Info(
		"update line item ledger: event received",
		"billID", event.BillID,
		"reference", event.Reference,
		"minorAmount", event.MinorAmount,
		"currency", event.Currency,
		"feeType", event.FeeType,
		"description", event.Description,
		"status", event.Status,
	)
	return nil
}
