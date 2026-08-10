package fees

import (
	"context"
	"fmt"
	"strconv"

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

func handleLineItemEvent(ctx context.Context, event *charge.LineItemEvent) error {
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
		"orderingID", event.OrderingID,
	)

	switch event.Status {
	case charge.LineItemStatusPending:
		return persistPendingLineItem(ctx, event)
	case charge.LineItemStatusFinalized:
		return finalizeLineItem(ctx, event)
	default:
		rlog.Info(
			"update line item ledger: status ignored",
			"billID", event.BillID,
			"reference", event.Reference,
			"status", event.Status,
		)
		return nil
	}
}

func persistPendingLineItem(ctx context.Context, event *charge.LineItemEvent) error {
	amountMinor, err := strconv.ParseInt(event.MinorAmount, 10, 64)
	if err != nil {
		return fmt.Errorf("persist pending line item %s/%s: parse minor amount: %w", event.BillID, event.Reference, err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO line_items
			(bill_id, reference, amount_minor, currency, fee_type, description, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
		ON CONFLICT (bill_id, reference) DO NOTHING`,
		event.BillID,
		event.Reference,
		amountMinor,
		event.Currency,
		event.FeeType,
		event.Description,
	)
	if err != nil {
		return fmt.Errorf("persist pending line item %s/%s: %w", event.BillID, event.Reference, err)
	}
	return nil
}

func finalizeLineItem(ctx context.Context, event *charge.LineItemEvent) error {
	tag, err := db.Exec(ctx, `
		UPDATE line_items
		   SET status = 'FINALIZED'
		 WHERE bill_id = $1
		   AND reference = $2`,
		event.BillID,
		event.Reference,
	)
	if err != nil {
		return fmt.Errorf("finalize line item %s/%s: %w", event.BillID, event.Reference, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("finalize line item %s/%s: pending line item does not exist", event.BillID, event.Reference)
	}
	return nil
}
