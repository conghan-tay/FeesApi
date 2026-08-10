package fees

import (
	"context"
	"testing"

	"encore.app/charge"
)

func TestUpdateLineItemLedgerSubscriptionConfiguration(t *testing.T) {
	meta := updateLineItemLedgerSubscription.Meta()
	if meta.Name != "update-line-item-ledger" {
		t.Fatalf("subscription name = %q, want update-line-item-ledger", meta.Name)
	}
	if meta.Topic.Name != "update-line-items" {
		t.Fatalf("subscription topic = %q, want update-line-items", meta.Topic.Name)
	}
}

func TestHandleLineItemEventLogsAndAcknowledges(t *testing.T) {
	event := &charge.LineItemEvent{
		BillID:      "bill-acme-USD-2099-01",
		Reference:   "ref-status",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
		Status:      charge.LineItemStatusPending,
	}

	if err := handleLineItemEvent(context.Background(), event); err != nil {
		t.Fatalf("handleLineItemEvent returned error: %v", err)
	}
}
