package fees

import (
	"context"
	"testing"
	"time"

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
	event := testLineItemEvent("bill-event-log-USD-2099-01", "ref-status", charge.LineItemStatusFailed)

	if err := handleLineItemEvent(context.Background(), event); err != nil {
		t.Fatalf("handleLineItemEvent returned error: %v", err)
	}
}

func TestHandleLineItemEventPendingPersistsFirstPayloadIdempotently(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-pending-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "event-pending", "USD", "2099-01", "OPEN", nil)

	event := testLineItemEvent(billID, "ref-pending", charge.LineItemStatusPending)
	event.MinorAmount = "-500"
	if err := handleLineItemEvent(ctx, event); err != nil {
		t.Fatalf("handle PENDING event: %v", err)
	}
	if err := handleLineItemEvent(ctx, event); err != nil {
		t.Fatalf("handle duplicate PENDING event: %v", err)
	}

	conflict := *event
	conflict.MinorAmount = "999"
	conflict.Currency = "GEL"
	conflict.Description = "conflicting duplicate"
	if err := handleLineItemEvent(ctx, &conflict); err != nil {
		t.Fatalf("handle conflicting duplicate PENDING event: %v", err)
	}

	var amountMinor int64
	var currency, description, status string
	if err := db.QueryRow(ctx, `
		SELECT amount_minor, currency, description, status
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`,
		billID,
		event.Reference,
	).Scan(&amountMinor, &currency, &description, &status); err != nil {
		t.Fatalf("query pending line item: %v", err)
	}
	if amountMinor != -500 || currency != "USD" || description != event.Description || status != "PENDING" {
		t.Fatalf("stored line item = amount:%d currency:%s description:%q status:%s, want first PENDING payload", amountMinor, currency, description, status)
	}
	assertActivityLineItemCount(t, ctx, billID, event.Reference, 1)
}

func TestHandleLineItemEventPendingPersistsAfterBillClosed(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "event-closed", "USD", "2099-01", "CLOSED", &closedAt)

	event := testLineItemEvent(billID, "ref-late-pending", charge.LineItemStatusPending)
	if err := handleLineItemEvent(ctx, event); err != nil {
		t.Fatalf("handle PENDING event after close: %v", err)
	}
	assertActivityLineItemStatus(t, ctx, billID, event.Reference, "PENDING")
}

func TestHandleLineItemEventPendingRejectsMissingBillAndInvalidAmount(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)

	missing := testLineItemEvent(billID, "ref-missing", charge.LineItemStatusPending)
	if err := handleLineItemEvent(ctx, missing); err == nil {
		t.Fatal("handle PENDING event for missing bill returned nil error")
	}

	invalid := testLineItemEvent(billID, "ref-invalid", charge.LineItemStatusPending)
	invalid.MinorAmount = "not-an-integer"
	if err := handleLineItemEvent(ctx, invalid); err == nil {
		t.Fatal("handle PENDING event with invalid amount returned nil error")
	}
}

func TestHandleLineItemEventFinalizedUpdatesStatusIdempotently(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-finalized-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "event-finalized", "USD", "2099-01", "OPEN", nil)

	pending := testLineItemEvent(billID, "ref-finalized", charge.LineItemStatusPending)
	if err := handleLineItemEvent(ctx, pending); err != nil {
		t.Fatalf("handle initial PENDING event: %v", err)
	}
	var appliedAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT applied_at
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`, billID, pending.Reference).Scan(&appliedAt); err != nil {
		t.Fatalf("query pending applied_at: %v", err)
	}

	finalized := *pending
	finalized.Status = charge.LineItemStatusFinalized
	if err := handleLineItemEvent(ctx, &finalized); err != nil {
		t.Fatalf("handle FINALIZED event: %v", err)
	}
	redeliveredPending := *pending
	redeliveredPending.MinorAmount = "999"
	redeliveredPending.Description = "must not replace finalized payload"
	if err := handleLineItemEvent(ctx, &redeliveredPending); err != nil {
		t.Fatalf("handle PENDING event after FINALIZED: %v", err)
	}
	if err := handleLineItemEvent(ctx, &finalized); err != nil {
		t.Fatalf("handle duplicate FINALIZED event: %v", err)
	}

	var gotAmountMinor int64
	var gotStatus string
	var gotAppliedAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT amount_minor, status, applied_at
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`, billID, pending.Reference).Scan(&gotAmountMinor, &gotStatus, &gotAppliedAt); err != nil {
		t.Fatalf("query finalized line item: %v", err)
	}
	if gotAmountMinor != 1500 {
		t.Fatalf("amount_minor = %d after PENDING redelivery, want original 1500", gotAmountMinor)
	}
	if gotStatus != "FINALIZED" {
		t.Fatalf("status = %q, want FINALIZED", gotStatus)
	}
	if !gotAppliedAt.Equal(appliedAt) {
		t.Fatalf("applied_at changed from %s to %s", appliedAt, gotAppliedAt)
	}
}

func TestHandleLineItemEventFinalizedRejectsMissingPendingRow(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-finalized-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "event-finalized-missing", "USD", "2099-01", "OPEN", nil)

	event := testLineItemEvent(billID, "ref-finalized-missing", charge.LineItemStatusFinalized)
	if err := handleLineItemEvent(ctx, event); err == nil {
		t.Fatal("handle FINALIZED event without PENDING row returned nil error")
	}
}

func TestHandleLineItemEventIgnoredStatusesAcknowledgeWithoutMutation(t *testing.T) {
	ctx := context.Background()
	billID := "bill-event-ignored-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "event-ignored", "USD", "2099-01", "OPEN", nil)

	for _, status := range []string{charge.LineItemStatusFailed, "UNEXPECTED"} {
		event := testLineItemEvent(billID, "ref-"+status, status)
		if err := handleLineItemEvent(ctx, event); err != nil {
			t.Fatalf("handle %s event: %v", status, err)
		}
		assertActivityLineItemCount(t, ctx, billID, event.Reference, 0)
	}
}

func testLineItemEvent(billID, reference, status string) *charge.LineItemEvent {
	return &charge.LineItemEvent{
		BillID:      billID,
		Reference:   reference,
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
		Status:      status,
		OrderingID:  billID + "-" + reference,
	}
}
