package fees

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"

	"encore.app/charge"
	"go.temporal.io/sdk/temporal"
)

type recordingLineItemStatusPublisher struct {
	err      error
	requests []charge.PublishLineItemStatusRequest
}

func (p *recordingLineItemStatusPublisher) PublishLineItemStatus(_ context.Context, req *charge.PublishLineItemStatusRequest) error {
	if req != nil {
		p.requests = append(p.requests, *req)
	}
	return p.err
}

func TestActivityPublishPendingFormatsInt64AndPreservesPayload(t *testing.T) {
	tests := []struct {
		name   string
		amount int64
	}{
		{name: "positive", amount: 1500},
		{name: "zero", amount: 0},
		{name: "negative", amount: -500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &recordingLineItemStatusPublisher{}
			activities := &Activities{lineItemStatusClient: publisher}
			row := LedgerRow{
				BillID:      "bill-activity-pending-USD-2099-01",
				Reference:   "ref-pending-" + tt.name,
				AmountMinor: tt.amount,
				Currency:    "USD",
				FeeType:     "wire_transfer",
				Description: "Outbound USD wire",
			}

			if err := activities.ActivityPublishPending(context.Background(), row); err != nil {
				t.Fatalf("ActivityPublishPending returned error: %v", err)
			}
			if len(publisher.requests) != 1 {
				t.Fatalf("publish requests = %d, want 1", len(publisher.requests))
			}
			got := publisher.requests[0]
			if got.BillID != row.BillID || got.Reference != row.Reference || got.Currency != row.Currency || got.FeeType != row.FeeType || got.Description != row.Description {
				t.Fatalf("published request = %#v, want row fields %#v", got, row)
			}
			if got.MinorAmount != strconv.FormatInt(tt.amount, 10) {
				t.Fatalf("published minorAmount = %#v, want %d", got.MinorAmount, tt.amount)
			}
			if got.Status != charge.LineItemStatusPending {
				t.Fatalf("published status = %q, want %q", got.Status, charge.LineItemStatusPending)
			}
		})
	}
}

func TestActivityPublishPendingPropagatesPublisherFailure(t *testing.T) {
	publishErr := errors.New("charge callback unavailable")
	publisher := &recordingLineItemStatusPublisher{err: publishErr}
	activities := &Activities{lineItemStatusClient: publisher}

	err := activities.ActivityPublishPending(context.Background(), LedgerRow{AmountMinor: 25})
	if !errors.Is(err, publishErr) {
		t.Fatalf("ActivityPublishPending error = %v, want wrapped %v", err, publishErr)
	}
}

func TestActivityPublishPendingRejectsMissingPublisher(t *testing.T) {
	err := (&Activities{}).ActivityPublishPending(context.Background(), LedgerRow{})
	if err == nil {
		t.Fatal("ActivityPublishPending returned nil error with no publisher")
	}
}

func TestActivityPersistLineItemFreshInsertAndDuplicate(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-line-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-line-open", "USD", "2099-01", "OPEN", nil)

	row := LedgerRow{
		BillID:      billID,
		Reference:   "ref-fresh",
		AmountMinor: 1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}

	applied, err := activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem fresh insert returned error: %v", err)
	}
	if !applied {
		t.Fatal("ActivityPersistLineItem fresh insert applied=false, want true")
	}
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 1)
	assertActivityLineItemStatus(t, ctx, billID, row.Reference, "FINALIZED")

	applied, err = activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem duplicate returned error: %v", err)
	}
	if applied {
		t.Fatal("ActivityPersistLineItem duplicate applied=true, want false")
	}
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 1)
}

func TestActivityPersistLineItemRejectsClosedBill(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-line-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-line-closed", "USD", "2099-01", "CLOSED", &closedAt)

	row := LedgerRow{
		BillID:      billID,
		Reference:   "ref-closed",
		AmountMinor: 1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}

	applied, err := activities.ActivityPersistLineItem(ctx, row)
	if err == nil {
		t.Fatal("ActivityPersistLineItem on closed bill returned nil error")
	}
	if applied {
		t.Fatal("ActivityPersistLineItem on closed bill applied=true, want false")
	}
	assertBillNotOpenError(t, err)
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 0)
}

func TestActivityPersistLineItemDuplicateOnClosedBillIsIdempotent(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-line-closedup-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-line-closedup", "USD", "2099-01", "OPEN", nil)

	row := LedgerRow{
		BillID:      billID,
		Reference:   "ref-race",
		AmountMinor: 1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}

	applied, err := activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem initial insert returned error: %v", err)
	}
	if !applied {
		t.Fatal("ActivityPersistLineItem initial insert applied=false, want true")
	}

	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	if _, err := db.Exec(ctx, `
		UPDATE bills
		   SET status = 'CLOSED',
		       closed_at = $2
		 WHERE bill_id = $1`,
		billID,
		closedAt,
	); err != nil {
		t.Fatalf("close bill after initial insert: %v", err)
	}

	applied, err = activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem duplicate after close returned error: %v", err)
	}
	if applied {
		t.Fatal("ActivityPersistLineItem duplicate after close applied=true, want false")
	}
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 1)
}

func TestActivityPersistLineItemRejectsMissingBill(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-line-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)

	row := LedgerRow{
		BillID:      billID,
		Reference:   "ref-missing",
		AmountMinor: 1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}

	applied, err := activities.ActivityPersistLineItem(ctx, row)
	if err == nil {
		t.Fatal("ActivityPersistLineItem on missing bill returned nil error")
	}
	if applied {
		t.Fatal("ActivityPersistLineItem on missing bill applied=true, want false")
	}
	assertBillNotOpenError(t, err)
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 0)
}

func TestActivityPersistLineItemAcceptsNegativeAmount(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-line-credit-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-line-credit", "USD", "2099-01", "OPEN", nil)

	row := LedgerRow{
		BillID:      billID,
		Reference:   "ref-credit",
		AmountMinor: -500,
		Currency:    "USD",
		FeeType:     "credit_adjustment",
		Description: "Credit adjustment",
	}

	applied, err := activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem negative amount returned error: %v", err)
	}
	if !applied {
		t.Fatal("ActivityPersistLineItem negative amount applied=false, want true")
	}

	var amountMinor int64
	if err := db.QueryRow(ctx, `
		SELECT amount_minor
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`,
		billID,
		row.Reference,
	).Scan(&amountMinor); err != nil {
		t.Fatalf("query inserted credit line item: %v", err)
	}
	if amountMinor != -500 {
		t.Fatalf("amount_minor = %d, want -500", amountMinor)
	}
}

func TestActivityPersistInvoiceSealsOpenBill(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-seal-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-seal-open", "USD", "2099-01", "OPEN", nil)

	view, err := activities.ActivityPersistInvoice(ctx, billID)
	if err != nil {
		t.Fatalf("ActivityPersistInvoice returned error: %v", err)
	}
	if view != (BillView{ClientID: "activity-seal-open", Currency: "USD", Period: "2099-01", Status: "CLOSED"}) {
		t.Fatalf("BillView = %#v, want sealed identity/status", view)
	}

	var status string
	var closedAt sql.NullTime
	if err := db.QueryRow(ctx, `
		SELECT status, closed_at
		  FROM bills
		 WHERE bill_id = $1`,
		billID,
	).Scan(&status, &closedAt); err != nil {
		t.Fatalf("query sealed bill: %v", err)
	}
	if status != "CLOSED" {
		t.Fatalf("status = %q, want CLOSED", status)
	}
	if !closedAt.Valid {
		t.Fatal("closed_at is NULL, want timestamp")
	}
}

func TestActivityPersistInvoiceZeroItemBillReadsAsEmptyInvoice(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-seal-zero-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-seal-zero", "USD", "2099-01", "OPEN", nil)

	if _, err := activities.ActivityPersistInvoice(ctx, billID); err != nil {
		t.Fatalf("ActivityPersistInvoice zero-item bill returned error: %v", err)
	}

	invoice, err := readClosedInvoiceResource(ctx, billID)
	if err != nil {
		t.Fatalf("read sealed zero-item invoice: %v", err)
	}
	if invoice.Status != "CLOSED" {
		t.Fatalf("status = %q, want CLOSED", invoice.Status)
	}
	if invoice.TotalMinorAmount != "0" || invoice.ItemCount != 0 {
		t.Fatalf("total/count = %s/%d, want 0/0", invoice.TotalMinorAmount, invoice.ItemCount)
	}
	if invoice.ClosedAt == nil {
		t.Fatal("closedAt is nil, want seal timestamp")
	}
	if invoice.LineItems == nil {
		t.Fatal("lineItems is nil, want empty slice")
	}
	if len(invoice.LineItems) != 0 {
		t.Fatalf("lineItems length = %d, want 0", len(invoice.LineItems))
	}
}

func TestActivityPersistInvoiceIsIdempotentForClosedBill(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-seal-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "activity-seal-closed", "USD", "2099-01", "CLOSED", &closedAt)

	view, err := activities.ActivityPersistInvoice(ctx, billID)
	if err != nil {
		t.Fatalf("ActivityPersistInvoice re-close returned error: %v", err)
	}
	if view != (BillView{ClientID: "activity-seal-closed", Currency: "USD", Period: "2099-01", Status: "CLOSED"}) {
		t.Fatalf("BillView = %#v, want existing sealed identity/status", view)
	}

	var gotClosedAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT closed_at
		  FROM bills
		 WHERE bill_id = $1`,
		billID,
	).Scan(&gotClosedAt); err != nil {
		t.Fatalf("query re-closed bill: %v", err)
	}
	if !gotClosedAt.Equal(closedAt) {
		t.Fatalf("closed_at = %s, want unchanged %s", gotClosedAt.Format(time.RFC3339Nano), closedAt.Format(time.RFC3339Nano))
	}
}

func TestActivityPersistInvoiceRejectsMissingBill(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	billID := "bill-activity-seal-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)

	view, err := activities.ActivityPersistInvoice(ctx, billID)
	if err == nil {
		t.Fatal("ActivityPersistInvoice on missing bill returned nil error")
	}
	if view != (BillView{}) {
		t.Fatalf("BillView = %#v, want zero value", view)
	}
	assertBillNotOpenError(t, err)
}

func cleanupActivityBill(t *testing.T, ctx context.Context, billID string) {
	t.Helper()

	cleanup := func() {
		_, _ = db.Exec(ctx, `DELETE FROM line_items WHERE bill_id = $1`, billID)
		_, _ = db.Exec(ctx, `DELETE FROM bills WHERE bill_id = $1`, billID)
	}
	cleanup()
	t.Cleanup(cleanup)
}

func seedActivityBill(t *testing.T, ctx context.Context, billID, clientID, currency, period, status string, closedAt *time.Time) {
	t.Helper()

	_, err := db.Exec(ctx, `
		INSERT INTO bills (bill_id, client_id, currency, period, status, closed_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		billID,
		clientID,
		currency,
		period,
		status,
		closedAt,
	)
	if err != nil {
		t.Fatalf("seed bill %s: %v", billID, err)
	}
}

func assertActivityLineItemCount(t *testing.T, ctx context.Context, billID, reference string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*)
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`,
		billID,
		reference,
	).Scan(&got); err != nil {
		t.Fatalf("count line items for %s/%s: %v", billID, reference, err)
	}
	if got != want {
		t.Fatalf("line item count for %s/%s = %d, want %d", billID, reference, got, want)
	}
}

func assertActivityLineItemStatus(t *testing.T, ctx context.Context, billID, reference, want string) {
	t.Helper()

	var got string
	if err := db.QueryRow(ctx, `
		SELECT status
		  FROM line_items
		 WHERE bill_id = $1
		   AND reference = $2`,
		billID,
		reference,
	).Scan(&got); err != nil {
		t.Fatalf("read line item status for %s/%s: %v", billID, reference, err)
	}
	if got != want {
		t.Fatalf("line item status for %s/%s = %q, want %q", billID, reference, got, want)
	}
}

func assertActivityBillRow(t *testing.T, ctx context.Context, billID, clientID, currency, period, status string, wantCount int) {
	t.Helper()

	var gotCount int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*)
		  FROM bills
		 WHERE bill_id = $1
		   AND client_id = $2
		   AND currency = $3
		   AND period = $4
		   AND status = $5`,
		billID,
		clientID,
		currency,
		period,
		status,
	).Scan(&gotCount); err != nil {
		t.Fatalf("count bill row %s: %v", billID, err)
	}
	if gotCount != wantCount {
		t.Fatalf("bill row count for %s = %d, want %d", billID, gotCount, wantCount)
	}
}

func assertBillNotOpenError(t *testing.T, err error) {
	t.Helper()

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T %v, want temporal ApplicationError", err, err)
	}
	if appErr.Type() != "BillNotOpen" {
		t.Fatalf("application error type = %q, want BillNotOpen", appErr.Type())
	}
	if !appErr.NonRetryable() {
		t.Fatal("application error is retryable, want non-retryable")
	}
}
