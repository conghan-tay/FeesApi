package fees

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
)

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

	applied, err = activities.ActivityPersistLineItem(ctx, row)
	if err != nil {
		t.Fatalf("ActivityPersistLineItem duplicate returned error: %v", err)
	}
	if applied {
		t.Fatal("ActivityPersistLineItem duplicate applied=true, want false")
	}
	assertActivityLineItemCount(t, ctx, billID, row.Reference, 1)
}

func TestActivityPersistBillFreshInsertAndDuplicate(t *testing.T) {
	ctx := context.Background()
	activities := NewActivities(db)
	input := BillInput{
		ClientID: "activity-open",
		Currency: "USD",
		Period:   "2099-01",
	}
	billID := billID(input.ClientID, input.Currency, input.Period)
	cleanupActivityBill(t, ctx, billID)

	if err := activities.ActivityPersistBill(ctx, input); err != nil {
		t.Fatalf("ActivityPersistBill fresh insert returned error: %v", err)
	}
	assertActivityBillRow(t, ctx, billID, input.ClientID, input.Currency, input.Period, "OPEN", 1)

	if err := activities.ActivityPersistBill(ctx, input); err != nil {
		t.Fatalf("ActivityPersistBill duplicate returned error: %v", err)
	}
	assertActivityBillRow(t, ctx, billID, input.ClientID, input.Currency, input.Period, "OPEN", 1)
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
