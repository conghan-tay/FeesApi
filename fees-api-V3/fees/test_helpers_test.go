package fees

import (
	"context"
	"testing"
	"time"
)

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
