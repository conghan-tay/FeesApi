package fees

import (
	"context"
	"errors"
	"testing"

	"encore.dev/storage/sqldb"
)

func TestPersistOpenBillResourceFreshAndExisting(t *testing.T) {
	ctx := context.Background()
	input := BillInput{
		ClientID: "store-persist-open",
		Currency: "USD",
		Period:   "2099-08",
	}
	id := billID(input.ClientID, input.Currency, input.Period)
	cleanupActivityBill(t, ctx, id)

	fresh, inserted, err := persistOpenBillResource(ctx, input)
	if err != nil {
		t.Fatalf("persist fresh open bill: %v", err)
	}
	if !inserted {
		t.Fatal("fresh persist inserted=false, want true")
	}
	if fresh.BillID != id || fresh.ClientID != input.ClientID || fresh.Currency != input.Currency || fresh.Period != input.Period {
		t.Fatalf("fresh resource = %#v, want input identity", fresh)
	}
	if fresh.Status != OPEN.String() || fresh.TotalMinorAmount != "0" || fresh.ItemCount != 0 || fresh.OpenedAt.IsZero() || fresh.ClosedAt != nil {
		t.Fatalf("fresh resource = %#v, want persisted OPEN zero-item bill", fresh)
	}

	existing, inserted, err := persistOpenBillResource(ctx, input)
	if err != nil {
		t.Fatalf("persist existing open bill: %v", err)
	}
	if inserted {
		t.Fatal("existing persist inserted=true, want false")
	}
	if existing.BillID != fresh.BillID || !existing.OpenedAt.Equal(fresh.OpenedAt) {
		t.Fatalf("existing resource = %#v, want original persisted row %#v", existing, fresh)
	}
	assertActivityBillRow(t, ctx, id, input.ClientID, input.Currency, input.Period, "OPEN", 1)
}

func TestPersistOpenBillResourceCanceledContextDoesNotInsert(t *testing.T) {
	cleanupCtx := context.Background()
	input := BillInput{
		ClientID: "store-persist-canceled",
		Currency: "USD",
		Period:   "2099-09",
	}
	id := billID(input.ClientID, input.Currency, input.Period)
	cleanupActivityBill(t, cleanupCtx, id)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := persistOpenBillResource(ctx, input); err == nil {
		t.Fatal("persist with canceled context returned nil error")
	}
	if _, err := readBillMetadata(cleanupCtx, id); !errors.Is(err, sqldb.ErrNoRows) {
		t.Fatalf("canceled persist read error = %v, want no row", err)
	}
}

func TestReadBillMetadataReturnsOnlyQueriedColumns(t *testing.T) {
	ctx := context.Background()
	id := "bill-store-metadata-USD-2099-10"
	cleanupActivityBill(t, ctx, id)
	seedActivityBill(t, ctx, id, "store-metadata", "USD", "2099-10", "OPEN", nil)
	seedAPILineItem(t, ctx, id, "ref-store-metadata", 1250, "USD", "wire_transfer", "Wire")

	metadata, err := readBillMetadata(ctx, id)
	if err != nil {
		t.Fatalf("read bill metadata: %v", err)
	}
	if metadata.TotalMinorAmount != "" || metadata.ItemCount != 0 {
		t.Fatalf("metadata aggregates = %q/%d, want zero values for columns not queried", metadata.TotalMinorAmount, metadata.ItemCount)
	}

	resource, err := readBillResource(ctx, id)
	if err != nil {
		t.Fatalf("read full bill resource: %v", err)
	}
	if resource.TotalMinorAmount != "1250" || resource.ItemCount != 1 {
		t.Fatalf("queried aggregates = %q/%d, want 1250/1", resource.TotalMinorAmount, resource.ItemCount)
	}
}
