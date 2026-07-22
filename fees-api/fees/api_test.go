package fees

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestListBillsReturnsEmptyScaffoldResponse(t *testing.T) {
	resp, err := (&Service{}).ListBills(context.Background())
	if err != nil {
		t.Fatalf("ListBills returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("ListBills returned nil response")
	}
	if len(resp.Bills) != 0 {
		t.Fatalf("expected no bills, got %d", len(resp.Bills))
	}
	if resp.NextCursor != "" {
		t.Fatalf("expected empty next cursor, got %q", resp.NextCursor)
	}
	if resp.HasMore {
		t.Fatal("expected hasMore=false")
	}
}

func TestBillResourceEncodesClosedAtNull(t *testing.T) {
	openedAt := time.Date(2026, 7, 3, 14, 21, 0, 0, time.UTC)
	resource := BillResource{
		BillID:           "bill-acme-USD-2026-07",
		ClientID:         "acme",
		Currency:         "USD",
		Period:           "2026-07",
		Status:           "OPEN",
		TotalMinorAmount: "0",
		OpenedAt:         openedAt,
	}

	encoded, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("marshal BillResource: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal BillResource JSON: %v", err)
	}
	if _, ok := body["closedAt"]; !ok {
		t.Fatal("expected closedAt key to be present")
	}
	if _, ok := body["totalMinorAmount"]; !ok {
		t.Fatal("expected totalMinorAmount key to be present")
	}
	if _, ok := body["total_minor_amount"]; ok {
		t.Fatal("did not expect legacy total_minor_amount key")
	}
	if body["closedAt"] != nil {
		t.Fatalf("expected closedAt=null, got %#v", body["closedAt"])
	}
	if body["openedAt"] != "2026-07-03T14:21:00Z" {
		t.Fatalf("openedAt = %#v, want RFC3339 timestamp", body["openedAt"])
	}
}
