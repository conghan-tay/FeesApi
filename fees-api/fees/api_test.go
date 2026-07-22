package fees

import (
	"context"
	"testing"
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
