package feesworkflowcontract

import (
	"testing"
	"time"
)

func TestTemporalNamesAndTaskQueue(t *testing.T) {
	if TaskQueue != "feeworker" {
		t.Fatalf("TaskQueue = %q, want feeworker", TaskQueue)
	}
	if BillWorkflowName != "BillWorkflow" || SignalCloseBill != "closeBill" || QueryGetBill != "getBill" {
		t.Fatalf("Temporal names = %q/%q/%q", BillWorkflowName, SignalCloseBill, QueryGetBill)
	}
}

func TestBillIDAndPeriodEnd(t *testing.T) {
	if got := BillID("acme", "USD", "2026-12"); got != "bill-acme-USD-2026-12" {
		t.Fatalf("BillID = %q", got)
	}
	want := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := ResolvePeriodEnd("2026-12"); !got.Equal(want) {
		t.Fatalf("ResolvePeriodEnd = %s, want %s", got, want)
	}
}
