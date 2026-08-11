package feeworker

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"encore.app/charge"
	"encore.app/fees"
	apierrs "encore.dev/beta/errs"
	"go.temporal.io/sdk/temporal"
)

type recordingLineItemStatusPublisher struct {
	err      error
	requests []charge.PublishLineItemStatusRequest
}

type recordingBillSealClient struct {
	response *fees.CloseBillResponse
	err      error
	requests []fees.SealBillRequest
}

func (c *recordingBillSealClient) SealBill(_ context.Context, req *fees.SealBillRequest) (*fees.CloseBillResponse, error) {
	if req != nil {
		c.requests = append(c.requests, *req)
	}
	return c.response, c.err
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

func TestActivityPublishFinalizedFormatsInt64AndPreservesPayload(t *testing.T) {
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
				BillID:      "bill-activity-finalized-USD-2099-01",
				Reference:   "ref-finalized-" + tt.name,
				AmountMinor: tt.amount,
				Currency:    "USD",
				FeeType:     "wire_transfer",
				Description: "Outbound USD wire",
			}

			if err := activities.ActivityPublishFinalized(context.Background(), row); err != nil {
				t.Fatalf("ActivityPublishFinalized returned error: %v", err)
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
			if got.Status != charge.LineItemStatusFinalized {
				t.Fatalf("published status = %q, want %q", got.Status, charge.LineItemStatusFinalized)
			}
		})
	}
}

func TestActivityPublishFinalizedPropagatesPublisherFailure(t *testing.T) {
	publishErr := errors.New("charge callback unavailable")
	publisher := &recordingLineItemStatusPublisher{err: publishErr}
	activities := &Activities{lineItemStatusClient: publisher}

	err := activities.ActivityPublishFinalized(context.Background(), LedgerRow{AmountMinor: 25})
	if !errors.Is(err, publishErr) {
		t.Fatalf("ActivityPublishFinalized error = %v, want wrapped %v", err, publishErr)
	}
}

func TestActivityPublishFinalizedRejectsMissingPublisher(t *testing.T) {
	err := (&Activities{}).ActivityPublishFinalized(context.Background(), LedgerRow{})
	if err == nil {
		t.Fatal("ActivityPublishFinalized returned nil error with no publisher")
	}
}

func TestActivityLongRunningInvokesConfiguredOperation(t *testing.T) {
	row := LedgerRow{BillID: "bill-long-running", Reference: "ref-long-running"}
	var got LedgerRow
	activities := &Activities{longRunningOperation: func(_ context.Context, input LedgerRow) error {
		got = input
		return nil
	}}

	if err := activities.ActivityLongRunning(context.Background(), row); err != nil {
		t.Fatalf("ActivityLongRunning returned error: %v", err)
	}
	if got != row {
		t.Fatalf("long-running operation input = %#v, want %#v", got, row)
	}
}

func TestActivityLongRunningPropagatesOperationFailure(t *testing.T) {
	operationErr := errors.New("external network rejected transaction")
	activities := &Activities{longRunningOperation: func(context.Context, LedgerRow) error {
		return operationErr
	}}

	err := activities.ActivityLongRunning(context.Background(), LedgerRow{})
	if !errors.Is(err, operationErr) {
		t.Fatalf("ActivityLongRunning error = %v, want wrapped %v", err, operationErr)
	}
}

func TestActivityLongRunningRejectsMissingOperation(t *testing.T) {
	if err := (&Activities{}).ActivityLongRunning(context.Background(), LedgerRow{}); err == nil {
		t.Fatal("ActivityLongRunning returned nil error with no operation")
	}
}

func TestWaitForLongRunningTransactionHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForLongRunningTransaction(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}
}

func TestRandomLongRunningDelayStaysWithinContinuousRange(t *testing.T) {
	sawSubsecondPrecision := false
	for i := 0; i < 1_000; i++ {
		delay := randomLongRunningDelay()
		if delay < 0 || delay > maxLongRunningDelay {
			t.Fatalf("random delay = %s, want within [0s, %s]", delay, maxLongRunningDelay)
		}
		if delay%time.Second != 0 {
			sawSubsecondPrecision = true
		}
	}
	if !sawSubsecondPrecision {
		t.Fatal("random delays used only whole seconds, want a continuous nanosecond range")
	}
}

func TestActivityAutoCloseBillCallsSealEndpoint(t *testing.T) {
	billID := "bill-activity-auto-close-USD-2099-01"
	client := &recordingBillSealClient{response: &fees.CloseBillResponse{Success: true}}
	activities := &Activities{billSealClient: client}

	if err := activities.ActivityAutoCloseBill(context.Background(), billID); err != nil {
		t.Fatalf("ActivityAutoCloseBill returned error: %v", err)
	}
	if len(client.requests) != 1 || client.requests[0] != (fees.SealBillRequest{BillID: billID}) {
		t.Fatalf("seal requests = %#v, want exact bill ID", client.requests)
	}
}

func TestActivityAutoCloseBillPropagatesRetryableFailures(t *testing.T) {
	sealErr := errors.New("seal endpoint unavailable")
	client := &recordingBillSealClient{err: sealErr}
	err := (&Activities{billSealClient: client}).ActivityAutoCloseBill(context.Background(), "bill-retry")
	if !errors.Is(err, sealErr) {
		t.Fatalf("ActivityAutoCloseBill error = %v, want wrapped %v", err, sealErr)
	}

	for name, response := range map[string]*fees.CloseBillResponse{
		"nil response":   nil,
		"false response": {Success: false},
	} {
		t.Run(name, func(t *testing.T) {
			err := (&Activities{billSealClient: &recordingBillSealClient{response: response}}).
				ActivityAutoCloseBill(context.Background(), "bill-unconfirmed")
			if err == nil {
				t.Fatal("ActivityAutoCloseBill returned nil error for unconfirmed response")
			}
		})
	}
}

func TestActivityAutoCloseBillRejectsMissingClient(t *testing.T) {
	if err := (&Activities{}).ActivityAutoCloseBill(context.Background(), "bill-missing-client"); err == nil {
		t.Fatal("ActivityAutoCloseBill returned nil error with no seal client")
	}
}

func TestActivityAutoCloseBillMakesMissingBillNonRetryable(t *testing.T) {
	client := &recordingBillSealClient{
		err: apierrs.B().Code(apierrs.NotFound).Msg("bill does not exist").Err(),
	}
	err := (&Activities{billSealClient: client}).ActivityAutoCloseBill(context.Background(), "bill-missing")

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T %v, want temporal ApplicationError", err, err)
	}
	if appErr.Type() != "BillNotFound" || !appErr.NonRetryable() {
		t.Fatalf("application error type/nonRetryable = %q/%v, want BillNotFound/true", appErr.Type(), appErr.NonRetryable())
	}
}
