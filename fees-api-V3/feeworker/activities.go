package feeworker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"encore.app/charge"
	"encore.app/fees"
	"encore.dev/beta/errs"
	"go.temporal.io/sdk/temporal"
)

const (
	ActivityPublishPending   = "ActivityPublishPending"
	ActivityPublishFinalized = "ActivityPublishFinalized"
	ActivityLongRunning      = "ActivityLongRunning"
	ActivityAutoCloseBill    = "ActivityAutoCloseBill"
)

const maxLongRunningDelay = 2 * time.Second

type lineItemStatusPublisher interface {
	PublishLineItemStatus(context.Context, *charge.PublishLineItemStatusRequest) error
}

type encoreLineItemStatusPublisher struct{}

func (encoreLineItemStatusPublisher) PublishLineItemStatus(ctx context.Context, req *charge.PublishLineItemStatusRequest) error {
	return charge.PublishLineItemStatus(ctx, req)
}

type longRunningOperation func(context.Context, LedgerRow) error

type billSealClient interface {
	SealBill(context.Context, *fees.SealBillRequest) (*fees.CloseBillResponse, error)
}

type encoreBillSealClient struct{}

func (encoreBillSealClient) SealBill(ctx context.Context, req *fees.SealBillRequest) (*fees.CloseBillResponse, error) {
	return fees.SealBill(ctx, req)
}

type Activities struct {
	lineItemStatusClient lineItemStatusPublisher
	longRunningOperation longRunningOperation
	billSealClient       billSealClient
}

func NewActivities() *Activities {
	return &Activities{
		lineItemStatusClient: encoreLineItemStatusPublisher{},
		longRunningOperation: simulateLongRunningTransaction,
		billSealClient:       encoreBillSealClient{},
	}
}

func (a *Activities) ActivityPublishPending(ctx context.Context, row LedgerRow) error {
	if a == nil || a.lineItemStatusClient == nil {
		return errors.New("publish pending line item status: charge client is not configured")
	}

	if err := a.lineItemStatusClient.PublishLineItemStatus(ctx, &charge.PublishLineItemStatusRequest{
		BillID:      row.BillID,
		Reference:   row.Reference,
		MinorAmount: strconv.FormatInt(row.AmountMinor, 10),
		Currency:    row.Currency,
		FeeType:     row.FeeType,
		Description: row.Description,
		Status:      charge.LineItemStatusPending,
	}); err != nil {
		return fmt.Errorf("publish pending line item status: %w", err)
	}
	return nil
}

func (a *Activities) ActivityPublishFinalized(ctx context.Context, row LedgerRow) error {
	if a == nil || a.lineItemStatusClient == nil {
		return errors.New("publish finalized line item status: charge client is not configured")
	}

	if err := a.lineItemStatusClient.PublishLineItemStatus(ctx, &charge.PublishLineItemStatusRequest{
		BillID:      row.BillID,
		Reference:   row.Reference,
		MinorAmount: strconv.FormatInt(row.AmountMinor, 10),
		Currency:    row.Currency,
		FeeType:     row.FeeType,
		Description: row.Description,
		Status:      charge.LineItemStatusFinalized,
	}); err != nil {
		return fmt.Errorf("publish finalized line item status: %w", err)
	}
	return nil
}

func (a *Activities) ActivityLongRunning(ctx context.Context, row LedgerRow) error {
	if a == nil || a.longRunningOperation == nil {
		return errors.New("long running transaction: operation is not configured")
	}
	if err := a.longRunningOperation(ctx, row); err != nil {
		return fmt.Errorf("long running transaction: %w", err)
	}
	return nil
}

func simulateLongRunningTransaction(ctx context.Context, _ LedgerRow) error {
	return waitForLongRunningTransaction(ctx, randomLongRunningDelay())
}

func randomLongRunningDelay() time.Duration {
	return time.Duration(rand.Int64N(int64(maxLongRunningDelay) + 1))
}

func waitForLongRunningTransaction(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Activities) ActivityAutoCloseBill(ctx context.Context, billID string) error {
	if a == nil || a.billSealClient == nil {
		return errors.New("auto-close bill: seal client is not configured")
	}

	resp, err := a.billSealClient.SealBill(ctx, &fees.SealBillRequest{BillID: billID})
	if err != nil {
		wrapped := fmt.Errorf("auto-close bill %s: %w", billID, err)
		if errs.Code(err) == errs.NotFound {
			return temporal.NewNonRetryableApplicationError(wrapped.Error(), "BillNotFound", wrapped)
		}
		return wrapped
	}
	if resp == nil {
		return fmt.Errorf("auto-close bill %s: seal endpoint returned no response", billID)
	}
	if !resp.Success {
		return fmt.Errorf("auto-close bill %s: seal endpoint did not confirm success", billID)
	}
	return nil
}
