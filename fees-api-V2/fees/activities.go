package fees

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"encore.app/charge"
	"encore.dev/storage/sqldb"
	"go.temporal.io/sdk/temporal"
)

const (
	ActivityPublishPending   = "ActivityPublishPending"
	ActivityPublishFinalized = "ActivityPublishFinalized"
	ActivityLongRunning      = "ActivityLongRunning"
	ActivityPersistInvoice   = "ActivityPersistInvoice"
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

type Activities struct {
	db                   *sqldb.Database
	lineItemStatusClient lineItemStatusPublisher
	longRunningOperation longRunningOperation
}

func NewActivities(db *sqldb.Database) *Activities {
	return &Activities{
		db:                   db,
		lineItemStatusClient: encoreLineItemStatusPublisher{},
		longRunningOperation: simulateLongRunningTransaction,
	}
}

func temporalNonRetryable(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "BillNotOpen", err)
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

func (a *Activities) ActivityPersistInvoice(ctx context.Context, billID string) (BillView, error) {
	var out BillView
	err := a.db.QueryRow(ctx, `
		UPDATE bills
		   SET status = 'CLOSED',
		       closed_at = now()
		 WHERE bill_id = $1
		   AND status = 'OPEN'
		RETURNING client_id, currency, period, status`,
		billID,
	).Scan(&out.ClientID, &out.Currency, &out.Period, &out.Status)
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, sqldb.ErrNoRows) {
		return BillView{}, fmt.Errorf("seal bill %s: %w", billID, err)
	}

	err = a.db.QueryRow(ctx, `
		SELECT client_id, currency, period, status
		  FROM bills
		 WHERE bill_id = $1`,
		billID,
	).Scan(&out.ClientID, &out.Currency, &out.Period, &out.Status)
	if err == nil {
		return out, nil
	}
	if errors.Is(err, sqldb.ErrNoRows) {
		return BillView{}, temporalNonRetryable(fmt.Errorf("bill %s does not exist", billID))
	}

	return BillView{}, fmt.Errorf("read sealed bill %s: %w", billID, err)
}
