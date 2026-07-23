package fees

import (
	"context"
	"errors"
	"fmt"

	"encore.dev/storage/sqldb"
	"go.temporal.io/sdk/temporal"
)

const (
	ActivityPersistBill     = "ActivityPersistBill"
	ActivityPersistLineItem = "ActivityPersistLineItem"
	ActivityPersistInvoice  = "ActivityPersistInvoice"
)

type Activities struct {
	db *sqldb.Database
}

func NewActivities(db *sqldb.Database) *Activities {
	return &Activities{db: db}
}

func temporalNonRetryable(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "BillNotOpen", err)
}

func (a *Activities) ActivityPersistBill(ctx context.Context, input BillInput) error {
	_, err := a.db.Exec(ctx, `
		INSERT INTO bills (bill_id, client_id, currency, period, status)
		VALUES ($1, $2, $3, $4, 'OPEN')
		ON CONFLICT (bill_id) DO NOTHING`,
		billID(input.ClientID, input.Currency, input.Period),
		input.ClientID,
		input.Currency,
		input.Period,
	)
	if err != nil {
		return fmt.Errorf("persist bill: %w", err)
	}
	return nil
}

func (a *Activities) ActivityPersistLineItem(ctx context.Context, row LedgerRow) (bool, error) {
	tag, err := a.db.Exec(ctx, `
		INSERT INTO line_items
			(bill_id, reference, amount_minor, currency, fee_type, description)
		SELECT $1, $2, $3, $4, $5, $6
		 WHERE EXISTS (
			SELECT 1
			  FROM bills
			 WHERE bill_id = $1
			   AND status = 'OPEN'
		 )
		ON CONFLICT (bill_id, reference) DO NOTHING`,
		row.BillID,
		row.Reference,
		row.AmountMinor,
		row.Currency,
		row.FeeType,
		row.Description,
	)
	if err != nil {
		return false, fmt.Errorf("insert line item: %w", err)
	}

	if tag.RowsAffected() == 1 {
		return true, nil
	}

	var exists bool
	if err := a.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM line_items
			 WHERE bill_id = $1
			   AND reference = $2
		)`,
		row.BillID,
		row.Reference,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("disambiguate line item insert: %w", err)
	}
	if exists {
		return false, nil
	}

	return false, temporalNonRetryable(
		fmt.Errorf("bill %s not OPEN; cannot apply line item %s", row.BillID, row.Reference))
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
