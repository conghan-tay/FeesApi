package fees

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"encore.dev/storage/sqldb"
)

const (
	defaultListBillsLimit = 50
	maxListBillsLimit     = 200
)

var errInvalidCursor = errors.New("invalid cursor")

type listBillsOptions struct {
	ClientID string
	Status   string
	Currency string
	Period   string
	Cursor   string
	Limit    int
}

type listBillsCursor struct {
	OpenedAt string `json:"openedAt"`
	BillID   string `json:"billId"`
}

type ledgerQuerier interface {
	QueryRow(ctx context.Context, query string, args ...interface{}) *sqldb.Row
	Query(ctx context.Context, query string, args ...interface{}) (*sqldb.Rows, error)
}

func readOpenedBillResource(ctx context.Context, id string) (*BillResource, error) {
	return readBillResource(ctx, id)
}

func isSupportedCurrency(ctx context.Context, code string) (bool, error) {
	var supported bool
	err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM currencies
			 WHERE code = $1
		)`,
		code,
	).Scan(&supported)
	if err != nil {
		return false, err
	}
	return supported, nil
}

func readBillResource(ctx context.Context, id string) (*BillResource, error) {
	return readBillResourceFrom(ctx, db, id)
}

func readBillResourceFrom(ctx context.Context, q ledgerQuerier, id string) (*BillResource, error) {
	var resource BillResource
	var totalMinor int64
	err := q.QueryRow(ctx, `
		SELECT b.bill_id,
		       b.client_id,
		       b.currency,
		       b.period,
		       b.status,
		       COALESCE(SUM(li.amount_minor), 0),
		       COUNT(li.id),
		       b.opened_at,
		       b.closed_at
		  FROM bills b
		  LEFT JOIN line_items li ON li.bill_id = b.bill_id
		 WHERE b.bill_id = $1
		 GROUP BY b.bill_id, b.client_id, b.currency, b.period, b.status, b.opened_at, b.closed_at`,
		id,
	).Scan(
		&resource.BillID,
		&resource.ClientID,
		&resource.Currency,
		&resource.Period,
		&resource.Status,
		&totalMinor,
		&resource.ItemCount,
		&resource.OpenedAt,
		&resource.ClosedAt,
	)
	if err != nil {
		return nil, err
	}
	resource.TotalMinorAmount = strconv.FormatInt(totalMinor, 10)
	return &resource, nil
}

func readBillMetadataFrom(ctx context.Context, q ledgerQuerier, id string) (*BillResource, error) {
	var resource BillResource
	err := q.QueryRow(ctx, `
		SELECT bill_id,
		       client_id,
		       currency,
		       period,
		       status,
		       opened_at,
		       closed_at
		  FROM bills
		 WHERE bill_id = $1`,
		id,
	).Scan(
		&resource.BillID,
		&resource.ClientID,
		&resource.Currency,
		&resource.Period,
		&resource.Status,
		&resource.OpenedAt,
		&resource.ClosedAt,
	)
	if err != nil {
		return nil, err
	}
	resource.TotalMinorAmount = "0"
	resource.ItemCount = 0
	return &resource, nil
}

func readBillWithLineItemsResource(ctx context.Context, id string) (*InvoiceResource, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin repeatable-read bill detail transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		return nil, fmt.Errorf("set repeatable-read bill detail transaction: %w", err)
	}

	resource, err := readBillMetadataFrom(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	items, err := readBillLineItemsFrom(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if err := applyLineItemAggregates(resource, items); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit repeatable-read bill detail transaction: %w", err)
	}
	committed = true

	return invoiceResourceFromBill(resource, items), nil
}

func readClosedInvoiceResource(ctx context.Context, id string) (*InvoiceResource, error) {
	resource, err := readBillWithLineItemsResource(ctx, id)
	if err != nil {
		return nil, err
	}
	if resource.Status != CLOSED.String() {
		return nil, sqldb.ErrNoRows
	}
	return resource, nil
}

func readBillLineItems(ctx context.Context, id string) ([]LineItemResource, error) {
	return readBillLineItemsFrom(ctx, db, id)
}

func readBillLineItemsFrom(ctx context.Context, q ledgerQuerier, id string) ([]LineItemResource, error) {
	rows, err := q.Query(ctx, `
		SELECT reference,
		       amount_minor,
		       currency,
		       fee_type,
		       description,
		       applied_at
		  FROM line_items
		 WHERE bill_id = $1
		 ORDER BY id`,
		id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []LineItemResource{}
	for rows.Next() {
		var item LineItemResource
		var amountMinor int64
		if err := rows.Scan(
			&item.Reference,
			&amountMinor,
			&item.Currency,
			&item.FeeType,
			&item.Description,
			&item.AppliedAt,
		); err != nil {
			return nil, err
		}
		item.MinorAmount = strconv.FormatInt(amountMinor, 10)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func applyLineItemAggregates(resource *BillResource, items []LineItemResource) error {
	var totalMinor int64
	for _, item := range items {
		amountMinor, err := strconv.ParseInt(item.MinorAmount, 10, 64)
		if err != nil {
			return fmt.Errorf("parse line item amount %q: %w", item.MinorAmount, err)
		}
		totalMinor += amountMinor
	}
	resource.TotalMinorAmount = strconv.FormatInt(totalMinor, 10)
	resource.ItemCount = len(items)
	return nil
}

func listBillResources(ctx context.Context, opts listBillsOptions) (*ListBillsResponse, error) {
	limit := opts.Limit
	if limit == 0 {
		limit = defaultListBillsLimit
	}

	where := []string{"1 = 1"}
	args := []interface{}{}
	addFilter := func(sql string, value interface{}) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(sql, len(args)))
	}

	if opts.ClientID != "" {
		addFilter("b.client_id = $%d", opts.ClientID)
	}
	if opts.Status != "" {
		addFilter("b.status = $%d", opts.Status)
	}
	if opts.Currency != "" {
		addFilter("b.currency = $%d", opts.Currency)
	}
	if opts.Period != "" {
		addFilter("b.period = $%d", opts.Period)
	}
	if opts.Cursor != "" {
		openedAt, billID, err := decodeListBillsCursor(opts.Cursor)
		if err != nil {
			return nil, err
		}
		args = append(args, openedAt, billID)
		openedAtParam := len(args) - 1
		billIDParam := len(args)
		where = append(where, fmt.Sprintf("(b.opened_at, b.bill_id) < ($%d, $%d)", openedAtParam, billIDParam))
	}

	args = append(args, limit+1)
	limitParam := len(args)
	query := fmt.Sprintf(`
		WITH page AS (
			SELECT b.bill_id,
			       b.client_id,
			       b.currency,
			       b.period,
			       b.status,
			       b.opened_at,
			       b.closed_at
			  FROM bills b
			 WHERE %s
			 ORDER BY b.opened_at DESC, b.bill_id DESC
			 LIMIT $%d
		)
		SELECT page.bill_id,
		       page.client_id,
		       page.currency,
		       page.period,
		       page.status,
		       COALESCE(SUM(li.amount_minor), 0),
		       COUNT(li.id),
		       page.opened_at,
		       page.closed_at
		  FROM page
		  LEFT JOIN line_items li ON li.bill_id = page.bill_id
		 GROUP BY page.bill_id, page.client_id, page.currency, page.period, page.status, page.opened_at, page.closed_at
		 ORDER BY page.opened_at DESC, page.bill_id DESC`,
		strings.Join(where, " AND "),
		limitParam,
	)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bills := []BillResource{}
	for rows.Next() {
		var resource BillResource
		var totalMinor int64
		if err := rows.Scan(
			&resource.BillID,
			&resource.ClientID,
			&resource.Currency,
			&resource.Period,
			&resource.Status,
			&totalMinor,
			&resource.ItemCount,
			&resource.OpenedAt,
			&resource.ClosedAt,
		); err != nil {
			return nil, err
		}
		resource.TotalMinorAmount = strconv.FormatInt(totalMinor, 10)
		bills = append(bills, resource)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(bills) > limit
	if hasMore {
		bills = bills[:limit]
	}

	nextCursor := ""
	if hasMore && len(bills) > 0 {
		nextCursor = encodeListBillsCursor(bills[len(bills)-1])
	}

	return &ListBillsResponse{
		Bills:      bills,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func encodeListBillsCursor(resource BillResource) string {
	payload, _ := json.Marshal(listBillsCursor{
		OpenedAt: resource.OpenedAt.UTC().Format(time.RFC3339Nano),
		BillID:   resource.BillID,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeListBillsCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", errInvalidCursor
	}

	var payload listBillsCursor
	if err := json.Unmarshal(raw, &payload); err != nil {
		return time.Time{}, "", errInvalidCursor
	}
	if payload.OpenedAt == "" || payload.BillID == "" {
		return time.Time{}, "", errInvalidCursor
	}

	openedAt, err := time.Parse(time.RFC3339Nano, payload.OpenedAt)
	if err != nil {
		return time.Time{}, "", errInvalidCursor
	}
	return openedAt, payload.BillID, nil
}
