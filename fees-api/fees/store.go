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

func readOpenedBillResource(ctx context.Context, id string) (*BillResource, error) {
	return readBillResource(ctx, id)
}

func readBillResource(ctx context.Context, id string) (*BillResource, error) {
	var resource BillResource
	var totalMinor int64
	err := db.QueryRow(ctx, `
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

func readBillWithLineItemsResource(ctx context.Context, id string) (*InvoiceResource, error) {
	resource, err := readBillResource(ctx, id)
	if err != nil {
		return nil, err
	}

	items, err := readBillLineItems(ctx, id)
	if err != nil {
		return nil, err
	}

	return &InvoiceResource{
		BillResource: *resource,
		LineItems:    items,
	}, nil
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
	rows, err := db.Query(ctx, `
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
