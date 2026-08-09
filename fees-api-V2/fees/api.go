package fees

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/rlog"
	"encore.dev/storage/sqldb"
	"encore.dev/types/option"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

var (
	periodPattern   = regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

type BillResource struct {
	BillID           string     `json:"billId"`
	ClientID         string     `json:"clientId"`
	Currency         string     `json:"currency"`
	Period           string     `json:"period"`
	Status           string     `json:"status"`
	TotalMinorAmount string     `json:"totalMinorAmount"`
	ItemCount        int        `json:"itemCount"`
	OpenedAt         time.Time  `json:"openedAt"`
	ClosedAt         *time.Time `json:"closedAt"`
}

type InvoiceResource struct {
	BillID           string             `json:"billId"`
	ClientID         string             `json:"clientId"`
	Currency         string             `json:"currency"`
	Period           string             `json:"period"`
	Status           string             `json:"status"`
	TotalMinorAmount string             `json:"totalMinorAmount"`
	ItemCount        int                `json:"itemCount"`
	OpenedAt         time.Time          `json:"openedAt"`
	ClosedAt         *time.Time         `json:"closedAt"`
	LineItems        []LineItemResource `json:"lineItems"`
}

type GetBillResponse struct {
	BillID           string             `json:"billId"`
	ClientID         string             `json:"clientId"`
	Currency         string             `json:"currency"`
	Period           string             `json:"period"`
	Status           string             `json:"status"`
	TotalMinorAmount string             `json:"totalMinorAmount"`
	ItemCount        int                `json:"itemCount"`
	OpenedAt         time.Time          `json:"openedAt"`
	ClosedAt         *time.Time         `json:"closedAt"`
	LineItems        []LineItemResource `json:"lineItems,omitempty"`
}

type LineItemResource struct {
	Reference   string    `json:"reference"`
	MinorAmount string    `json:"minorAmount"`
	Currency    string    `json:"currency"`
	FeeType     string    `json:"feeType"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
	AppliedAt   time.Time `json:"appliedAt"`
}

type ListBillsResponse struct {
	Bills      []BillResource `json:"bills"`
	NextCursor string         `json:"nextCursor"`
	HasMore    bool           `json:"hasMore"`
}

type OpenBillRequest struct {
	ClientID string `json:"clientId"`
	Currency string `json:"currency"`
	Period   string `json:"period"`
}

type OpenBillResponse struct {
	BillID           string     `json:"billId"`
	ClientID         string     `json:"clientId"`
	Currency         string     `json:"currency"`
	Period           string     `json:"period"`
	Status           string     `json:"status"`
	TotalMinorAmount string     `json:"totalMinorAmount"`
	ItemCount        int        `json:"itemCount"`
	OpenedAt         time.Time  `json:"openedAt"`
	ClosedAt         *time.Time `json:"closedAt"`
	Location         string     `header:"Location"`
	HTTPStatus       int        `encore:"httpstatus"`
}

type AddLineItemRequest struct {
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
}

type CloseBillRequest struct {
	Reason string `json:"reason"`
}

type GetBillRequest struct {
	IncludeLineItems bool `query:"includeLineItems"`
}

type ListBillsRequest struct {
	ClientID string             `query:"clientId"`
	Status   string             `query:"status"`
	Currency string             `query:"currency"`
	Period   string             `query:"period"`
	Cursor   string             `query:"cursor"`
	Limit    option.Option[int] `query:"limit"`
}

type AddLineItemResponse struct {
	Reference  string `json:"reference"`
	Applied    bool   `json:"applied"`
	HTTPStatus int    `encore:"httpstatus"`
}

type APIErrorDetails struct {
	Type string `json:"type"`
}

func (APIErrorDetails) ErrDetails() {}

//encore:api public method=POST path=/v1/bills
func (s *Service) OpenBill(ctx context.Context, req *OpenBillRequest) (*OpenBillResponse, error) {
	if req == nil {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "request body is required")
	}
	billInput, validationErr := validateOpenBillRequest(*req)
	if validationErr != "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", validationErr)
	}
	supported, err := isSupportedCurrency(ctx, billInput.Currency)
	if err != nil {
		rlog.Error("open bill: supported currency lookup failed", "currency", billInput.Currency, "problemType", "open-unavailable", "err", err)
		return nil, apiError(errs.Unavailable, "open-unavailable", "open workflow did not complete; retry after a short delay")
	}
	if !supported {
		return nil, apiError(errs.InvalidArgument, "unsupported-currency", "currency is not supported")
	}
	if s == nil || s.temporalClient == nil {
		return nil, apiError(errs.Unavailable, "open-unavailable", "temporal client is not available")
	}

	id := billID(billInput.ClientID, billInput.Currency, billInput.Period)
	resource, err := readBillResource(ctx, id)
	if errors.Is(err, sqldb.ErrNoRows) {
		// Bill does not exists, Fresh Open

		// trying to open for a period which elapsed
		if !time.Now().UTC().Before(resolvePeriodEnd(billInput.Period)) {
			return nil, apiError(errs.InvalidArgument, "period-elapsed", "billing period has already elapsed")
		}
		// Persist
		resource, _, err = persistOpenBillResource(ctx, billInput)
	}

	// Other Error
	if err != nil {
		rlog.Error("open bill: persist or recover bill failed", "billID", id, "problemType", "open-unavailable", "err", err)
		return nil, apiError(errs.Unavailable, "open-unavailable", "bill could not be persisted; retry after a short delay")
	}

	if resource.Status != OPEN.String() {
		return nil, apiError(errs.Aborted, "bill-not-open", "bill workflow not open")
	}

	_, err = s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                s.temporalConfig.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, BillWorkflow, billInput)
	if err != nil {
		return nil, openTemporalError(err)
	}

	return &OpenBillResponse{
		BillID:           resource.BillID,
		ClientID:         resource.ClientID,
		Currency:         resource.Currency,
		Period:           resource.Period,
		Status:           resource.Status,
		TotalMinorAmount: resource.TotalMinorAmount,
		ItemCount:        resource.ItemCount,
		OpenedAt:         resource.OpenedAt,
		ClosedAt:         resource.ClosedAt,
		Location:         "/v1/bills/" + id,
		HTTPStatus:       http.StatusCreated,
	}, nil
}

//encore:api public method=POST path=/v1/bills/:billId/line-items
func (s *Service) AddLineItem(ctx context.Context, billId string, req *AddLineItemRequest) (*AddLineItemResponse, error) {
	billID := billId
	if billID == "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "billId is required")
	}
	if req == nil {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "request body is required")
	}

	lineItem, validationErr := validateAddLineItemRequest(*req)
	if validationErr != "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", validationErr)
	}
	if s == nil || s.temporalClient == nil {
		return nil, apiError(errs.Unavailable, "add-line-item-unavailable", "add-line-item update did not complete; retry after a short delay")
	}

	handle, err := s.temporalClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   billID,
		UpdateName:   UpdateAddLineItem,
		Args:         []interface{}{lineItem},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, addLineItemTemporalError(ctx, billID, err)
	}

	var result LineItemResult
	if err := handle.Get(ctx, &result); err != nil {
		return nil, addLineItemTemporalError(ctx, billID, err)
	}

	status := http.StatusOK
	if result.Applied {
		status = http.StatusCreated
	}
	return &AddLineItemResponse{
		Reference:  result.Reference,
		Applied:    result.Applied,
		HTTPStatus: status,
	}, nil
}

//encore:api public method=POST path=/v1/bills/:billId/close
func (s *Service) CloseBill(ctx context.Context, billId string, req *CloseBillRequest) (*InvoiceResource, error) {
	id := billId
	if id == "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "billId is required")
	}
	input := CloseBillRequest{}
	if req != nil {
		input = *req
	}

	resource, err := readBillResource(ctx, id)
	if err != nil {
		if errors.Is(err, sqldb.ErrNoRows) {
			return nil, apiError(errs.NotFound, "bill-not-found", "bill does not exist")
		}
		rlog.Error("close bill: read bill failed", "billID", id, "problemType", "close-unavailable", "err", err)
		return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
	}

	if resource.Status == CLOSED.String() {
		return closedInvoice(ctx, id)
	}
	if s == nil || s.temporalClient == nil {
		return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
	}

	if err := s.temporalClient.SignalWorkflow(ctx, id, "", SignalCloseBill, CloseSignal{Reason: input.Reason}); err != nil {
		return closeTemporalError(ctx, id, err)
	}
	if err := s.temporalClient.GetWorkflow(ctx, id, "").Get(ctx, nil); err != nil {
		return closeTemporalError(ctx, id, err)
	}

	sealed, err := readBillResource(ctx, id)
	if err != nil {
		rlog.Error("close bill: read sealed bill failed", "billID", id, "problemType", "close-unavailable", "err", err)
		return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
	}
	if sealed.Status != CLOSED.String() {
		rlog.Error("close bill: workflow completed before ledger was sealed", "billID", id, "status", sealed.Status)
		return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
	}
	return closedInvoice(ctx, id)
}

//encore:api public method=GET path=/v1/bills/:billId
func (s *Service) GetBill(ctx context.Context, billId string, req *GetBillRequest) (*GetBillResponse, error) {
	id := billId
	if id == "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "billId is required")
	}
	includeLineItems := false
	if req != nil {
		includeLineItems = req.IncludeLineItems
	}

	if includeLineItems {
		resource, err := readBillWithLineItemsResource(ctx, id)
		if err != nil {
			return nil, readBillError(id, err)
		}
		return getBillResponseFromInvoice(resource), nil
	}

	resource, err := readBillResource(ctx, id)
	if err != nil {
		return nil, readBillError(id, err)
	}
	return getBillResponseFromBill(resource), nil
}

//encore:api public method=GET path=/v1/bills
func (s *Service) ListBills(ctx context.Context, req *ListBillsRequest) (*ListBillsResponse, error) {
	opts, err := parseListBillsOptions(req)
	if err != nil {
		return nil, err
	}

	resp, err := listBillResources(ctx, opts)
	if err != nil {
		if errors.Is(err, errInvalidCursor) {
			return nil, apiError(errs.InvalidArgument, "invalid-request", "cursor is invalid")
		}
		rlog.Error("list bills: read failed", "problemType", "list-unavailable", "err", err)
		return nil, apiError(errs.Unavailable, "list-unavailable", "bills could not be listed; retry after a short delay")
	}
	return resp, nil
}

func validateOpenBillRequest(input OpenBillRequest) (BillInput, string) {
	if input.ClientID == "" {
		return BillInput{}, "clientId is required"
	}
	if !currencyPattern.MatchString(input.Currency) {
		return BillInput{}, "currency must be a three-letter uppercase ISO-4217 code"
	}
	if !periodPattern.MatchString(input.Period) {
		return BillInput{}, "period must use YYYY-MM calendar-month format"
	}
	return BillInput{
		ClientID: input.ClientID,
		Currency: input.Currency,
		Period:   input.Period,
	}, ""
}

func validateAddLineItemRequest(input AddLineItemRequest) (LineItem, string) {
	if input.Reference == "" {
		return LineItem{}, "reference is required"
	}
	if input.MinorAmount == "" {
		return LineItem{}, "minorAmount is required"
	}
	amountMinor, err := strconv.ParseInt(input.MinorAmount, 10, 64)
	if err != nil {
		return LineItem{}, "minorAmount must be an integer minor-unit amount encoded as a string"
	}
	if !currencyPattern.MatchString(input.Currency) {
		return LineItem{}, "currency must be a three-letter uppercase ISO-4217 code"
	}
	if input.FeeType == "" {
		return LineItem{}, "feeType is required"
	}
	return LineItem{
		Reference:   input.Reference,
		AmountMinor: amountMinor,
		Currency:    input.Currency,
		FeeType:     input.FeeType,
		Description: input.Description,
	}, ""
}

func parseListBillsOptions(req *ListBillsRequest) (listBillsOptions, error) {
	if req == nil {
		return listBillsOptions{Limit: defaultListBillsLimit}, nil
	}
	opts := listBillsOptions{
		ClientID: req.ClientID,
		Status:   req.Status,
		Currency: req.Currency,
		Period:   req.Period,
		Cursor:   req.Cursor,
	}

	if opts.Status != "" && opts.Status != OPEN.String() && opts.Status != CLOSED.String() {
		return listBillsOptions{}, apiError(errs.InvalidArgument, "invalid-request", "status must be OPEN or CLOSED")
	}
	if opts.Currency != "" && !currencyPattern.MatchString(opts.Currency) {
		return listBillsOptions{}, apiError(errs.InvalidArgument, "invalid-request", "currency must be a three-letter uppercase ISO-4217 code")
	}
	if opts.Period != "" && !periodPattern.MatchString(opts.Period) {
		return listBillsOptions{}, apiError(errs.InvalidArgument, "invalid-request", "period must use YYYY-MM calendar-month format")
	}

	limit, ok := req.Limit.Get()
	if !ok {
		opts.Limit = defaultListBillsLimit
		return opts, nil
	}

	if limit <= 0 {
		return listBillsOptions{}, apiError(errs.InvalidArgument, "invalid-request", "limit must be a positive integer")
	}
	if limit > maxListBillsLimit {
		limit = maxListBillsLimit
	}
	opts.Limit = limit
	return opts, nil
}

func openTemporalError(err error) error {
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return apiError(errs.AlreadyExists, "bill-already-open", "bill workflow already exists")
	}
	rlog.Error("open bill: temporal update failed", "problemType", "open-unavailable", "err", err)
	return apiError(errs.Unavailable, "open-unavailable", "open workflow did not complete; retry after a short delay")
}

func addLineItemTemporalError(ctx context.Context, billID string, err error) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case "CurrencyMismatch":
			return apiError(errs.InvalidArgument, "currency-mismatch", "line item currency must match bill currency")
		case "BillNotOpen":
			return apiError(errs.Aborted, "bill-not-open", "bill is not accepting line items")
		}
	}

	var notFoundErr *serviceerror.NotFound
	if errors.As(err, &notFoundErr) {
		return addLineItemNotFoundFallback(ctx, billID, err)
	}

	rlog.Error("add line item: temporal update failed", "problemType", "add-line-item-unavailable", "err", err)
	return apiError(errs.Unavailable, "add-line-item-unavailable", "add-line-item update did not complete; retry after a short delay")
}

func addLineItemNotFoundFallback(ctx context.Context, billID string, temporalErr error) error {
	resource, err := readBillResource(ctx, billID)
	if errors.Is(err, sqldb.ErrNoRows) {
		return apiError(errs.NotFound, "no-bill", "bill does not exist")
	}
	if err != nil {
		rlog.Error("add line item: ledger fallback failed", "billID", billID, "problemType", "add-line-item-unavailable", "err", err)
		return apiError(errs.Unavailable, "add-line-item-unavailable", "add-line-item update did not complete; retry after a short delay")
	}
	if resource.Status == CLOSED.String() {
		return apiError(errs.Aborted, "bill-closed", "bill is closed and cannot accept line items")
	}

	rlog.Error("add line item: workflow missing for ledger bill", "billID", billID, "status", resource.Status, "problemType", "add-line-item-unavailable", "err", temporalErr)
	return apiError(errs.Unavailable, "add-line-item-unavailable", "add-line-item update did not complete; retry after a short delay")
}

func closeTemporalError(ctx context.Context, id string, err error) (*InvoiceResource, error) {
	var notFoundErr *serviceerror.NotFound
	if errors.As(err, &notFoundErr) {
		invoice, readErr := readClosedInvoiceResource(ctx, id)
		if readErr == nil {
			return invoice, nil
		}
		if errors.Is(readErr, sqldb.ErrNoRows) {
			if _, billErr := readBillResource(ctx, id); errors.Is(billErr, sqldb.ErrNoRows) {
				return nil, apiError(errs.NotFound, "bill-not-found", "bill does not exist")
			}
		}
		rlog.Error("close bill: not-found fallback did not find closed invoice", "billID", id, "problemType", "close-unavailable", "err", readErr)
		return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
	}

	rlog.Error("close bill: temporal close failed", "billID", id, "problemType", "close-unavailable", "err", err)
	return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
}

func readBillError(id string, err error) error {
	if errors.Is(err, sqldb.ErrNoRows) {
		return apiError(errs.NotFound, "bill-not-found", "bill does not exist")
	}
	rlog.Error("get bill: read failed", "billID", id, "problemType", "read-unavailable", "err", err)
	return apiError(errs.Unavailable, "read-unavailable", "bill could not be read; retry after a short delay")
}

func closedInvoice(ctx context.Context, id string) (*InvoiceResource, error) {
	invoice, err := readClosedInvoiceResource(ctx, id)
	if err == nil {
		return invoice, nil
	}
	if errors.Is(err, sqldb.ErrNoRows) {
		rlog.Error("close bill: read closed invoice failed", "billID", id, "problemType", "bill-not-found", "err", err)
		return nil, apiError(errs.NotFound, "bill-not-found", "bill does not exist")
	}
	rlog.Error("close bill: read closed invoice failed", "billID", id, "problemType", "close-unavailable", "err", err)
	return nil, apiError(errs.Unavailable, "close-unavailable", "close did not complete; retry after a short delay")
}

func getBillResponseFromBill(resource *BillResource) *GetBillResponse {
	return &GetBillResponse{
		BillID:           resource.BillID,
		ClientID:         resource.ClientID,
		Currency:         resource.Currency,
		Period:           resource.Period,
		Status:           resource.Status,
		TotalMinorAmount: resource.TotalMinorAmount,
		ItemCount:        resource.ItemCount,
		OpenedAt:         resource.OpenedAt,
		ClosedAt:         resource.ClosedAt,
	}
}

func getBillResponseFromInvoice(resource *InvoiceResource) *GetBillResponse {
	return &GetBillResponse{
		BillID:           resource.BillID,
		ClientID:         resource.ClientID,
		Currency:         resource.Currency,
		Period:           resource.Period,
		Status:           resource.Status,
		TotalMinorAmount: resource.TotalMinorAmount,
		ItemCount:        resource.ItemCount,
		OpenedAt:         resource.OpenedAt,
		ClosedAt:         resource.ClosedAt,
		LineItems:        resource.LineItems,
	}
}

func invoiceResourceFromBill(resource *BillResource, items []LineItemResource) *InvoiceResource {
	return &InvoiceResource{
		BillID:           resource.BillID,
		ClientID:         resource.ClientID,
		Currency:         resource.Currency,
		Period:           resource.Period,
		Status:           resource.Status,
		TotalMinorAmount: resource.TotalMinorAmount,
		ItemCount:        resource.ItemCount,
		OpenedAt:         resource.OpenedAt,
		ClosedAt:         resource.ClosedAt,
		LineItems:        items,
	}
}

func apiError(code errs.ErrCode, problemType, message string, metaPairs ...interface{}) error {
	return errs.B().
		Code(code).
		Msg(message).
		Details(APIErrorDetails{Type: problemType}).
		Meta(metaPairs...).
		Err()
}
