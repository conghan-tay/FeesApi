package fees

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"encore.dev"
	"encore.dev/rlog"
	"encore.dev/storage/sqldb"
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
	BillResource
	LineItems []LineItemResource `json:"lineItems"`
}

type LineItemResource struct {
	Reference   string    `json:"reference"`
	MinorAmount string    `json:"minorAmount"`
	Currency    string    `json:"currency"`
	FeeType     string    `json:"feeType"`
	Description string    `json:"description"`
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

type AddLineItemResponse struct {
	Reference string `json:"reference"`
	Applied   bool   `json:"applied"`
}

type problemResponse struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

//encore:api public raw method=POST path=/v1/bills
func (s *Service) OpenBill(w http.ResponseWriter, req *http.Request) {
	var input OpenBillRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", "request body must be valid JSON")
		return
	}

	billInput, validationErr := validateOpenBillRequest(input)
	if validationErr != "" {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", validationErr)
		return
	}
	if !time.Now().UTC().Before(resolvePeriodEnd(billInput.Period)) {
		writeProblem(w, req, http.StatusUnprocessableEntity, "period-elapsed", "Period elapsed", "billing period has already elapsed")
		return
	}
	if s == nil || s.temporalClient == nil {
		writeProblem(w, req, http.StatusServiceUnavailable, "open-unavailable", "Open unavailable", "temporal client is not available")
		return
	}

	id := billID(billInput.ClientID, billInput.Currency, billInput.Period)
	startOp := s.temporalClient.NewWithStartWorkflowOperation(
		client.StartWorkflowOptions{
			ID:                       id,
			TaskQueue:                s.temporalConfig.TaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		BillWorkflow,
		billInput,
	)
	handle, err := s.temporalClient.UpdateWithStartWorkflow(req.Context(), client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   UpdateAwaitOpen,
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		writeOpenTemporalError(w, req, err)
		return
	}

	var view BillView
	if err := handle.Get(req.Context(), &view); err != nil {
		writeOpenTemporalError(w, req, err)
		return
	}

	resource, err := readOpenedBillResource(req.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		problemType := "internal-error"
		title := "Internal error"
		detail := "opened bill could not be read"
		if errors.Is(err, sqldb.ErrNoRows) {
			status = http.StatusServiceUnavailable
			problemType = "open-unavailable"
			title = "Open unavailable"
			detail = "opened bill was not available after open completed; retry after a short delay"
		}
		rlog.Error("open bill: read opened bill failed", "billID", id, "problemType", problemType, "err", err)
		writeProblem(w, req, status, problemType, title, detail)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/v1/bills/"+id)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resource)
}

//encore:api public raw method=POST path=/v1/bills/:billId/line-items
func (s *Service) AddLineItem(w http.ResponseWriter, req *http.Request) {
	billID := currentBillID(req)
	if billID == "" {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", "billId is required")
		return
	}

	var input AddLineItemRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", "request body must be valid JSON")
		return
	}

	lineItem, validationErr := validateAddLineItemRequest(input)
	if validationErr != "" {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", validationErr)
		return
	}
	if s == nil || s.temporalClient == nil {
		writeProblem(w, req, http.StatusServiceUnavailable, "add-line-item-unavailable", "Add line item unavailable", "add-line-item update did not complete; retry after a short delay")
		return
	}

	handle, err := s.temporalClient.UpdateWorkflow(req.Context(), client.UpdateWorkflowOptions{
		WorkflowID:   billID,
		UpdateName:   UpdateAddLineItem,
		Args:         []interface{}{lineItem},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		writeAddLineItemTemporalError(w, req, billID, err)
		return
	}

	var result LineItemResult
	if err := handle.Get(req.Context(), &result); err != nil {
		writeAddLineItemTemporalError(w, req, billID, err)
		return
	}

	status := http.StatusOK
	if result.Applied {
		status = http.StatusCreated
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(AddLineItemResponse{
		Reference: result.Reference,
		Applied:   result.Applied,
	})
}

//encore:api public raw method=POST path=/v1/bills/:billId/close
func (s *Service) CloseBill(w http.ResponseWriter, req *http.Request) {
	id := currentBillID(req)
	if id == "" {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", "billId is required")
		return
	}

	input, ok := decodeCloseBillRequest(w, req)
	if !ok {
		return
	}

	resource, err := readBillResource(req.Context(), id)
	if err != nil {
		if errors.Is(err, sqldb.ErrNoRows) {
			writeProblem(w, req, http.StatusNotFound, "bill-not-found", "Bill not found", "bill does not exist")
			return
		}
		rlog.Error("close bill: read bill failed", "billID", id, "problemType", "close-unavailable", "err", err)
		writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
		return
	}

	if resource.Status == CLOSED.String() {
		writeClosedInvoice(w, req, id)
		return
	}
	if s == nil || s.temporalClient == nil {
		writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
		return
	}

	if err := s.temporalClient.SignalWorkflow(req.Context(), id, "", SignalCloseBill, CloseSignal{Reason: input.Reason}); err != nil {
		writeCloseTemporalError(w, req, id, err)
		return
	}
	if err := s.temporalClient.GetWorkflow(req.Context(), id, "").Get(req.Context(), nil); err != nil {
		writeCloseTemporalError(w, req, id, err)
		return
	}

	sealed, err := readBillResource(req.Context(), id)
	if err != nil {
		rlog.Error("close bill: read sealed bill failed", "billID", id, "problemType", "close-unavailable", "err", err)
		writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
		return
	}
	if sealed.Status != CLOSED.String() {
		rlog.Error("close bill: workflow completed before ledger was sealed", "billID", id, "status", sealed.Status)
		writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
		return
	}
	writeClosedInvoice(w, req, id)
}

//encore:api public method=GET path=/v1/bills
func (s *Service) ListBills(ctx context.Context) (*ListBillsResponse, error) {
	return &ListBillsResponse{
		Bills:      []BillResource{},
		NextCursor: "",
		HasMore:    false,
	}, nil
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

func decodeCloseBillRequest(w http.ResponseWriter, req *http.Request) (CloseBillRequest, bool) {
	var input CloseBillRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil && !errors.Is(err, io.EOF) {
		writeProblem(w, req, http.StatusBadRequest, "invalid-request", "Invalid request", "request body must be valid JSON")
		return CloseBillRequest{}, false
	}
	return input, true
}

func writeOpenTemporalError(w http.ResponseWriter, req *http.Request, err error) {
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		writeProblem(w, req, http.StatusConflict, "bill-already-open", "Bill already open", "bill workflow already exists")
		return
	}
	rlog.Error("open bill: temporal update failed", "problemType", "open-unavailable", "err", err)
	writeProblem(w, req, http.StatusServiceUnavailable, "open-unavailable", "Open unavailable", "open workflow did not complete; retry after a short delay")
}

func writeAddLineItemTemporalError(w http.ResponseWriter, req *http.Request, billID string, err error) {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case "CurrencyMismatch":
			writeProblem(w, req, http.StatusBadRequest, "currency-mismatch", "Currency mismatch", "line item currency must match bill currency")
			return
		case "BillNotOpen":
			writeProblem(w, req, http.StatusConflict, "bill-not-open", "Bill not open", "bill is not accepting line items")
			return
		}
	}

	var notFoundErr *serviceerror.NotFound
	if errors.As(err, &notFoundErr) {
		writeAddLineItemNotFoundFallback(w, req, billID, err)
		return
	}

	rlog.Error("add line item: temporal update failed", "problemType", "add-line-item-unavailable", "err", err)
	writeProblem(w, req, http.StatusServiceUnavailable, "add-line-item-unavailable", "Add line item unavailable", "add-line-item update did not complete; retry after a short delay")
}

func writeAddLineItemNotFoundFallback(w http.ResponseWriter, req *http.Request, billID string, temporalErr error) {
	resource, err := readBillResource(req.Context(), billID)
	if errors.Is(err, sqldb.ErrNoRows) {
		writeProblem(w, req, http.StatusNotFound, "no-bill", "No bill", "bill does not exist")
		return
	}
	if err != nil {
		rlog.Error("add line item: ledger fallback failed", "billID", billID, "problemType", "add-line-item-unavailable", "err", err)
		writeProblem(w, req, http.StatusServiceUnavailable, "add-line-item-unavailable", "Add line item unavailable", "add-line-item update did not complete; retry after a short delay")
		return
	}
	if resource.Status == CLOSED.String() {
		writeProblem(w, req, http.StatusConflict, "bill-closed", "Bill closed", "bill is closed and cannot accept line items")
		return
	}

	rlog.Error("add line item: workflow missing for ledger bill", "billID", billID, "status", resource.Status, "problemType", "add-line-item-unavailable", "err", temporalErr)
	writeProblem(w, req, http.StatusServiceUnavailable, "add-line-item-unavailable", "Add line item unavailable", "add-line-item update did not complete; retry after a short delay")
}

func writeCloseTemporalError(w http.ResponseWriter, req *http.Request, id string, err error) {
	var notFoundErr *serviceerror.NotFound
	if errors.As(err, &notFoundErr) {
		invoice, readErr := readClosedInvoiceResource(req.Context(), id)
		if readErr == nil {
			writeJSON(w, http.StatusOK, invoice)
			return
		}
		if errors.Is(readErr, sqldb.ErrNoRows) {
			if _, billErr := readBillResource(req.Context(), id); errors.Is(billErr, sqldb.ErrNoRows) {
				writeProblem(w, req, http.StatusNotFound, "bill-not-found", "Bill not found", "bill does not exist")
				return
			}
		}
		rlog.Error("close bill: not-found fallback did not find closed invoice", "billID", id, "problemType", "close-unavailable", "err", readErr)
		writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
		return
	}

	rlog.Error("close bill: temporal close failed", "billID", id, "problemType", "close-unavailable", "err", err)
	writeProblem(w, req, http.StatusServiceUnavailable, "close-unavailable", "Close unavailable", "close did not complete; retry after a short delay")
}

func writeProblem(w http.ResponseWriter, req *http.Request, status int, problemType, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemResponse{
		Type:     problemType,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: req.URL.Path,
	})
}

func writeClosedInvoice(w http.ResponseWriter, req *http.Request, id string) {
	invoice, err := readClosedInvoiceResource(req.Context(), id)
	if err != nil {
		status := http.StatusServiceUnavailable
		problemType := "close-unavailable"
		title := "Close unavailable"
		detail := "close did not complete; retry after a short delay"
		if errors.Is(err, sqldb.ErrNoRows) {
			status = http.StatusNotFound
			problemType = "bill-not-found"
			title = "Bill not found"
			detail = "bill does not exist"
		}
		rlog.Error("close bill: read closed invoice failed", "billID", id, "problemType", problemType, "err", err)
		writeProblem(w, req, status, problemType, title, detail)
		return
	}
	writeJSON(w, http.StatusOK, invoice)
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func currentBillID(req *http.Request) string {
	if billID := currentEncoreBillID(); billID != "" {
		return billID
	}

	// Direct handler tests do not run through Encore's router, so path params are
	// unavailable. Keep production extraction above and parse only as a fallback.
	path := strings.Trim(req.URL.Path, "/")
	const prefix = "v1/bills/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	for _, suffix := range []string{"/line-items", "/close"} {
		if strings.HasSuffix(rest, suffix) {
			return strings.TrimSuffix(rest, suffix)
		}
	}
	return ""
}

func currentEncoreBillID() (billID string) {
	defer func() {
		if recover() != nil {
			billID = ""
		}
	}()

	return encore.CurrentRequest().PathParams.Get("billId")
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

func readClosedInvoiceResource(ctx context.Context, id string) (*InvoiceResource, error) {
	resource, err := readBillResource(ctx, id)
	if err != nil {
		return nil, err
	}
	if resource.Status != CLOSED.String() {
		return nil, sqldb.ErrNoRows
	}

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

	return &InvoiceResource{
		BillResource: *resource,
		LineItems:    items,
	}, nil
}
