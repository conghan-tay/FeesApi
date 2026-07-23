package fees

import (
	"context"
	"encoding/json"
	"errors"
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
		writeAddLineItemTemporalError(w, req, err)
		return
	}

	var result LineItemResult
	if err := handle.Get(req.Context(), &result); err != nil {
		writeAddLineItemTemporalError(w, req, err)
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

func writeOpenTemporalError(w http.ResponseWriter, req *http.Request, err error) {
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		writeProblem(w, req, http.StatusConflict, "bill-already-open", "Bill already open", "bill workflow already exists")
		return
	}
	rlog.Error("open bill: temporal update failed", "problemType", "open-unavailable", "err", err)
	writeProblem(w, req, http.StatusServiceUnavailable, "open-unavailable", "Open unavailable", "open workflow did not complete; retry after a short delay")
}

func writeAddLineItemTemporalError(w http.ResponseWriter, req *http.Request, err error) {
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
		writeProblem(w, req, http.StatusNotFound, "no-open-bill", "No open bill", "no open bill workflow exists for billId")
		return
	}

	rlog.Error("add line item: temporal update failed", "problemType", "add-line-item-unavailable", "err", err)
	writeProblem(w, req, http.StatusServiceUnavailable, "add-line-item-unavailable", "Add line item unavailable", "add-line-item update did not complete; retry after a short delay")
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

func currentBillID(req *http.Request) string {
	if billID := currentEncoreBillID(); billID != "" {
		return billID
	}

	// Direct handler tests do not run through Encore's router, so path params are
	// unavailable. Keep production extraction above and parse only as a fallback.
	path := strings.Trim(req.URL.Path, "/")
	const prefix = "v1/bills/"
	const suffix = "/line-items"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
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
