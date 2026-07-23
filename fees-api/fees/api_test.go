package fees

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func TestListBillsReturnsEmptyScaffoldResponse(t *testing.T) {
	resp, err := (&Service{}).ListBills(context.Background())
	if err != nil {
		t.Fatalf("ListBills returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("ListBills returned nil response")
	}
	if len(resp.Bills) != 0 {
		t.Fatalf("expected no bills, got %d", len(resp.Bills))
	}
	if resp.NextCursor != "" {
		t.Fatalf("expected empty next cursor, got %q", resp.NextCursor)
	}
	if resp.HasMore {
		t.Fatal("expected hasMore=false")
	}
}

func TestBillResourceEncodesClosedAtNull(t *testing.T) {
	openedAt := time.Date(2026, 7, 3, 14, 21, 0, 0, time.UTC)
	resource := BillResource{
		BillID:           "bill-acme-USD-2026-07",
		ClientID:         "acme",
		Currency:         "USD",
		Period:           "2026-07",
		Status:           "OPEN",
		TotalMinorAmount: "0",
		OpenedAt:         openedAt,
	}

	encoded, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("marshal BillResource: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal BillResource JSON: %v", err)
	}
	if _, ok := body["closedAt"]; !ok {
		t.Fatal("expected closedAt key to be present")
	}
	if _, ok := body["totalMinorAmount"]; !ok {
		t.Fatal("expected totalMinorAmount key to be present")
	}
	if _, ok := body["total_minor_amount"]; ok {
		t.Fatal("did not expect legacy total_minor_amount key")
	}
	if body["closedAt"] != nil {
		t.Fatalf("expected closedAt=null, got %#v", body["closedAt"])
	}
	if body["openedAt"] != "2026-07-03T14:21:00Z" {
		t.Fatalf("openedAt = %#v, want RFC3339 timestamp", body["openedAt"])
	}
}

func TestOpenBillSuccessStartsWorkflowAndReturnsCreatedResource(t *testing.T) {
	ctx := context.Background()
	runID := time.Now().Format("150405.000000000")
	reqBody := OpenBillRequest{
		ClientID: "api-open-" + strings.ReplaceAll(runID, ".", "-"),
		Currency: "USD",
		Period:   "2099-01",
	}
	expectedBillID := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, expectedBillID)
	seedActivityBill(t, ctx, expectedBillID, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", nil)

	temporalClient := &openTemporalClient{
		handle: &openUpdateHandle{
			view: BillView{
				ClientID: reqBody.ClientID,
				Currency: reqBody.Currency,
				Period:   reqBody.Period,
				Status:   "OPEN",
			},
		},
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, reqBody)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d. Body: %s", resp.Code, http.StatusCreated, resp.Body.String())
	}
	if got := resp.Header().Get("Location"); got != "/v1/bills/"+expectedBillID {
		t.Fatalf("Location = %q, want /v1/bills/%s", got, expectedBillID)
	}
	var body BillResource
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body.BillID != expectedBillID || body.ClientID != reqBody.ClientID || body.Currency != reqBody.Currency || body.Period != reqBody.Period {
		t.Fatalf("BillResource identity = %#v, want bill %s", body, expectedBillID)
	}
	if body.Status != "OPEN" {
		t.Fatalf("status = %q, want OPEN", body.Status)
	}
	if body.TotalMinorAmount != "0" {
		t.Fatalf("totalMinorAmount = %q, want 0", body.TotalMinorAmount)
	}
	if body.ItemCount != 0 {
		t.Fatalf("itemCount = %d, want 0", body.ItemCount)
	}
	if body.OpenedAt.IsZero() {
		t.Fatal("openedAt is zero, want persisted timestamp")
	}
	if body.ClosedAt != nil {
		t.Fatalf("closedAt = %v, want nil", body.ClosedAt)
	}

	if temporalClient.newStartCount != 1 {
		t.Fatalf("NewWithStartWorkflowOperation calls = %d, want 1", temporalClient.newStartCount)
	}
	if temporalClient.updateWithStartCount != 1 {
		t.Fatalf("UpdateWithStartWorkflow calls = %d, want 1", temporalClient.updateWithStartCount)
	}
	if temporalClient.startOptions.ID != expectedBillID {
		t.Fatalf("workflow ID = %q, want %q", temporalClient.startOptions.ID, expectedBillID)
	}
	if temporalClient.startOptions.TaskQueue != temporalTaskQueue {
		t.Fatalf("task queue = %q, want %q", temporalClient.startOptions.TaskQueue, temporalTaskQueue)
	}
	if temporalClient.startOptions.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
		t.Fatalf("conflict policy = %s, want FAIL", temporalClient.startOptions.WorkflowIDConflictPolicy)
	}
	if temporalClient.startOptions.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Fatalf("reuse policy = %s, want REJECT_DUPLICATE", temporalClient.startOptions.WorkflowIDReusePolicy)
	}
	if temporalClient.updateOptions.UpdateName != UpdateAwaitOpen {
		t.Fatalf("update name = %q, want %q", temporalClient.updateOptions.UpdateName, UpdateAwaitOpen)
	}
	if temporalClient.updateOptions.WaitForStage != client.WorkflowUpdateStageCompleted {
		t.Fatalf("wait stage = %v, want completed", temporalClient.updateOptions.WaitForStage)
	}
}

func TestOpenBillValidationFailuresDoNotCallTemporal(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed JSON", body: `{"clientId":`},
		{name: "missing client", body: `{"currency":"USD","period":"2099-01"}`},
		{name: "bad currency", body: `{"clientId":"acme","currency":"usd","period":"2099-01"}`},
		{name: "bad period", body: `{"clientId":"acme","currency":"USD","period":"2099-13"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{}
			svc := &Service{
				temporalClient: temporalClient,
				temporalConfig: defaultTemporalConfig(),
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/bills", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			svc.OpenBill(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
			}
			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if temporalClient.newStartCount != 0 || temporalClient.updateWithStartCount != 0 {
				t.Fatalf("Temporal was called for invalid request: new=%d update=%d", temporalClient.newStartCount, temporalClient.updateWithStartCount)
			}
		})
	}
}

func TestOpenBillElapsedPeriodReturns422AndDoesNotCallTemporal(t *testing.T) {
	temporalClient := &openTemporalClient{}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-elapsed",
		Currency: "USD",
		Period:   "2000-01",
	})

	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "period-elapsed", http.StatusUnprocessableEntity)
	if temporalClient.newStartCount != 0 || temporalClient.updateWithStartCount != 0 {
		t.Fatalf("Temporal was called for elapsed period: new=%d update=%d", temporalClient.newStartCount, temporalClient.updateWithStartCount)
	}
}

func TestOpenBillAlreadyStartedReturns409(t *testing.T) {
	temporalClient := &openTemporalClient{
		updateErr: serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", ""),
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-duplicate",
		Currency: "USD",
		Period:   "2099-01",
	})

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "bill-already-open", http.StatusConflict)
}

func TestOpenBillRejectsClosedWorkflowReuseAsDuplicate(t *testing.T) {
	ctx := context.Background()
	reqBody := OpenBillRequest{
		ClientID: "api-closed-duplicate",
		Currency: "USD",
		Period:   "2099-01",
	}
	expectedBillID := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, expectedBillID)
	seedActivityBill(t, ctx, expectedBillID, reqBody.ClientID, reqBody.Currency, reqBody.Period, "CLOSED", &closedAt)

	temporalClient := &openTemporalClient{
		updateErr: serviceerror.NewWorkflowExecutionAlreadyStarted("closed workflow already exists", "", ""),
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, reqBody)

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "bill-already-open", http.StatusConflict)
	if strings.Contains(resp.Body.String(), "CLOSED") {
		t.Fatalf("problem response leaked closed bill body: %s", resp.Body.String())
	}
}

func TestOpenBillUpdateFailureReturns503(t *testing.T) {
	rawErr := "dial tcp 10.0.4.23:7233: connect: connection refused"
	temporalClient := &openTemporalClient{
		updateErr: errors.New(rawErr),
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-unavailable",
		Currency: "USD",
		Period:   "2099-01",
	})

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "open-unavailable", http.StatusServiceUnavailable)
	assertProblemDetail(t, resp, "open workflow did not complete; retry after a short delay")
	assertProblemDoesNotContain(t, resp, rawErr)
}

func TestOpenBillHandleGetFailureReturns503(t *testing.T) {
	rawErr := "persist failed: INSERT INTO bills host=10.0.4.23"
	temporalClient := &openTemporalClient{
		handle: &openUpdateHandle{err: errors.New(rawErr)},
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-persist-failed",
		Currency: "USD",
		Period:   "2099-01",
	})

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "open-unavailable", http.StatusServiceUnavailable)
	assertProblemDetail(t, resp, "open workflow did not complete; retry after a short delay")
	assertProblemDoesNotContain(t, resp, rawErr)
}

func TestOpenBillMissingLedgerRowAfterUpdateReturnsRedacted503(t *testing.T) {
	rawBillID := "bill-api-missing-ledger-USD-2099-01"
	temporalClient := &openTemporalClient{
		handle: &openUpdateHandle{
			view: BillView{
				ClientID: "api-missing-ledger",
				Currency: "USD",
				Period:   "2099-01",
				Status:   "OPEN",
			},
		},
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-missing-ledger",
		Currency: "USD",
		Period:   "2099-01",
	})

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "open-unavailable", http.StatusServiceUnavailable)
	assertProblemDetail(t, resp, "opened bill was not available after open completed; retry after a short delay")
	assertProblemDoesNotContain(t, resp, rawBillID)
}

func TestAddLineItemFreshReturns201AndCallsWorkflowUpdate(t *testing.T) {
	reqBody := AddLineItemRequest{
		Reference:   "pay-svc-evt-98213",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}
	temporalClient := &openTemporalClient{
		workflowHandle: &openUpdateHandle{
			lineItemResult: LineItemResult{Reference: reqBody.Reference, Applied: true},
		},
	}
	svc := &Service{temporalClient: temporalClient}
	billID := "bill-acme-USD-2099-01"

	resp := performAddLineItem(t, svc, billID, reqBody)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201. Body: %s", resp.Code, resp.Body.String())
	}
	var body AddLineItemResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body != (AddLineItemResponse{Reference: reqBody.Reference, Applied: true}) {
		t.Fatalf("response = %#v, want fresh result", body)
	}
	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw response body: %v", err)
	}
	if _, ok := raw["reference"]; !ok {
		t.Fatal("expected lowercase JSON key reference")
	}
	if _, ok := raw["applied"]; !ok {
		t.Fatal("expected lowercase JSON key applied")
	}
	if _, ok := raw["Reference"]; ok {
		t.Fatal("did not expect exported Go field name Reference in JSON")
	}
	assertAddLineItemUpdateOptions(t, temporalClient, billID, LineItem{
		Reference:   reqBody.Reference,
		AmountMinor: 1500,
		Currency:    reqBody.Currency,
		FeeType:     reqBody.FeeType,
		Description: reqBody.Description,
	})
}

func TestAddLineItemDuplicateReturns200(t *testing.T) {
	reqBody := AddLineItemRequest{
		Reference:   "pay-svc-evt-duplicate",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}
	temporalClient := &openTemporalClient{
		workflowHandle: &openUpdateHandle{
			lineItemResult: LineItemResult{Reference: reqBody.Reference, Applied: false},
		},
	}
	svc := &Service{temporalClient: temporalClient}

	resp := performAddLineItem(t, svc, "bill-acme-USD-2099-01", reqBody)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	var body AddLineItemResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body != (AddLineItemResponse{Reference: reqBody.Reference, Applied: false}) {
		t.Fatalf("response = %#v, want duplicate result", body)
	}
}

func TestAddLineItemValidationFailuresDoNotCallTemporal(t *testing.T) {
	tests := []struct {
		name   string
		billID string
		body   string
	}{
		{name: "missing bill ID", billID: "", body: `{"reference":"ref","minorAmount":"1","currency":"USD","feeType":"wire"}`},
		{name: "malformed JSON", billID: "bill-acme-USD-2099-01", body: `{"reference":`},
		{name: "unknown field", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","minorAmount":"1","currency":"USD","feeType":"wire","unexpected":true}`},
		{name: "missing reference", billID: "bill-acme-USD-2099-01", body: `{"minorAmount":"1","currency":"USD","feeType":"wire"}`},
		{name: "missing minor amount", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","currency":"USD","feeType":"wire"}`},
		{name: "invalid minor amount", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","minorAmount":"1.25","currency":"USD","feeType":"wire"}`},
		{name: "overflow minor amount", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","minorAmount":"9223372036854775808","currency":"USD","feeType":"wire"}`},
		{name: "lowercase currency", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","minorAmount":"1","currency":"usd","feeType":"wire"}`},
		{name: "missing fee type", billID: "bill-acme-USD-2099-01", body: `{"reference":"ref","minorAmount":"1","currency":"USD"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{}
			svc := &Service{temporalClient: temporalClient}

			resp := performAddLineItemRaw(t, svc, tt.billID, tt.body)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
			}
			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if temporalClient.updateWorkflowCount != 0 {
				t.Fatalf("Temporal UpdateWorkflow calls = %d, want 0", temporalClient.updateWorkflowCount)
			}
		})
	}
}

func TestAddLineItemTemporalErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		directErr  error
		handleErr  error
		wantStatus int
		wantType   string
	}{
		{
			name:       "direct not found",
			directErr:  serviceerror.NewNotFound("workflow not found"),
			wantStatus: http.StatusNotFound,
			wantType:   "no-open-bill",
		},
		{
			name:       "handle not found",
			handleErr:  serviceerror.NewNotFound("workflow not found"),
			wantStatus: http.StatusNotFound,
			wantType:   "no-open-bill",
		},
		{
			name:       "currency mismatch",
			handleErr:  temporal.NewApplicationError("currency mismatch", "CurrencyMismatch"),
			wantStatus: http.StatusBadRequest,
			wantType:   "currency-mismatch",
		},
		{
			name:       "bill not open",
			handleErr:  temporal.NewApplicationError("bill not open", "BillNotOpen"),
			wantStatus: http.StatusConflict,
			wantType:   "bill-not-open",
		},
		{
			name:       "direct generic error",
			directErr:  errors.New("dial tcp 10.0.4.23:7233: connect: connection refused"),
			wantStatus: http.StatusServiceUnavailable,
			wantType:   "add-line-item-unavailable",
		},
		{
			name:       "handle generic error",
			handleErr:  errors.New("persist failed: host=10.0.4.23"),
			wantStatus: http.StatusServiceUnavailable,
			wantType:   "add-line-item-unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{
				workflowErr: tt.directErr,
				workflowHandle: &openUpdateHandle{
					err: tt.handleErr,
				},
			}
			svc := &Service{temporalClient: temporalClient}

			resp := performAddLineItem(t, svc, "bill-acme-USD-2099-01", AddLineItemRequest{
				Reference:   "ref-error",
				MinorAmount: "1500",
				Currency:    "USD",
				FeeType:     "wire_transfer",
				Description: "Outbound USD wire",
			})

			if resp.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d. Body: %s", resp.Code, tt.wantStatus, resp.Body.String())
			}
			assertProblem(t, resp, tt.wantType, tt.wantStatus)
			assertProblemDoesNotContain(t, resp, "10.0.4.23")
		})
	}
}

func TestAddLineItemNilTemporalClientReturns503(t *testing.T) {
	resp := performAddLineItem(t, &Service{}, "bill-acme-USD-2099-01", AddLineItemRequest{
		Reference:   "ref-unavailable",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
	})

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "add-line-item-unavailable", http.StatusServiceUnavailable)
}

func TestCloseBillOpenBillSignalsWorkflowAndReturnsInvoice(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-open", "USD", "2099-01", "OPEN", nil)
	seedAPILineItem(t, ctx, billID, "ref-close-001", 1500, "USD", "wire_transfer", "Outbound wire")
	seedAPILineItem(t, ctx, billID, "ref-close-002", -250, "USD", "correction", "Fee correction")

	temporalClient := &openTemporalClient{
		run: &openWorkflowRun{
			onGet: func(ctx context.Context) error {
				_, err := db.Exec(ctx, `
					UPDATE bills
					   SET status = 'CLOSED',
					       closed_at = $2
					 WHERE bill_id = $1`,
					billID,
					time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC),
				)
				return err
			},
		},
	}
	svc := &Service{temporalClient: temporalClient}

	resp := performCloseBill(t, svc, billID, `{"reason":"explicit-test-close"}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	assertCloseTemporalCalls(t, temporalClient, billID, CloseSignal{Reason: "explicit-test-close"})
	invoice := decodeInvoiceResource(t, resp)
	if invoice.BillID != billID || invoice.ClientID != "api-close-open" || invoice.Currency != "USD" || invoice.Period != "2099-01" {
		t.Fatalf("invoice identity = %#v, want seeded bill", invoice.BillResource)
	}
	if invoice.Status != "CLOSED" {
		t.Fatalf("status = %q, want CLOSED", invoice.Status)
	}
	if invoice.TotalMinorAmount != "1250" {
		t.Fatalf("totalMinorAmount = %q, want 1250", invoice.TotalMinorAmount)
	}
	if invoice.ItemCount != 2 {
		t.Fatalf("itemCount = %d, want 2", invoice.ItemCount)
	}
	if invoice.ClosedAt == nil {
		t.Fatal("closedAt is nil, want close timestamp")
	}
	if len(invoice.LineItems) != 2 {
		t.Fatalf("lineItems length = %d, want 2", len(invoice.LineItems))
	}
	if invoice.LineItems[0].Reference != "ref-close-001" || invoice.LineItems[0].MinorAmount != "1500" {
		t.Fatalf("first line item = %#v, want ref-close-001 amount 1500", invoice.LineItems[0])
	}
	if invoice.LineItems[1].Reference != "ref-close-002" || invoice.LineItems[1].MinorAmount != "-250" {
		t.Fatalf("second line item = %#v, want ref-close-002 amount -250", invoice.LineItems[1])
	}
}

func TestCloseBillAlreadyClosedReturnsInvoiceWithoutTemporal(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-closed", "USD", "2099-01", "CLOSED", &closedAt)
	seedAPILineItem(t, ctx, billID, "ref-reclose", 2500, "USD", "monthly_account", "Monthly fee")

	temporalClient := &openTemporalClient{}
	svc := &Service{temporalClient: temporalClient}

	resp := performCloseBill(t, svc, billID, `{"reason":"already-closed"}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	if temporalClient.signalWorkflowCount != 0 || temporalClient.getWorkflowCount != 0 {
		t.Fatalf("Temporal calls = signal:%d get:%d, want none", temporalClient.signalWorkflowCount, temporalClient.getWorkflowCount)
	}
	invoice := decodeInvoiceResource(t, resp)
	if invoice.Status != "CLOSED" || invoice.TotalMinorAmount != "2500" || invoice.ItemCount != 1 {
		t.Fatalf("invoice = %#v, want closed total 2500 count 1", invoice)
	}
	if invoice.ClosedAt == nil || !invoice.ClosedAt.Equal(closedAt) {
		t.Fatalf("closedAt = %v, want unchanged %s", invoice.ClosedAt, closedAt.Format(time.RFC3339))
	}
}

func TestCloseBillZeroItemClosedBillReturnsEmptyLineItems(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-zero-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-zero", "USD", "2099-01", "CLOSED", &closedAt)

	resp := performCloseBill(t, &Service{}, billID, `{}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	invoice := decodeInvoiceResource(t, resp)
	if invoice.TotalMinorAmount != "0" || invoice.ItemCount != 0 {
		t.Fatalf("total/count = %s/%d, want 0/0", invoice.TotalMinorAmount, invoice.ItemCount)
	}
	if invoice.LineItems == nil {
		t.Fatal("lineItems is nil, want empty array")
	}
	if len(invoice.LineItems) != 0 {
		t.Fatalf("lineItems length = %d, want 0", len(invoice.LineItems))
	}
	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw invoice: %v", err)
	}
	if _, ok := raw["lineItems"]; !ok {
		t.Fatal("expected lineItems key to be present")
	}
}

func TestCloseBillValidationFailuresDoNotCallTemporal(t *testing.T) {
	tests := []struct {
		name   string
		billID string
		body   string
	}{
		{name: "missing bill ID", billID: "", body: `{}`},
		{name: "malformed JSON", billID: "bill-acme-USD-2099-01", body: `{"reason":`},
		{name: "unknown field", billID: "bill-acme-USD-2099-01", body: `{"reason":"close","unexpected":true}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{}
			resp := performCloseBill(t, &Service{temporalClient: temporalClient}, tt.billID, tt.body)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
			}
			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if temporalClient.signalWorkflowCount != 0 || temporalClient.getWorkflowCount != 0 {
				t.Fatalf("Temporal calls = signal:%d get:%d, want none", temporalClient.signalWorkflowCount, temporalClient.getWorkflowCount)
			}
		})
	}
}

func TestCloseBillMissingLedgerBillReturns404(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)

	resp := performCloseBill(t, &Service{temporalClient: &openTemporalClient{}}, billID, `{}`)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "bill-not-found", http.StatusNotFound)
}

func TestCloseBillOpenBillWithNilTemporalClientReturns503(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-nil-temporal-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-nil-temporal", "USD", "2099-01", "OPEN", nil)

	resp := performCloseBill(t, &Service{}, billID, `{}`)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "close-unavailable", http.StatusServiceUnavailable)
}

func TestCloseBillTemporalNotFoundWithClosedLedgerFallbackReturns200(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-not-found-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-not-found-closed", "USD", "2099-01", "OPEN", nil)

	temporalClient := &openTemporalClient{
		signalErr: serviceerror.NewNotFound("workflow not found"),
		beforeSignalErr: func(ctx context.Context) error {
			_, err := db.Exec(ctx, `
				UPDATE bills
				   SET status = 'CLOSED',
				       closed_at = $2
				 WHERE bill_id = $1`,
				billID,
				closedAt,
			)
			return err
		},
	}

	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	invoice := decodeInvoiceResource(t, resp)
	if invoice.Status != "CLOSED" {
		t.Fatalf("status = %q, want CLOSED", invoice.Status)
	}
}

func TestCloseBillTemporalNotFoundWithOpenLedgerReturns503(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-not-found-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-not-found-open", "USD", "2099-01", "OPEN", nil)

	temporalClient := &openTemporalClient{
		signalErr: serviceerror.NewNotFound("workflow not found"),
	}

	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "close-unavailable", http.StatusServiceUnavailable)
}

func TestCloseBillGenericTemporalErrorsReturnRedacted503(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-generic-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-generic", "USD", "2099-01", "OPEN", nil)

	rawErr := "dial tcp 10.0.4.23:7233: connect: connection refused"
	temporalClient := &openTemporalClient{
		signalErr: errors.New(rawErr),
	}

	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "close-unavailable", http.StatusServiceUnavailable)
	assertProblemDoesNotContain(t, resp, rawErr)
}

func TestCloseBillWorkflowGetErrorReturnsRedacted503(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-get-error-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-get-error", "USD", "2099-01", "OPEN", nil)

	rawErr := "persist invoice failed: host=10.0.4.23"
	temporalClient := &openTemporalClient{
		run: &openWorkflowRun{err: errors.New(rawErr)},
	}

	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "close-unavailable", http.StatusServiceUnavailable)
	assertProblemDoesNotContain(t, resp, rawErr)
}

type openTemporalClient struct {
	closeCount int

	newStartCount        int
	updateWithStartCount int
	updateWorkflowCount  int
	signalWorkflowCount  int
	getWorkflowCount     int

	startOptions          client.StartWorkflowOptions
	startWorkflow         interface{}
	startArgs             []interface{}
	updateOptions         client.UpdateWorkflowOptions
	workflowUpdateOptions client.UpdateWorkflowOptions
	signalWorkflowID      string
	signalRunID           string
	signalName            string
	signalArg             interface{}
	getWorkflowID         string
	getRunID              string

	handle          client.WorkflowUpdateHandle
	updateErr       error
	workflowHandle  client.WorkflowUpdateHandle
	workflowErr     error
	run             client.WorkflowRun
	signalErr       error
	beforeSignalErr func(context.Context) error
}

func (c *openTemporalClient) Close() {
	c.closeCount++
}

func (c *openTemporalClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) client.WithStartWorkflowOperation {
	c.newStartCount++
	c.startOptions = options
	c.startWorkflow = workflow
	c.startArgs = args
	return openWithStartWorkflowOperation{}
}

func (c *openTemporalClient) UpdateWithStartWorkflow(_ context.Context, options client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updateWithStartCount++
	c.updateOptions = options.UpdateOptions
	if c.updateErr != nil {
		return nil, c.updateErr
	}
	if c.handle != nil {
		return c.handle, nil
	}
	return &openUpdateHandle{}, nil
}

func (c *openTemporalClient) UpdateWorkflow(_ context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updateWorkflowCount++
	c.workflowUpdateOptions = options
	if c.workflowErr != nil {
		return nil, c.workflowErr
	}
	if c.workflowHandle != nil {
		return c.workflowHandle, nil
	}
	return &openUpdateHandle{}, nil
}

func (c *openTemporalClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	c.signalWorkflowCount++
	c.signalWorkflowID = workflowID
	c.signalRunID = runID
	c.signalName = signalName
	c.signalArg = arg
	if c.beforeSignalErr != nil {
		if err := c.beforeSignalErr(ctx); err != nil {
			return err
		}
	}
	return c.signalErr
}

func (c *openTemporalClient) GetWorkflow(_ context.Context, workflowID string, runID string) client.WorkflowRun {
	c.getWorkflowCount++
	c.getWorkflowID = workflowID
	c.getRunID = runID
	if c.run != nil {
		return c.run
	}
	return &openWorkflowRun{}
}

type openWithStartWorkflowOperation struct{}

func (openWithStartWorkflowOperation) Get(context.Context) (client.WorkflowRun, error) {
	return nil, nil
}

type openWorkflowRun struct {
	err   error
	onGet func(context.Context) error
}

func (r *openWorkflowRun) Get(ctx context.Context, _ interface{}) error {
	if r.onGet != nil {
		if err := r.onGet(ctx); err != nil {
			return err
		}
	}
	return r.err
}

func (r *openWorkflowRun) GetID() string {
	return ""
}

func (r *openWorkflowRun) GetRunID() string {
	return ""
}

func (r *openWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, _ client.WorkflowRunGetOptions) error {
	return r.Get(ctx, valuePtr)
}

type openUpdateHandle struct {
	view           BillView
	lineItemResult LineItemResult
	err            error
}

func (h *openUpdateHandle) WorkflowID() string { return "" }
func (h *openUpdateHandle) RunID() string      { return "" }
func (h *openUpdateHandle) UpdateID() string   { return "" }

func (h *openUpdateHandle) Get(_ context.Context, valuePtr interface{}) error {
	if h.err != nil {
		return h.err
	}
	if out, ok := valuePtr.(*BillView); ok {
		*out = h.view
	}
	if out, ok := valuePtr.(*LineItemResult); ok {
		*out = h.lineItemResult
	}
	return nil
}

func performOpenBill(t *testing.T, svc *Service, body OpenBillRequest) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/bills", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	svc.OpenBill(resp, req)
	return resp
}

func performAddLineItem(t *testing.T, svc *Service, billID string, body AddLineItemRequest) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return performAddLineItemRaw(t, svc, billID, string(payload))
}

func performAddLineItemRaw(t *testing.T, svc *Service, billID, body string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/v1/bills/" + billID + "/line-items"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	svc.AddLineItem(resp, req)
	return resp
}

func performCloseBill(t *testing.T, svc *Service, billID, body string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/v1/bills/" + billID + "/close"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	svc.CloseBill(resp, req)
	return resp
}

func assertAddLineItemUpdateOptions(t *testing.T, temporalClient *openTemporalClient, billID string, want LineItem) {
	t.Helper()

	if temporalClient.updateWorkflowCount != 1 {
		t.Fatalf("UpdateWorkflow calls = %d, want 1", temporalClient.updateWorkflowCount)
	}
	options := temporalClient.workflowUpdateOptions
	if options.WorkflowID != billID {
		t.Fatalf("workflow ID = %q, want %q", options.WorkflowID, billID)
	}
	if options.UpdateName != UpdateAddLineItem {
		t.Fatalf("update name = %q, want %q", options.UpdateName, UpdateAddLineItem)
	}
	if options.WaitForStage != client.WorkflowUpdateStageCompleted {
		t.Fatalf("wait stage = %v, want completed", options.WaitForStage)
	}
	if options.UpdateID != "" {
		t.Fatalf("UpdateID = %q, want empty", options.UpdateID)
	}
	if len(options.Args) != 1 {
		t.Fatalf("args length = %d, want 1", len(options.Args))
	}
	got, ok := options.Args[0].(LineItem)
	if !ok {
		t.Fatalf("arg[0] = %T, want LineItem", options.Args[0])
	}
	if got != want {
		t.Fatalf("LineItem arg = %#v, want %#v", got, want)
	}
}

func assertCloseTemporalCalls(t *testing.T, temporalClient *openTemporalClient, billID string, wantSignal CloseSignal) {
	t.Helper()

	if temporalClient.signalWorkflowCount != 1 {
		t.Fatalf("SignalWorkflow calls = %d, want 1", temporalClient.signalWorkflowCount)
	}
	if temporalClient.signalWorkflowID != billID {
		t.Fatalf("signal workflow ID = %q, want %q", temporalClient.signalWorkflowID, billID)
	}
	if temporalClient.signalRunID != "" {
		t.Fatalf("signal run ID = %q, want empty", temporalClient.signalRunID)
	}
	if temporalClient.signalName != SignalCloseBill {
		t.Fatalf("signal name = %q, want %q", temporalClient.signalName, SignalCloseBill)
	}
	gotSignal, ok := temporalClient.signalArg.(CloseSignal)
	if !ok {
		t.Fatalf("signal arg = %T, want CloseSignal", temporalClient.signalArg)
	}
	if gotSignal != wantSignal {
		t.Fatalf("signal arg = %#v, want %#v", gotSignal, wantSignal)
	}
	if temporalClient.getWorkflowCount != 1 {
		t.Fatalf("GetWorkflow calls = %d, want 1", temporalClient.getWorkflowCount)
	}
	if temporalClient.getWorkflowID != billID {
		t.Fatalf("get workflow ID = %q, want %q", temporalClient.getWorkflowID, billID)
	}
	if temporalClient.getRunID != "" {
		t.Fatalf("get run ID = %q, want empty", temporalClient.getRunID)
	}
}

func decodeInvoiceResource(t *testing.T, resp *httptest.ResponseRecorder) InvoiceResource {
	t.Helper()

	var body InvoiceResource
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode invoice response body: %v", err)
	}
	return body
}

func seedAPILineItem(t *testing.T, ctx context.Context, billID, reference string, amountMinor int64, currency, feeType, description string) {
	t.Helper()

	_, err := db.Exec(ctx, `
		INSERT INTO line_items
			(bill_id, reference, amount_minor, currency, fee_type, description)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		billID,
		reference,
		amountMinor,
		currency,
		feeType,
		description,
	)
	if err != nil {
		t.Fatalf("seed line item %s/%s: %v", billID, reference, err)
	}
}

func assertProblem(t *testing.T, resp *httptest.ResponseRecorder, wantType string, wantStatus int) {
	t.Helper()

	var problem problemResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem response: %v", err)
	}
	if problem.Type != wantType {
		t.Fatalf("problem type = %q, want %q", problem.Type, wantType)
	}
	if problem.Status != wantStatus {
		t.Fatalf("problem status = %d, want %d", problem.Status, wantStatus)
	}
}

func assertProblemDetail(t *testing.T, resp *httptest.ResponseRecorder, wantDetail string) {
	t.Helper()

	var problem problemResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem response: %v", err)
	}
	if problem.Detail != wantDetail {
		t.Fatalf("problem detail = %q, want %q", problem.Detail, wantDetail)
	}
}

func assertProblemDoesNotContain(t *testing.T, resp *httptest.ResponseRecorder, forbidden string) {
	t.Helper()

	if strings.Contains(resp.Body.String(), forbidden) {
		t.Fatalf("problem response leaked %q: %s", forbidden, resp.Body.String())
	}
}
