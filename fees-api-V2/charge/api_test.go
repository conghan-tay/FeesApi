package charge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"encore.app/internal/chargecontract"
	"encore.dev/beta/errs"
	"go.temporal.io/api/serviceerror"
)

type recordingTemporalClient struct {
	closeCount int
	signalErr  error
	signals    []recordedSignal
}

type recordedSignal struct {
	workflowID string
	runID      string
	name       string
	arg        interface{}
}

func (c *recordingTemporalClient) Close() {
	c.closeCount++
}

func (c *recordingTemporalClient) SignalWorkflow(
	_ context.Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) error {
	c.signals = append(c.signals, recordedSignal{
		workflowID: workflowID,
		runID:      runID,
		name:       signalName,
		arg:        arg,
	})
	return c.signalErr
}

func TestAddLineItemAcceptedReturns202AndSignalsWorkflow(t *testing.T) {
	temporalClient := &recordingTemporalClient{}
	svc := &Service{temporalClient: temporalClient}
	billID := "bill-acme-USD-2099-01"
	req := AddLineItemRequest{
		Reference:   "pay-svc-evt-98213",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
	}

	for call := 0; call < 2; call++ {
		resp := performAddLineItem(t, svc, billID, req)
		if resp.Code != http.StatusAccepted {
			t.Fatalf("call %d status = %d, want 202. Body: %s", call+1, resp.Code, resp.Body.String())
		}

		var body AddLineItemResponse
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response body: %v", err)
		}
		if body.Reference != req.Reference || !body.Applied {
			t.Fatalf("call %d response = %#v, want accepted response", call+1, body)
		}
	}

	if len(temporalClient.signals) != 2 {
		t.Fatalf("SignalWorkflow calls = %d, want 2", len(temporalClient.signals))
	}
	for i, signal := range temporalClient.signals {
		if signal.workflowID != billID {
			t.Fatalf("signal %d workflow ID = %q, want %q", i+1, signal.workflowID, billID)
		}
		if signal.runID != "" {
			t.Fatalf("signal %d run ID = %q, want empty", i+1, signal.runID)
		}
		if signal.name != chargecontract.SignalAddLineItem {
			t.Fatalf("signal %d name = %q, want %q", i+1, signal.name, chargecontract.SignalAddLineItem)
		}
		got, ok := signal.arg.(chargecontract.LineItem)
		if !ok {
			t.Fatalf("signal %d payload = %T, want chargecontract.LineItem", i+1, signal.arg)
		}
		want := chargecontract.LineItem{
			Reference:   req.Reference,
			AmountMinor: 1500,
			Currency:    req.Currency,
			FeeType:     req.FeeType,
			Description: req.Description,
		}
		if got != want {
			t.Fatalf("signal %d payload = %#v, want %#v", i+1, got, want)
		}
	}
}

func TestAddLineItemValidationFailuresDoNotSignalTemporal(t *testing.T) {
	tests := []struct {
		name   string
		billID string
		body   *AddLineItemRequest
	}{
		{name: "missing bill ID", billID: "", body: &AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "USD", FeeType: "wire"}},
		{name: "missing body", billID: "bill-acme-USD-2099-01", body: nil},
		{name: "missing reference", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{MinorAmount: "1", Currency: "USD", FeeType: "wire"}},
		{name: "missing minor amount", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{Reference: "ref", Currency: "USD", FeeType: "wire"}},
		{name: "invalid minor amount", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{Reference: "ref", MinorAmount: "1.25", Currency: "USD", FeeType: "wire"}},
		{name: "overflow minor amount", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{Reference: "ref", MinorAmount: "9223372036854775808", Currency: "USD", FeeType: "wire"}},
		{name: "lowercase currency", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "usd", FeeType: "wire"}},
		{name: "missing fee type", billID: "bill-acme-USD-2099-01", body: &AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "USD"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &recordingTemporalClient{}
			svc := &Service{temporalClient: temporalClient}
			resp := performAddLineItemRequest(t, svc, tt.billID, tt.body)

			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if len(temporalClient.signals) != 0 {
				t.Fatalf("SignalWorkflow calls = %d, want 0", len(temporalClient.signals))
			}
		})
	}
}

func TestAddLineItemTemporalFailuresReturnRedacted503(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "not found", err: serviceerror.NewNotFound("workflow not found")},
		{name: "generic", err: errors.New("dial tcp 10.0.4.23:7233: connect: connection refused")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &recordingTemporalClient{signalErr: tt.err}
			resp := performAddLineItem(t, &Service{temporalClient: temporalClient}, "bill-acme-USD-2099-01", validAddLineItemRequest())

			assertProblem(t, resp, "add-line-item-unavailable", http.StatusServiceUnavailable)
			if strings.Contains(resp.Body.String(), tt.err.Error()) {
				t.Fatalf("problem response leaked %q: %s", tt.err.Error(), resp.Body.String())
			}
		})
	}
}

func TestAddLineItemNilTemporalClientReturns503(t *testing.T) {
	resp := performAddLineItem(t, &Service{}, "bill-acme-USD-2099-01", validAddLineItemRequest())
	assertProblem(t, resp, "add-line-item-unavailable", http.StatusServiceUnavailable)
}

func TestPublishLineItemStatusAcceptsSupportedStatusesAndStringAmounts(t *testing.T) {
	tests := []struct {
		name   string
		status string
		amount string
	}{
		{name: "pending positive", status: LineItemStatusPending, amount: "1500"},
		{name: "finalized zero", status: LineItemStatusFinalized, amount: "0"},
		{name: "failed negative", status: LineItemStatusFailed, amount: "-500"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &recordingTemporalClient{}
			svc := &Service{temporalClient: temporalClient}
			req := validPublishLineItemStatusRequest(tt.amount, tt.status)

			resp := performPublishLineItemStatus(t, svc, &req)

			if resp.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
			}
			if resp.Body.Len() != 0 {
				t.Fatalf("response body = %q, want empty", resp.Body.String())
			}
			if len(temporalClient.signals) != 0 {
				t.Fatalf("SignalWorkflow calls = %d, want 0", len(temporalClient.signals))
			}
		})
	}
}

func TestPublishLineItemStatusValidationFailuresReturn400(t *testing.T) {
	tests := []struct {
		name string
		body *PublishLineItemStatusRequest
	}{
		{name: "missing body", body: nil},
		{name: "missing bill ID", body: &PublishLineItemStatusRequest{Reference: "ref", MinorAmount: "1500", Currency: "USD", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "missing reference", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", MinorAmount: "1500", Currency: "USD", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "missing minor amount", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", Currency: "USD", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "invalid minor amount", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1.25", Currency: "USD", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "overflow minor amount", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "9223372036854775808", Currency: "USD", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "missing currency", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "lowercase currency", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", Currency: "usd", FeeType: "wire", Status: LineItemStatusPending}},
		{name: "missing fee type", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", Currency: "USD", Status: LineItemStatusPending}},
		{name: "missing status", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", Currency: "USD", FeeType: "wire"}},
		{name: "lowercase status", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", Currency: "USD", FeeType: "wire", Status: "pending"}},
		{name: "unknown status", body: &PublishLineItemStatusRequest{BillID: "bill-acme-USD-2099-01", Reference: "ref", MinorAmount: "1500", Currency: "USD", FeeType: "wire", Status: "CANCELLED"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &recordingTemporalClient{}
			resp := performPublishLineItemStatus(t, &Service{temporalClient: temporalClient}, tt.body)

			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if len(temporalClient.signals) != 0 {
				t.Fatalf("SignalWorkflow calls = %d, want 0", len(temporalClient.signals))
			}
		})
	}
}

func TestPublishLineItemStatusMinorAmountJSONContract(t *testing.T) {
	valid := validPublishLineItemStatusRequest("1500", LineItemStatusPending)
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if !strings.Contains(string(encoded), `"minorAmount":"1500"`) {
		t.Fatalf("encoded request = %s, want string minorAmount", encoded)
	}
	if strings.Contains(string(encoded), `"minorAmount":1500`) {
		t.Fatalf("encoded request = %s, minorAmount must not be numeric", encoded)
	}

	validJSON := `{"billId":"bill-acme-USD-2099-01","reference":"ref","minorAmount":"-500","currency":"USD","feeType":"wire","description":"test","status":"PENDING"}`
	var decoded PublishLineItemStatusRequest
	if err := json.Unmarshal([]byte(validJSON), &decoded); err != nil {
		t.Fatalf("decode string minorAmount: %v", err)
	}
	if decoded.MinorAmount != "-500" {
		t.Fatalf("decoded minorAmount = %#v, want -500", decoded.MinorAmount)
	}

	for _, invalidJSON := range []string{
		`{"minorAmount":1500}`,
		`{"minorAmount":1.5}`,
		`{"minorAmount":9223372036854775808}`,
	} {
		var req PublishLineItemStatusRequest
		if err := json.Unmarshal([]byte(invalidJSON), &req); err == nil {
			t.Fatalf("json.Unmarshal(%s) returned nil error", invalidJSON)
		}
	}
}

func validAddLineItemRequest() AddLineItemRequest {
	return AddLineItemRequest{
		Reference:   "ref-unavailable",
		MinorAmount: "1500",
		Currency:    "USD",
		FeeType:     "wire_transfer",
	}
}

func validPublishLineItemStatusRequest(amount string, status string) PublishLineItemStatusRequest {
	return PublishLineItemStatusRequest{
		BillID:      "bill-acme-USD-2099-01",
		Reference:   "ref-status",
		MinorAmount: amount,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire",
		Status:      status,
	}
}

func performAddLineItem(t *testing.T, svc *Service, billID string, body AddLineItemRequest) *httptest.ResponseRecorder {
	t.Helper()
	return performAddLineItemRequest(t, svc, billID, &body)
}

func performAddLineItemRequest(t *testing.T, svc *Service, billID string, body *AddLineItemRequest) *httptest.ResponseRecorder {
	t.Helper()

	resp := httptest.NewRecorder()
	out, err := svc.AddLineItem(context.Background(), billID, body)
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(out.HTTPStatus)
	if err := json.NewEncoder(resp).Encode(struct {
		Reference string `json:"reference"`
		Applied   bool   `json:"applied"`
	}{Reference: out.Reference, Applied: out.Applied}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
	return resp
}

func performPublishLineItemStatus(t *testing.T, svc *Service, body *PublishLineItemStatusRequest) *httptest.ResponseRecorder {
	t.Helper()

	resp := httptest.NewRecorder()
	if err := svc.PublishLineItemStatus(context.Background(), body); err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	resp.WriteHeader(http.StatusOK)
	return resp
}

func assertProblem(t *testing.T, resp *httptest.ResponseRecorder, wantType string, wantStatus int) {
	t.Helper()

	var body struct {
		Details APIErrorDetails `json:"details"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode Encore error response: %v", err)
	}
	if body.Details.Type != wantType {
		t.Fatalf("error details type = %q, want %q. Body: %s", body.Details.Type, wantType, resp.Body.String())
	}
	if resp.Code != wantStatus {
		t.Fatalf("error status = %d, want %d. Body: %s", resp.Code, wantStatus, resp.Body.String())
	}
}
