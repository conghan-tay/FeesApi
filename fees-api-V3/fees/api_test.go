package fees

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/storage/sqldb"
	"encore.dev/types/option"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

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

func TestGetBillReturnsComputedTotalWithoutTemporal(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-open", "USD", "2099-01", "OPEN", nil)
	seedAPILineItem(t, ctx, billID, "ref-get-001", 1500, "USD", "wire_transfer", "Outbound wire")
	seedAPILineItem(t, ctx, billID, "ref-get-002", -250, "USD", "correction", "Fee correction")

	resp := performGetBill(t, &Service{}, billID, "")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	var body BillResource
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode bill response: %v", err)
	}
	if body.BillID != billID || body.Status != "OPEN" {
		t.Fatalf("bill identity/status = %#v, want open bill %s", body, billID)
	}
	if body.TotalMinorAmount != "1250" || body.ItemCount != 2 {
		t.Fatalf("total/count = %s/%d, want 1250/2", body.TotalMinorAmount, body.ItemCount)
	}
	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw bill response: %v", err)
	}
	if _, ok := raw["lineItems"]; ok {
		t.Fatal("lineItems present without includeLineItems=true")
	}
}

func TestGetBillWithLineItemsIncludesOrderedItems(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-items-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-items", "USD", "2099-01", "OPEN", nil)
	seedAPILineItem(t, ctx, billID, "ref-get-items-001", 300, "USD", "wire_transfer", "First")
	seedAPILineItem(t, ctx, billID, "ref-get-items-002", 700, "USD", "monthly_account", "Second")

	resp := performGetBill(t, &Service{}, billID, "true")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	invoice := decodeInvoiceResource(t, resp)
	if invoice.TotalMinorAmount != "1000" || invoice.ItemCount != 2 {
		t.Fatalf("total/count = %s/%d, want 1000/2", invoice.TotalMinorAmount, invoice.ItemCount)
	}
	if len(invoice.LineItems) != 2 {
		t.Fatalf("lineItems length = %d, want 2", len(invoice.LineItems))
	}
	if invoice.LineItems[0].Reference != "ref-get-items-001" || invoice.LineItems[1].Reference != "ref-get-items-002" {
		t.Fatalf("line item order = %#v, want insertion order", invoice.LineItems)
	}
	for _, item := range invoice.LineItems {
		if item.Status != "FINALIZED" {
			t.Fatalf("line item %s status = %q, want FINALIZED", item.Reference, item.Status)
		}
	}
}

func TestLineItemStatusesAreExposedAndOnlyFinalizedContributeToAggregates(t *testing.T) {
	ctx := context.Background()
	clientID := "api-line-statuses"
	billID := "bill-api-line-statuses-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, clientID, "USD", "2099-01", "OPEN", nil)

	items := []struct {
		reference   string
		amountMinor int64
		status      string
	}{
		{reference: "ref-status-pending", amountMinor: 100, status: "PENDING"},
		{reference: "ref-status-finalized", amountMinor: 200, status: "FINALIZED"},
		{reference: "ref-status-failed", amountMinor: -50, status: "FAILED"},
	}
	for _, item := range items {
		seedAPILineItemWithStatus(t, ctx, billID, item.reference, item.amountMinor, "USD", "status_test", item.status, item.status)
	}

	summaryResp := performGetBill(t, &Service{}, billID, "")
	if summaryResp.Code != http.StatusOK {
		t.Fatalf("summary status = %d, want 200. Body: %s", summaryResp.Code, summaryResp.Body.String())
	}
	var summary BillResource
	if err := json.Unmarshal(summaryResp.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary response: %v", err)
	}
	if summary.TotalMinorAmount != "200" || summary.ItemCount != 1 {
		t.Fatalf("summary total/count = %s/%d, want finalized-only 200/1", summary.TotalMinorAmount, summary.ItemCount)
	}

	detailResp := performGetBill(t, &Service{}, billID, "true")
	if detailResp.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want 200. Body: %s", detailResp.Code, detailResp.Body.String())
	}
	detail := decodeInvoiceResource(t, detailResp)
	if detail.TotalMinorAmount != "200" || detail.ItemCount != 1 || len(detail.LineItems) != 3 {
		t.Fatalf("detail total/count/items = %s/%d/%d, want finalized-only 200/1 with all 3 items visible", detail.TotalMinorAmount, detail.ItemCount, len(detail.LineItems))
	}
	for i, item := range items {
		if detail.LineItems[i].Reference != item.reference || detail.LineItems[i].Status != item.status {
			t.Fatalf("detail line item %d = %#v, want reference/status %s/%s", i, detail.LineItems[i], item.reference, item.status)
		}
	}

	listResp := performListBills(t, &Service{}, url.Values{"clientId": []string{clientID}})
	if listResp.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200. Body: %s", listResp.Code, listResp.Body.String())
	}
	listed := decodeListBillsResponse(t, listResp)
	if len(listed.Bills) != 1 || listed.Bills[0].TotalMinorAmount != "200" || listed.Bills[0].ItemCount != 1 {
		t.Fatalf("listed bills = %#v, want one bill with finalized-only total/count 200/1", listed.Bills)
	}
}

func TestReadBillWithLineItemsResourceHasConsistentAggregates(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-consistent-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-consistent", "USD", "2099-01", "OPEN", nil)
	seedAPILineItem(t, ctx, billID, "ref-consistent-001", 1500, "USD", "wire_transfer", "Outbound wire")
	seedAPILineItem(t, ctx, billID, "ref-consistent-002", -250, "USD", "correction", "Fee correction")

	invoice, err := readBillWithLineItemsResource(ctx, billID)
	if err != nil {
		t.Fatalf("read bill with line items: %v", err)
	}

	if invoice.ItemCount != len(invoice.LineItems) {
		t.Fatalf("itemCount = %d, want len(lineItems) %d", invoice.ItemCount, len(invoice.LineItems))
	}
	if len(invoice.LineItems) != 2 {
		t.Fatalf("lineItems length = %d, want 2", len(invoice.LineItems))
	}

	var total int64
	sawCredit := false
	for _, item := range invoice.LineItems {
		amount, err := strconv.ParseInt(item.MinorAmount, 10, 64)
		if err != nil {
			t.Fatalf("parse line item amount %q: %v", item.MinorAmount, err)
		}
		if amount < 0 {
			sawCredit = true
		}
		total += amount
	}
	if !sawCredit {
		t.Fatal("expected returned lineItems to include a negative credit row")
	}
	if invoice.TotalMinorAmount != strconv.FormatInt(total, 10) {
		t.Fatalf("totalMinorAmount = %q, want sum(lineItems) %d", invoice.TotalMinorAmount, total)
	}
	if invoice.TotalMinorAmount != "1250" {
		t.Fatalf("totalMinorAmount = %q, want 1250 including negative credit", invoice.TotalMinorAmount)
	}
}

func TestReadBillWithLineItemsResourceZeroItems(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-zero-items-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-zero-items", "USD", "2099-01", "OPEN", nil)

	invoice, err := readBillWithLineItemsResource(ctx, billID)
	if err != nil {
		t.Fatalf("read bill with zero line items: %v", err)
	}
	if invoice.TotalMinorAmount != "0" {
		t.Fatalf("totalMinorAmount = %q, want 0", invoice.TotalMinorAmount)
	}
	if invoice.ItemCount != 0 {
		t.Fatalf("itemCount = %d, want 0", invoice.ItemCount)
	}
	if invoice.LineItems == nil {
		t.Fatal("lineItems is nil, want empty slice")
	}
	if len(invoice.LineItems) != 0 {
		t.Fatalf("lineItems length = %d, want 0", len(invoice.LineItems))
	}
}

func TestGetBillMissingAndBadIncludeLineItems(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-missing-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)

	missing := performGetBill(t, &Service{}, billID, "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want 404. Body: %s", missing.Code, missing.Body.String())
	}
	assertProblem(t, missing, "bill-not-found", http.StatusNotFound)

	badQuery := performGetBill(t, &Service{}, billID, "definitely")
	if badQuery.Code != http.StatusBadRequest {
		t.Fatalf("bad query status = %d, want 400. Body: %s", badQuery.Code, badQuery.Body.String())
	}
	assertProblem(t, badQuery, "invalid-request", http.StatusBadRequest)
}

func TestListBillsReturnsEmptyForUnmatchedFilter(t *testing.T) {
	resp := performListBills(t, &Service{}, url.Values{"clientId": []string{"api-list-empty"}})

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	body := decodeListBillsResponse(t, resp)
	if len(body.Bills) != 0 || body.NextCursor != "" || body.HasMore {
		t.Fatalf("list response = %#v, want empty terminal page", body)
	}
}

func TestListBillsFiltersAndComputedTotals(t *testing.T) {
	ctx := context.Background()
	clientID := "api-list-filter"
	targetID := "bill-api-list-filter-USD-2099-01"
	otherStatusID := "bill-api-list-filter-USD-2099-02"
	otherCurrencyID := "bill-api-list-filter-GEL-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{targetID, otherStatusID, otherCurrencyID} {
		cleanupActivityBill(t, ctx, id)
	}
	seedActivityBill(t, ctx, targetID, clientID, "USD", "2099-01", "CLOSED", &closedAt)
	seedActivityBill(t, ctx, otherStatusID, clientID, "USD", "2099-02", "OPEN", nil)
	seedActivityBill(t, ctx, otherCurrencyID, clientID, "GEL", "2099-01", "CLOSED", &closedAt)
	seedAPILineItem(t, ctx, targetID, "ref-list-filter-001", 1000, "USD", "wire_transfer", "Wire")
	seedAPILineItem(t, ctx, targetID, "ref-list-filter-002", -125, "USD", "correction", "Credit")

	resp := performListBills(t, &Service{}, url.Values{
		"clientId": []string{clientID},
		"status":   []string{"CLOSED"},
		"currency": []string{"USD"},
		"period":   []string{"2099-01"},
		"limit":    []string{"50"},
	})

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	body := decodeListBillsResponse(t, resp)
	if len(body.Bills) != 1 {
		t.Fatalf("bills length = %d, want 1: %#v", len(body.Bills), body.Bills)
	}
	got := body.Bills[0]
	if got.BillID != targetID || got.TotalMinorAmount != "875" || got.ItemCount != 2 {
		t.Fatalf("listed bill = %#v, want target total/count 875/2", got)
	}
	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw list response: %v", err)
	}
	firstBill := raw["bills"].([]any)[0].(map[string]any)
	if _, ok := firstBill["lineItems"]; ok {
		t.Fatal("list response inlined lineItems")
	}
}

func TestListBillsCursorPaginationIsDeterministicForSameOpenedAt(t *testing.T) {
	ctx := context.Background()
	clientID := "api-list-page"
	openedAt := time.Date(2099, 1, 10, 10, 0, 0, 0, time.UTC)
	olderOpenedAt := time.Date(2099, 1, 10, 9, 59, 0, 0, time.UTC)
	billIDs := []string{
		"bill-api-list-page-c-USD-2099-01",
		"bill-api-list-page-z-USD-2099-02",
		"bill-api-list-page-b-USD-2099-03",
		"bill-api-list-page-a-USD-2099-04",
	}
	for _, id := range billIDs {
		cleanupActivityBill(t, ctx, id)
	}
	periods := []string{"2099-01", "2099-02", "2099-03"}
	for i, id := range billIDs[:3] {
		seedActivityBill(t, ctx, id, clientID, "USD", periods[i], "OPEN", nil)
		setAPIBillOpenedAt(t, ctx, id, openedAt)
	}
	seedActivityBill(t, ctx, billIDs[3], clientID, "USD", "2099-04", "OPEN", nil)
	setAPIBillOpenedAt(t, ctx, billIDs[3], olderOpenedAt)

	expectedOrder := []string{
		"bill-api-list-page-z-USD-2099-02",
		"bill-api-list-page-c-USD-2099-01",
		"bill-api-list-page-b-USD-2099-03",
		"bill-api-list-page-a-USD-2099-04",
	}
	cursor := ""
	for page, wantBillID := range expectedOrder {
		values := url.Values{
			"clientId": []string{clientID},
			"limit":    []string{"1"},
		}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		resp := performListBills(t, &Service{}, values)
		if resp.Code != http.StatusOK {
			t.Fatalf("page %d status = %d, want 200. Body: %s", page+1, resp.Code, resp.Body.String())
		}
		body := decodeListBillsResponse(t, resp)
		if len(body.Bills) != 1 {
			t.Fatalf("page %d bills length = %d, want 1: %#v", page+1, len(body.Bills), body.Bills)
		}
		if body.Bills[0].BillID != wantBillID {
			t.Fatalf("page %d bill = %q, want %q", page+1, body.Bills[0].BillID, wantBillID)
		}
		wantHasMore := page < len(expectedOrder)-1
		if body.HasMore != wantHasMore {
			t.Fatalf("page %d hasMore = %v, want %v", page+1, body.HasMore, wantHasMore)
		}
		if wantHasMore && body.NextCursor == "" {
			t.Fatalf("page %d nextCursor is empty, want cursor", page+1)
		}
		if !wantHasMore && body.NextCursor != "" {
			t.Fatalf("page %d nextCursor = %q, want empty", page+1, body.NextCursor)
		}
		cursor = body.NextCursor
	}
}

func TestListBillsValidationFailures(t *testing.T) {
	tests := []struct {
		name  string
		query url.Values
	}{
		{name: "bad status", query: url.Values{"status": []string{"DRAINING"}}},
		{name: "bad currency", query: url.Values{"currency": []string{"usd"}}},
		{name: "bad period", query: url.Values{"period": []string{"2099-13"}}},
		{name: "zero limit", query: url.Values{"limit": []string{"0"}}},
		{name: "non integer limit", query: url.Values{"limit": []string{"many"}}},
		{name: "bad cursor", query: url.Values{"cursor": []string{"not-a-cursor"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := performListBills(t, &Service{}, tt.query)
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
			}
			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
		})
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

	temporalClient := &openTemporalClient{
		beforeExecute: func(ctx context.Context, _ client.StartWorkflowOptions, _ interface{}, _ []interface{}) error {
			resource, err := readBillMetadata(ctx, expectedBillID)
			if err != nil {
				t.Fatalf("bill was not persisted before ExecuteWorkflow: %v", err)
			}
			if resource.ClientID != reqBody.ClientID || resource.Status != OPEN.String() {
				t.Fatalf("persisted bill before ExecuteWorkflow = %#v", resource)
			}
			return nil
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

	if temporalClient.executeWorkflowCount != 1 {
		t.Fatalf("ExecuteWorkflow calls = %d, want 1", temporalClient.executeWorkflowCount)
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
	if temporalClient.startWorkflow != BillWorkflowName || len(temporalClient.startArgs) != 1 {
		t.Fatalf("workflow start = %#v args=%#v, want %q with one BillInput", temporalClient.startWorkflow, temporalClient.startArgs, BillWorkflowName)
	}
	if gotInput, ok := temporalClient.startArgs[0].(BillInput); !ok || gotInput != (BillInput{
		ClientID: reqBody.ClientID,
		Currency: reqBody.Currency,
		Period:   reqBody.Period,
	}) {
		t.Fatalf("workflow input = %#v, want shared BillInput for request", temporalClient.startArgs[0])
	}
	assertActivityBillRow(t, ctx, expectedBillID, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", 1)
}

func TestOpenBillValidationFailuresDoNotCallTemporal(t *testing.T) {
	tests := []struct {
		name string
		body OpenBillRequest
	}{
		{name: "missing client", body: OpenBillRequest{Currency: "USD", Period: "2099-01"}},
		{name: "bad currency", body: OpenBillRequest{ClientID: "acme", Currency: "usd", Period: "2099-01"}},
		{name: "bad period", body: OpenBillRequest{ClientID: "acme", Currency: "USD", Period: "2099-13"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{}
			svc := &Service{
				temporalClient: temporalClient,
				temporalConfig: defaultTemporalConfig(),
			}

			resp := performOpenBill(t, svc, tt.body)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
			}
			assertProblem(t, resp, "invalid-request", http.StatusBadRequest)
			if temporalClient.executeWorkflowCount != 0 {
				t.Fatalf("Temporal was called for invalid request: execute=%d", temporalClient.executeWorkflowCount)
			}
		})
	}
}

func TestOpenBillUnsupportedCurrencyReturns400AndDoesNotCallTemporal(t *testing.T) {
	temporalClient := &openTemporalClient{}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, OpenBillRequest{
		ClientID: "api-unsupported-currency",
		Currency: "EUR",
		Period:   "2099-01",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "unsupported-currency", http.StatusBadRequest)
	if temporalClient.executeWorkflowCount != 0 {
		t.Fatalf("Temporal was called for unsupported currency: execute=%d", temporalClient.executeWorkflowCount)
	}
}

func TestOpenBillElapsedPeriodReturns400AndDoesNotCallTemporal(t *testing.T) {
	ctx := context.Background()
	id := billID("api-elapsed", "USD", "2000-01")
	cleanupActivityBill(t, ctx, id)
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

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "period-elapsed", http.StatusBadRequest)
	if temporalClient.executeWorkflowCount != 0 {
		t.Fatalf("Temporal was called for elapsed period: execute=%d", temporalClient.executeWorkflowCount)
	}
	if _, err := readBillMetadata(ctx, id); !errors.Is(err, sqldb.ErrNoRows) {
		t.Fatalf("elapsed bill persistence error = %v, want no row", err)
	}
}

func TestOpenBillAlreadyStartedReturns409(t *testing.T) {
	ctx := context.Background()
	reqBody := OpenBillRequest{ClientID: "api-duplicate", Currency: "USD", Period: "2099-01"}
	id := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, id)
	seedActivityBill(t, ctx, id, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", nil)
	temporalClient := &openTemporalClient{
		executeErr: serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", ""),
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
}

func TestOpenBillRejectsClosedBillAsNotOpen(t *testing.T) {
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

	temporalClient := &openTemporalClient{}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, reqBody)

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "bill-not-open", http.StatusConflict)
	assertProblemDetail(t, resp, "bill workflow not open")
	if temporalClient.executeWorkflowCount != 0 {
		t.Fatalf("ExecuteWorkflow calls = %d, want 0 for CLOSED bill", temporalClient.executeWorkflowCount)
	}
	if strings.Contains(resp.Body.String(), "CLOSED") {
		t.Fatalf("problem response leaked closed bill body: %s", resp.Body.String())
	}
}

func TestOpenBillStartFailureLeavesOpenRowAndRetryHealsIt(t *testing.T) {
	ctx := context.Background()
	rawErr := "dial tcp 10.0.4.23:7233: connect: connection refused"
	reqBody := OpenBillRequest{ClientID: "api-unavailable", Currency: "USD", Period: "2099-01"}
	id := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, id)
	temporalClient := &openTemporalClient{
		executeErr: errors.New(rawErr),
	}
	svc := &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}

	resp := performOpenBill(t, svc, reqBody)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "open-unavailable", http.StatusServiceUnavailable)
	assertProblemDetail(t, resp, "open workflow did not complete; retry after a short delay")
	assertProblemDoesNotContain(t, resp, rawErr)
	assertActivityBillRow(t, ctx, id, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", 1)

	temporalClient.executeErr = nil
	retryResp := performOpenBill(t, svc, reqBody)
	if retryResp.Code != http.StatusCreated {
		t.Fatalf("retry status = %d, want 201. Body: %s", retryResp.Code, retryResp.Body.String())
	}
	if temporalClient.executeWorkflowCount != 2 {
		t.Fatalf("ExecuteWorkflow calls = %d, want 2", temporalClient.executeWorkflowCount)
	}
	assertActivityBillRow(t, ctx, id, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", 1)
}

func TestOpenBillConcurrentIdenticalRequestsCreateOneRowAndOneWorkflow(t *testing.T) {
	ctx := context.Background()
	reqBody := OpenBillRequest{ClientID: "api-concurrent-open", Currency: "USD", Period: "2099-10"}
	id := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, id)

	workflowStarted := false
	temporalClient := &openTemporalClient{
		beforeExecute: func(context.Context, client.StartWorkflowOptions, interface{}, []interface{}) error {
			if workflowStarted {
				return serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", "")
			}
			workflowStarted = true
			return nil
		},
	}
	svc := &Service{temporalClient: temporalClient, temporalConfig: defaultTemporalConfig()}

	type result struct {
		response *OpenBillResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			response, err := svc.OpenBill(ctx, &reqBody)
			results <- result{response: response, err: err}
		}()
	}
	close(start)

	created := 0
	conflicts := 0
	for range 2 {
		got := <-results
		switch {
		case got.err == nil && got.response != nil && got.response.HTTPStatus == http.StatusCreated:
			created++
		case got.err != nil && errs.Code(got.err) == errs.AlreadyExists:
			conflicts++
		default:
			t.Fatalf("unexpected concurrent open result: response=%#v err=%v", got.response, got.err)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("concurrent results: created=%d conflicts=%d, want 1/1", created, conflicts)
	}
	if temporalClient.executeWorkflowCount != 2 {
		t.Fatalf("ExecuteWorkflow calls = %d, want 2", temporalClient.executeWorkflowCount)
	}
	assertActivityBillRow(t, ctx, id, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", 1)
}

func TestOpenBillExistingOpenBillCanRecoverAfterPeriodElapsed(t *testing.T) {
	ctx := context.Background()
	reqBody := OpenBillRequest{ClientID: "api-elapsed-recovery", Currency: "USD", Period: "2000-01"}
	id := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, id)
	seedActivityBill(t, ctx, id, reqBody.ClientID, reqBody.Currency, reqBody.Period, "OPEN", nil)
	temporalClient := &openTemporalClient{}

	resp := performOpenBill(t, &Service{
		temporalClient: temporalClient,
		temporalConfig: defaultTemporalConfig(),
	}, reqBody)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201. Body: %s", resp.Code, resp.Body.String())
	}
	if temporalClient.executeWorkflowCount != 1 {
		t.Fatalf("ExecuteWorkflow calls = %d, want 1", temporalClient.executeWorkflowCount)
	}
}

func TestOpenBillNilTemporalClientDoesNotPersistBill(t *testing.T) {
	ctx := context.Background()
	reqBody := OpenBillRequest{ClientID: "api-nil-temporal", Currency: "USD", Period: "2099-01"}
	id := billID(reqBody.ClientID, reqBody.Currency, reqBody.Period)
	cleanupActivityBill(t, ctx, id)

	resp := performOpenBill(t, &Service{}, reqBody)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503. Body: %s", resp.Code, resp.Body.String())
	}
	assertProblem(t, resp, "open-unavailable", http.StatusServiceUnavailable)
	if _, err := readBillMetadata(ctx, id); !errors.Is(err, sqldb.ErrNoRows) {
		t.Fatalf("nil-Temporal persistence error = %v, want no row", err)
	}
}

func TestCloseBillOpenBillSignalsWorkflowSealsLedgerAndReturnsSuccess(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-open", "USD", "2099-01", "OPEN", nil)

	temporalClient := &openTemporalClient{
		run: &openWorkflowRun{},
	}
	svc := &Service{temporalClient: temporalClient}

	resp := performCloseBill(t, svc, billID, `{"reason":"explicit-test-close"}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	assertCloseTemporalCalls(t, temporalClient, billID, CloseSignal{Reason: "explicit-test-close"})
	assertCloseSuccessResponse(t, resp)
	sealed, err := readBillMetadata(ctx, billID)
	if err != nil {
		t.Fatalf("read sealed bill: %v", err)
	}
	if sealed.Status != CLOSED.String() || sealed.ClosedAt == nil {
		t.Fatalf("sealed bill status/closedAt = %s/%v, want CLOSED/non-nil", sealed.Status, sealed.ClosedAt)
	}
}

func TestCloseBillAlreadyClosedReturnsSuccessWithoutTemporal(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-closed-USD-2099-01"
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-closed", "USD", "2099-01", "CLOSED", &closedAt)
	temporalClient := &openTemporalClient{}
	svc := &Service{temporalClient: temporalClient}

	resp := performCloseBill(t, svc, billID, `{"reason":"already-closed"}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	if temporalClient.signalWorkflowCount != 0 || temporalClient.getWorkflowCount != 0 {
		t.Fatalf("Temporal calls = signal:%d get:%d, want none", temporalClient.signalWorkflowCount, temporalClient.getWorkflowCount)
	}
	assertCloseSuccessResponse(t, resp)
	sealed, err := readBillMetadata(ctx, billID)
	if err != nil {
		t.Fatalf("read re-closed bill: %v", err)
	}
	if sealed.ClosedAt == nil || !sealed.ClosedAt.Equal(closedAt) {
		t.Fatalf("closedAt = %v, want unchanged %s", sealed.ClosedAt, closedAt.Format(time.RFC3339))
	}
}

func TestCloseBillValidationFailuresDoNotCallTemporal(t *testing.T) {
	tests := []struct {
		name   string
		billID string
		body   string
	}{
		{name: "missing bill ID", billID: "", body: `{}`},
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

func TestCloseBillTemporalNotFoundWithClosedLedgerReturnsSuccess(t *testing.T) {
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
	assertCloseSuccessResponse(t, resp)
}

func TestCloseBillTemporalNotFoundWithOpenLedgerSealsAndReturnsSuccess(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-not-found-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-not-found-open", "USD", "2099-01", "OPEN", nil)

	temporalClient := &openTemporalClient{
		signalErr: serviceerror.NewNotFound("workflow not found"),
	}

	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	assertCloseSuccessResponse(t, resp)
	sealed, err := readBillMetadata(ctx, billID)
	if err != nil {
		t.Fatalf("read NotFound-recovered bill: %v", err)
	}
	if sealed.Status != CLOSED.String() || sealed.ClosedAt == nil {
		t.Fatalf("recovered bill status/closedAt = %s/%v, want CLOSED/non-nil", sealed.Status, sealed.ClosedAt)
	}
}

func TestCloseBillWorkflowGetNotFoundSealsAndReturnsSuccess(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-close-get-not-found-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-close-get-not-found", "USD", "2099-01", "OPEN", nil)

	temporalClient := &openTemporalClient{
		run: &openWorkflowRun{err: serviceerror.NewNotFound("workflow result not found")},
	}
	resp := performCloseBill(t, &Service{temporalClient: temporalClient}, billID, `{}`)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	assertCloseSuccessResponse(t, resp)
}

func TestSealBillSealsOpenBillIdempotently(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-seal-open-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-seal-open", "USD", "2099-01", "OPEN", nil)

	svc := &Service{}
	first, err := svc.SealBill(ctx, &SealBillRequest{BillID: billID})
	if err != nil {
		t.Fatalf("first SealBill returned error: %v", err)
	}
	if first == nil || !first.Success {
		t.Fatalf("first SealBill response = %#v, want success", first)
	}
	sealed, err := readBillMetadata(ctx, billID)
	if err != nil {
		t.Fatalf("read sealed bill: %v", err)
	}
	if sealed.Status != CLOSED.String() || sealed.ClosedAt == nil {
		t.Fatalf("sealed bill status/closedAt = %s/%v, want CLOSED/non-nil", sealed.Status, sealed.ClosedAt)
	}
	closedAt := *sealed.ClosedAt

	second, err := svc.SealBill(ctx, &SealBillRequest{BillID: billID})
	if err != nil {
		t.Fatalf("second SealBill returned error: %v", err)
	}
	if second == nil || !second.Success {
		t.Fatalf("second SealBill response = %#v, want success", second)
	}
	resealed, err := readBillMetadata(ctx, billID)
	if err != nil {
		t.Fatalf("read re-sealed bill: %v", err)
	}
	if resealed.ClosedAt == nil || !resealed.ClosedAt.Equal(closedAt) {
		t.Fatalf("closedAt = %v, want unchanged %s", resealed.ClosedAt, closedAt.Format(time.RFC3339Nano))
	}
}

func TestSealBillConcurrentCallsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-seal-concurrent-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-seal-concurrent", "USD", "2099-01", "OPEN", nil)

	svc := &Service{}
	errsCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := svc.SealBill(ctx, &SealBillRequest{BillID: billID})
			if err == nil && (resp == nil || !resp.Success) {
				err = errors.New("seal response did not confirm success")
			}
			errsCh <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errsCh; err != nil {
			t.Fatalf("concurrent SealBill returned error: %v", err)
		}
	}
}

func TestSealBillValidationNoMatchingRowAndDatabaseFailure(t *testing.T) {
	svc := &Service{}
	if _, err := svc.SealBill(context.Background(), nil); errs.Code(err) != errs.InvalidArgument {
		t.Fatalf("nil request error code = %s, want invalid_argument", errs.Code(err))
	}

	missingID := "bill-api-seal-missing-USD-2099-01"
	cleanupActivityBill(t, context.Background(), missingID)
	resp, err := svc.SealBill(context.Background(), &SealBillRequest{BillID: missingID})
	if err != nil {
		t.Fatalf("missing bill SealBill returned error: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("missing bill SealBill response = %#v, want success", resp)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.SealBill(canceledCtx, &SealBillRequest{BillID: missingID}); errs.Code(err) != errs.Unavailable {
		t.Fatalf("database failure error code = %s, want unavailable", errs.Code(err))
	}
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
	executeMu  sync.Mutex

	executeWorkflowCount int
	signalWorkflowCount  int
	getWorkflowCount     int

	startOptions     client.StartWorkflowOptions
	startWorkflow    interface{}
	startArgs        []interface{}
	signalWorkflowID string
	signalRunID      string
	signalName       string
	signalArg        interface{}
	getWorkflowID    string
	getRunID         string

	executeErr      error
	beforeExecute   func(context.Context, client.StartWorkflowOptions, interface{}, []interface{}) error
	run             client.WorkflowRun
	signalErr       error
	beforeSignalErr func(context.Context) error
}

func (c *openTemporalClient) Close() {
	c.closeCount++
}

func (c *openTemporalClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	c.executeMu.Lock()
	defer c.executeMu.Unlock()

	c.executeWorkflowCount++
	c.startOptions = options
	c.startWorkflow = workflow
	c.startArgs = args
	if c.beforeExecute != nil {
		if err := c.beforeExecute(ctx, options, workflow, args); err != nil {
			return nil, err
		}
	}
	if c.executeErr != nil {
		return nil, c.executeErr
	}
	return &openWorkflowRun{}, nil
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

func performOpenBill(t *testing.T, svc *Service, body OpenBillRequest) *httptest.ResponseRecorder {
	t.Helper()

	resp := httptest.NewRecorder()
	out, err := svc.OpenBill(context.Background(), &body)
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	resp.Header().Set("Location", out.Location)
	writeTestJSON(t, resp, out.HTTPStatus, struct {
		BillID           string     `json:"billId"`
		ClientID         string     `json:"clientId"`
		Currency         string     `json:"currency"`
		Period           string     `json:"period"`
		Status           string     `json:"status"`
		TotalMinorAmount string     `json:"totalMinorAmount"`
		ItemCount        int        `json:"itemCount"`
		OpenedAt         time.Time  `json:"openedAt"`
		ClosedAt         *time.Time `json:"closedAt"`
	}{
		BillID:           out.BillID,
		ClientID:         out.ClientID,
		Currency:         out.Currency,
		Period:           out.Period,
		Status:           out.Status,
		TotalMinorAmount: out.TotalMinorAmount,
		ItemCount:        out.ItemCount,
		OpenedAt:         out.OpenedAt,
		ClosedAt:         out.ClosedAt,
	})
	return resp
}

func performCloseBill(t *testing.T, svc *Service, billID, body string) *httptest.ResponseRecorder {
	t.Helper()

	var input CloseBillRequest
	if body != "" {
		if err := json.Unmarshal([]byte(body), &input); err != nil {
			resp := httptest.NewRecorder()
			errs.HTTPError(resp, apiError(errs.InvalidArgument, "invalid-request", "request body must be valid JSON"))
			return resp
		}
	}
	resp := httptest.NewRecorder()
	out, err := svc.CloseBill(context.Background(), billID, &input)
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	writeTestJSON(t, resp, http.StatusOK, out)
	return resp
}

func performGetBill(t *testing.T, svc *Service, billID, includeLineItems string) *httptest.ResponseRecorder {
	t.Helper()

	include := false
	if includeLineItems != "" {
		parsed, err := strconv.ParseBool(includeLineItems)
		if err != nil {
			resp := httptest.NewRecorder()
			errs.HTTPError(resp, apiError(errs.InvalidArgument, "invalid-request", "includeLineItems must be a boolean"))
			return resp
		}
		include = parsed
	}
	resp := httptest.NewRecorder()
	out, err := svc.GetBill(context.Background(), billID, &GetBillRequest{IncludeLineItems: include})
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	writeTestJSON(t, resp, http.StatusOK, out)
	return resp
}

func performListBills(t *testing.T, svc *Service, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	req := &ListBillsRequest{
		ClientID: values.Get("clientId"),
		Status:   values.Get("status"),
		Currency: values.Get("currency"),
		Period:   values.Get("period"),
		Cursor:   values.Get("cursor"),
	}
	if rawLimit := values.Get("limit"); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil {
			resp := httptest.NewRecorder()
			errs.HTTPError(resp, apiError(errs.InvalidArgument, "invalid-request", "limit must be an integer"))
			return resp
		}
		req.Limit = option.Some(limit)
	}
	resp := httptest.NewRecorder()
	out, err := svc.ListBills(context.Background(), req)
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	writeTestJSON(t, resp, http.StatusOK, out)
	return resp
}

func writeTestJSON(t *testing.T, resp *httptest.ResponseRecorder, status int, body interface{}) {
	t.Helper()

	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(status)
	if err := json.NewEncoder(resp).Encode(body); err != nil {
		t.Fatalf("encode response: %v", err)
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

func assertCloseSuccessResponse(t *testing.T, resp *httptest.ResponseRecorder) {
	t.Helper()

	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode close response: %v", err)
	}
	if len(raw) != 1 || raw["success"] != true {
		t.Fatalf("close response = %s, want exactly {\"success\":true}", resp.Body.String())
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

func decodeListBillsResponse(t *testing.T, resp *httptest.ResponseRecorder) ListBillsResponse {
	t.Helper()

	var body ListBillsResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode list response body: %v", err)
	}
	return body
}

func seedAPILineItem(t *testing.T, ctx context.Context, billID, reference string, amountMinor int64, currency, feeType, description string) {
	t.Helper()
	seedAPILineItemWithStatus(t, ctx, billID, reference, amountMinor, currency, feeType, description, "FINALIZED")
}

func seedAPILineItemWithStatus(t *testing.T, ctx context.Context, billID, reference string, amountMinor int64, currency, feeType, description, status string) {
	t.Helper()

	_, err := db.Exec(ctx, `
		INSERT INTO line_items
			(bill_id, reference, amount_minor, currency, fee_type, description, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		billID,
		reference,
		amountMinor,
		currency,
		feeType,
		description,
		status,
	)
	if err != nil {
		t.Fatalf("seed line item %s/%s: %v", billID, reference, err)
	}
}

func setAPIBillOpenedAt(t *testing.T, ctx context.Context, billID string, openedAt time.Time) {
	t.Helper()

	_, err := db.Exec(ctx, `
		UPDATE bills
		   SET opened_at = $2
		 WHERE bill_id = $1`,
		billID,
		openedAt,
	)
	if err != nil {
		t.Fatalf("set opened_at for %s: %v", billID, err)
	}
}

func assertProblem(t *testing.T, resp *httptest.ResponseRecorder, wantType string, wantStatus int) {
	t.Helper()

	var body struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
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

func assertProblemDetail(t *testing.T, resp *httptest.ResponseRecorder, wantDetail string) {
	t.Helper()

	var body struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode Encore error response: %v", err)
	}
	if body.Message != wantDetail {
		t.Fatalf("error message = %q, want %q", body.Message, wantDetail)
	}
}

func assertProblemDoesNotContain(t *testing.T, resp *httptest.ResponseRecorder, forbidden string) {
	t.Helper()

	if strings.Contains(resp.Body.String(), forbidden) {
		t.Fatalf("problem response leaked %q: %s", forbidden, resp.Body.String())
	}
}
