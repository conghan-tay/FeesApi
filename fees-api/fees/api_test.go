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
	"testing"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/types/option"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
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
	if _, ok := raw["nextCursor"]; ok {
		t.Fatal("nextCursor present without includeLineItems=true")
	}
	if _, ok := raw["hasMore"]; ok {
		t.Fatal("hasMore present without includeLineItems=true")
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
	body := decodeGetBillResponse(t, resp)
	if body.TotalMinorAmount != "1000" || body.ItemCount != 2 {
		t.Fatalf("total/count = %s/%d, want 1000/2", body.TotalMinorAmount, body.ItemCount)
	}
	if body.LineItems == nil {
		t.Fatal("lineItems missing, want included page")
	}
	if len(*body.LineItems) != 2 {
		t.Fatalf("lineItems length = %d, want 2", len(*body.LineItems))
	}
	if (*body.LineItems)[0].Reference != "ref-get-items-001" || (*body.LineItems)[1].Reference != "ref-get-items-002" {
		t.Fatalf("line item order = %#v, want insertion order", *body.LineItems)
	}
	if body.NextCursor == nil || *body.NextCursor != "" {
		t.Fatalf("nextCursor = %v, want empty cursor", body.NextCursor)
	}
	if body.HasMore == nil || *body.HasMore {
		t.Fatalf("hasMore = %v, want false", body.HasMore)
	}
}

func TestGetBillLineItemsPaginatesWithCursor(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-page-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-page", "USD", "2099-01", "OPEN", nil)
	for i := 1; i <= 5; i++ {
		seedAPILineItem(t, ctx, billID, "ref-get-page-00"+strconv.Itoa(i), 100, "USD", "wire_transfer", "Paged item")
	}

	cursor := ""
	wantPages := [][]string{
		{"ref-get-page-001", "ref-get-page-002"},
		{"ref-get-page-003", "ref-get-page-004"},
		{"ref-get-page-005"},
	}
	for page, wantRefs := range wantPages {
		values := url.Values{
			"includeLineItems": []string{"true"},
			"limit":            []string{"2"},
		}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		resp := performGetBillWithQuery(t, &Service{}, billID, values)
		if resp.Code != http.StatusOK {
			t.Fatalf("page %d status = %d, want 200. Body: %s", page+1, resp.Code, resp.Body.String())
		}
		body := decodeGetBillResponse(t, resp)
		if body.TotalMinorAmount != "500" || body.ItemCount != 5 {
			t.Fatalf("page %d total/count = %s/%d, want 500/5", page+1, body.TotalMinorAmount, body.ItemCount)
		}
		if body.LineItems == nil {
			t.Fatalf("page %d lineItems missing", page+1)
		}
		if len(*body.LineItems) != len(wantRefs) {
			t.Fatalf("page %d lineItems length = %d, want %d: %#v", page+1, len(*body.LineItems), len(wantRefs), *body.LineItems)
		}
		for i, wantRef := range wantRefs {
			if (*body.LineItems)[i].Reference != wantRef {
				t.Fatalf("page %d item %d reference = %q, want %q", page+1, i, (*body.LineItems)[i].Reference, wantRef)
			}
		}

		wantHasMore := page < len(wantPages)-1
		if body.HasMore == nil || *body.HasMore != wantHasMore {
			t.Fatalf("page %d hasMore = %v, want %v", page+1, body.HasMore, wantHasMore)
		}
		if body.NextCursor == nil {
			t.Fatalf("page %d nextCursor missing", page+1)
		}
		if wantHasMore && *body.NextCursor == "" {
			t.Fatalf("page %d nextCursor is empty, want cursor", page+1)
		}
		if !wantHasMore && *body.NextCursor != "" {
			t.Fatalf("page %d nextCursor = %q, want empty", page+1, *body.NextCursor)
		}
		cursor = *body.NextCursor
	}
}

func TestGetBillLineItemsDefaultLimit(t *testing.T) {
	ctx := context.Background()
	billID := "bill-api-get-default-page-USD-2099-01"
	cleanupActivityBill(t, ctx, billID)
	seedActivityBill(t, ctx, billID, "api-get-default-page", "USD", "2099-01", "OPEN", nil)
	for i := 1; i <= defaultLineItemsLimit+1; i++ {
		seedAPILineItem(t, ctx, billID, "ref-get-default-"+strconv.Itoa(i), 1, "USD", "wire_transfer", "Default page item")
	}

	resp := performGetBill(t, &Service{}, billID, "true")
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. Body: %s", resp.Code, resp.Body.String())
	}
	body := decodeGetBillResponse(t, resp)
	if body.LineItems == nil {
		t.Fatal("lineItems missing, want default page")
	}
	if len(*body.LineItems) != defaultLineItemsLimit {
		t.Fatalf("lineItems length = %d, want default limit %d", len(*body.LineItems), defaultLineItemsLimit)
	}
	if body.ItemCount != defaultLineItemsLimit+1 || body.TotalMinorAmount != strconv.Itoa(defaultLineItemsLimit+1) {
		t.Fatalf("total/count = %s/%d, want %d/%d", body.TotalMinorAmount, body.ItemCount, defaultLineItemsLimit+1, defaultLineItemsLimit+1)
	}
	if body.HasMore == nil || !*body.HasMore {
		t.Fatalf("hasMore = %v, want true", body.HasMore)
	}
	if body.NextCursor == nil || *body.NextCursor == "" {
		t.Fatalf("nextCursor = %v, want cursor", body.NextCursor)
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

	seedActivityBill(t, ctx, billID, "api-get-missing", "USD", "2099-01", "OPEN", nil)
	badCursor := performGetBillWithQuery(t, &Service{}, billID, url.Values{
		"includeLineItems": []string{"true"},
		"cursor":           []string{"not-a-cursor"},
	})
	if badCursor.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d, want 400. Body: %s", badCursor.Code, badCursor.Body.String())
	}
	assertProblem(t, badCursor, "invalid-request", http.StatusBadRequest)

	wrongBillCursor := performGetBillWithQuery(t, &Service{}, billID, url.Values{
		"includeLineItems": []string{"true"},
		"cursor":           []string{encodeLineItemsCursor("bill-api-get-other-USD-2099-01", 1)},
	})
	if wrongBillCursor.Code != http.StatusBadRequest {
		t.Fatalf("wrong-bill cursor status = %d, want 400. Body: %s", wrongBillCursor.Code, wrongBillCursor.Body.String())
	}
	assertProblem(t, wrongBillCursor, "invalid-request", http.StatusBadRequest)

	limitWithoutItems := performGetBillWithQuery(t, &Service{}, billID, url.Values{
		"limit": []string{"2"},
	})
	if limitWithoutItems.Code != http.StatusBadRequest {
		t.Fatalf("limit without include status = %d, want 400. Body: %s", limitWithoutItems.Code, limitWithoutItems.Body.String())
	}
	assertProblem(t, limitWithoutItems, "invalid-request", http.StatusBadRequest)

	cursorWithoutItems := performGetBillWithQuery(t, &Service{}, billID, url.Values{
		"cursor": []string{encodeLineItemsCursor(billID, 1)},
	})
	if cursorWithoutItems.Code != http.StatusBadRequest {
		t.Fatalf("cursor without include status = %d, want 400. Body: %s", cursorWithoutItems.Code, cursorWithoutItems.Body.String())
	}
	assertProblem(t, cursorWithoutItems, "invalid-request", http.StatusBadRequest)
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
			if temporalClient.newStartCount != 0 || temporalClient.updateWithStartCount != 0 {
				t.Fatalf("Temporal was called for invalid request: new=%d update=%d", temporalClient.newStartCount, temporalClient.updateWithStartCount)
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
	if temporalClient.newStartCount != 0 || temporalClient.updateWithStartCount != 0 {
		t.Fatalf("Temporal was called for unsupported currency: new=%d update=%d", temporalClient.newStartCount, temporalClient.updateWithStartCount)
	}
}

func TestOpenBillElapsedPeriodReturns400AndDoesNotCallTemporal(t *testing.T) {
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
		body   AddLineItemRequest
	}{
		{name: "missing bill ID", billID: "", body: AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "USD", FeeType: "wire"}},
		{name: "missing reference", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{MinorAmount: "1", Currency: "USD", FeeType: "wire"}},
		{name: "missing minor amount", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{Reference: "ref", Currency: "USD", FeeType: "wire"}},
		{name: "invalid minor amount", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{Reference: "ref", MinorAmount: "1.25", Currency: "USD", FeeType: "wire"}},
		{name: "overflow minor amount", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{Reference: "ref", MinorAmount: "9223372036854775808", Currency: "USD", FeeType: "wire"}},
		{name: "lowercase currency", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "usd", FeeType: "wire"}},
		{name: "missing fee type", billID: "bill-acme-USD-2099-01", body: AddLineItemRequest{Reference: "ref", MinorAmount: "1", Currency: "USD"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temporalClient := &openTemporalClient{}
			svc := &Service{temporalClient: temporalClient}

			resp := performAddLineItem(t, svc, tt.billID, tt.body)

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
			wantType:   "no-bill",
		},
		{
			name:       "handle not found",
			handleErr:  serviceerror.NewNotFound("workflow not found"),
			wantStatus: http.StatusNotFound,
			wantType:   "no-bill",
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

func TestAddLineItemWorkflowNotFoundLedgerFallback(t *testing.T) {
	ctx := context.Background()
	closedAt := time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		directErr  error
		handleErr  error
		status     string
		closedAt   *time.Time
		wantStatus int
		wantType   string
	}{
		{
			name:       "direct missing ledger bill",
			directErr:  serviceerror.NewNotFound("workflow not found"),
			wantStatus: http.StatusNotFound,
			wantType:   "no-bill",
		},
		{
			name:       "handle missing ledger bill",
			handleErr:  serviceerror.NewNotFound("workflow not found"),
			wantStatus: http.StatusNotFound,
			wantType:   "no-bill",
		},
		{
			name:       "direct closed ledger bill",
			directErr:  serviceerror.NewNotFound("workflow not found"),
			status:     "CLOSED",
			closedAt:   &closedAt,
			wantStatus: http.StatusConflict,
			wantType:   "bill-closed",
		},
		{
			name:       "handle closed ledger bill",
			handleErr:  serviceerror.NewNotFound("workflow not found"),
			status:     "CLOSED",
			closedAt:   &closedAt,
			wantStatus: http.StatusConflict,
			wantType:   "bill-closed",
		},
		{
			name:       "direct open ledger bill",
			directErr:  serviceerror.NewNotFound("workflow not found"),
			status:     "OPEN",
			wantStatus: http.StatusServiceUnavailable,
			wantType:   "add-line-item-unavailable",
		},
		{
			name:       "handle open ledger bill",
			handleErr:  serviceerror.NewNotFound("workflow not found"),
			status:     "OPEN",
			wantStatus: http.StatusServiceUnavailable,
			wantType:   "add-line-item-unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			billID := "bill-api-add-fallback-" + strings.ReplaceAll(tt.name, " ", "-") + "-USD-2099-01"
			cleanupActivityBill(t, ctx, billID)
			if tt.status != "" {
				clientID := strings.TrimSuffix(strings.TrimPrefix(billID, "bill-"), "-USD-2099-01")
				seedActivityBill(t, ctx, billID, clientID, "USD", "2099-01", tt.status, tt.closedAt)
			}

			temporalClient := &openTemporalClient{
				workflowErr: tt.directErr,
				workflowHandle: &openUpdateHandle{
					err: tt.handleErr,
				},
			}
			resp := performAddLineItem(t, &Service{temporalClient: temporalClient}, billID, AddLineItemRequest{
				Reference:   "ref-fallback",
				MinorAmount: "1500",
				Currency:    "USD",
				FeeType:     "wire_transfer",
				Description: "Outbound USD wire",
			})

			if resp.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d. Body: %s", resp.Code, tt.wantStatus, resp.Body.String())
			}
			assertProblem(t, resp, tt.wantType, tt.wantStatus)
			assertProblemDoesNotContain(t, resp, "workflow not found")
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
		t.Fatalf("invoice identity = %#v, want seeded bill", invoice)
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

func performAddLineItem(t *testing.T, svc *Service, billID string, body AddLineItemRequest) *httptest.ResponseRecorder {
	t.Helper()

	resp := httptest.NewRecorder()
	out, err := svc.AddLineItem(context.Background(), billID, &body)
	if err != nil {
		errs.HTTPError(resp, err)
		return resp
	}
	writeTestJSON(t, resp, out.HTTPStatus, struct {
		Reference string `json:"reference"`
		Applied   bool   `json:"applied"`
	}{
		Reference: out.Reference,
		Applied:   out.Applied,
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

	if includeLineItems != "" {
		return performGetBillWithQuery(t, svc, billID, url.Values{"includeLineItems": []string{includeLineItems}})
	}
	return performGetBillWithQuery(t, svc, billID, nil)
}

func performGetBillWithQuery(t *testing.T, svc *Service, billID string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	req := &GetBillRequest{
		Cursor: values.Get("cursor"),
	}
	if rawInclude := values.Get("includeLineItems"); rawInclude != "" {
		parsed, err := strconv.ParseBool(rawInclude)
		if err != nil {
			resp := httptest.NewRecorder()
			errs.HTTPError(resp, apiError(errs.InvalidArgument, "invalid-request", "includeLineItems must be a boolean"))
			return resp
		}
		req.IncludeLineItems = parsed
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
	out, err := svc.GetBill(context.Background(), billID, req)
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

func decodeGetBillResponse(t *testing.T, resp *httptest.ResponseRecorder) GetBillResponse {
	t.Helper()

	var body GetBillResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode get bill response body: %v", err)
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
