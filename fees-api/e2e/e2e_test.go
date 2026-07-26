package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFeesLifecycleE2E(t *testing.T) {
	if os.Getenv("PAVEBANK_E2E") != "1" {
		t.Skip("set PAVEBANK_E2E=1 with temporal server start-dev and encore run already running")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := NewClient("")
	preflight(t, ctx, client)

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	clientID := "e2e-" + runID
	currency := "USD"
	period := "2099-07"
	expectedBillID := fmt.Sprintf("bill-%s-%s-%s", clientID, currency, period)

	var billID string
	t.Run("open bill", func(t *testing.T) {
		resp, err := client.OpenBill(ctx, OpenBillRequest{
			ClientID: clientID,
			Currency: currency,
			Period:   period,
		})
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusCreated)
		if resp.Body == nil {
			t.Fatal("expected bill body")
		}
		billID = resp.Body.BillID
		if billID != expectedBillID {
			t.Fatalf("billId = %q, want %q", billID, expectedBillID)
		}
		requireLocationForBill(t, resp.Header.Get("Location"), expectedBillID)
		if resp.Body.Status != "OPEN" {
			t.Fatalf("status = %q, want OPEN", resp.Body.Status)
		}
		if resp.Body.TotalMinorAmount != "0" {
			t.Fatalf("totalMinorAmount = %q, want 0", resp.Body.TotalMinorAmount)
		}
		if resp.Body.ItemCount != 0 {
			t.Fatalf("itemCount = %d, want 0", resp.Body.ItemCount)
		}
	})
	if billID == "" {
		t.Fatalf("open bill did not produce a bill ID; later lifecycle assertions cannot run")
	}

	items := []LineItemRequest{
		{
			Reference:   "pay-svc-" + runID + "-001",
			MinorAmount: "1500",
			Currency:    currency,
			FeeType:     "wire_transfer",
			Description: "Outbound USD wire",
		},
		{
			Reference:   "pay-svc-" + runID + "-002",
			MinorAmount: "2500",
			Currency:    currency,
			FeeType:     "monthly_account",
			Description: "Monthly account fee",
		},
		{
			Reference:   "pay-svc-" + runID + "-003",
			MinorAmount: "-250",
			Currency:    currency,
			FeeType:     "correction",
			Description: "Fee correction credit",
		},
	}
	expectedTotal := "3750"

	t.Run("add distinct items", func(t *testing.T) {
		for _, item := range items {
			resp, err := client.AddLineItem(ctx, billID, item)
			requireNoClientError(t, err)
			requireStatus(t, resp, http.StatusCreated)
			if resp.Body == nil {
				t.Fatal("expected line item result body")
			}
			if resp.Body.Reference != item.Reference {
				t.Fatalf("reference = %q, want %q", resp.Body.Reference, item.Reference)
			}
			if !resp.Body.Applied {
				t.Fatalf("applied = false for fresh reference %q, want true", item.Reference)
			}
		}
	})

	t.Run("get running total", func(t *testing.T) {
		resp, err := client.GetBill(ctx, billID, false)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil {
			t.Fatal("expected bill body")
		}
		if resp.Body.TotalMinorAmount != expectedTotal {
			t.Fatalf("totalMinorAmount = %q, want %s", resp.Body.TotalMinorAmount, expectedTotal)
		}
		if resp.Body.ItemCount != len(items) {
			t.Fatalf("itemCount = %d, want %d", resp.Body.ItemCount, len(items))
		}
	})

	t.Run("duplicate item is idempotent", func(t *testing.T) {
		resp, err := client.AddLineItem(ctx, billID, items[1])
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil {
			t.Fatal("expected line item result body")
		}
		if resp.Body.Applied {
			t.Fatal("applied = true for duplicate reference, want false")
		}

		getResp, err := client.GetBill(ctx, billID, false)
		requireNoClientError(t, err)
		requireStatus(t, getResp, http.StatusOK)
		if getResp.Body.TotalMinorAmount != expectedTotal {
			t.Fatalf("total changed after duplicate: got %q, want %s", getResp.Body.TotalMinorAmount, expectedTotal)
		}
	})

	t.Run("mismatched currency is rejected", func(t *testing.T) {
		item := items[0]
		item.Reference = "pay-svc-" + runID + "-mismatch"
		item.Currency = "GEL"
		resp, err := client.AddLineItem(ctx, billID, item)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusBadRequest)
	})

	var closedBody BillResource
	t.Run("close bill returns invoice body", func(t *testing.T) {
		resp, err := client.CloseBill(ctx, billID, CloseBillRequest{Reason: "e2e-explicit-close"})
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil {
			t.Fatal("expected closed bill body")
		}
		closedBody = *resp.Body
		if closedBody.Status != "CLOSED" {
			t.Fatalf("status = %q, want CLOSED", closedBody.Status)
		}
		if closedBody.TotalMinorAmount != expectedTotal {
			t.Fatalf("totalMinorAmount = %q, want %s", closedBody.TotalMinorAmount, expectedTotal)
		}
		if len(closedBody.LineItems) != len(items) {
			t.Fatalf("lineItems length = %d, want %d", len(closedBody.LineItems), len(items))
		}
	})

	t.Run("add after close is rejected", func(t *testing.T) {
		item := items[0]
		item.Reference = "pay-svc-" + runID + "-after-close"
		resp, err := client.AddLineItem(ctx, billID, item)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusConflict)
	})

	t.Run("re-close is idempotent", func(t *testing.T) {
		resp, err := client.CloseBill(ctx, billID, CloseBillRequest{Reason: "e2e-reclose"})
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil {
			t.Fatal("expected closed bill body")
		}
		if !sameInvoiceFacts(*resp.Body, closedBody) {
			got, _ := json.MarshalIndent(resp.Body, "", "  ")
			want, _ := json.MarshalIndent(closedBody, "", "  ")
			t.Fatalf("re-close invoice facts changed\ngot:  %s\nwant: %s", got, want)
		}
	})

	t.Run("get with line items and list find the bill", func(t *testing.T) {
		var gotItems []LineItemResource
		cursor := ""
		for page := 1; page <= 2; page++ {
			getResp, err := client.GetBillPage(ctx, billID, GetBillParams{
				IncludeLineItems: true,
				Cursor:           cursor,
				Limit:            2,
			})
			requireNoClientError(t, err)
			requireStatus(t, getResp, http.StatusOK)
			if getResp.Body == nil {
				t.Fatalf("page %d expected bill body", page)
			}
			gotItems = append(gotItems, getResp.Body.LineItems...)

			wantHasMore := page == 1
			if getResp.Body.HasMore != wantHasMore {
				t.Fatalf("page %d hasMore = %v, want %v", page, getResp.Body.HasMore, wantHasMore)
			}
			if wantHasMore && getResp.Body.NextCursor == "" {
				t.Fatalf("page %d nextCursor is empty, want cursor", page)
			}
			if !wantHasMore && getResp.Body.NextCursor != "" {
				t.Fatalf("page %d nextCursor = %q, want empty", page, getResp.Body.NextCursor)
			}
			cursor = getResp.Body.NextCursor
		}
		if len(gotItems) != len(items) {
			t.Fatalf("GET lineItems length = %d, want %d", len(gotItems), len(items))
		}

		listResp, err := client.ListBills(ctx, ListBillsParams{
			ClientID: clientID,
			Status:   "CLOSED",
			Currency: currency,
			Period:   period,
			Limit:    50,
		})
		requireNoClientError(t, err)
		requireStatus(t, listResp, http.StatusOK)
		if listResp.Body == nil {
			t.Fatal("expected list body")
		}
		if !containsBillWithTotal(listResp.Body.Bills, billID, expectedTotal) {
			t.Fatalf("list response did not include bill %q with total %s: %#v", billID, expectedTotal, listResp.Body.Bills)
		}

		openResp, err := client.ListBills(ctx, ListBillsParams{
			ClientID: clientID,
			Status:   "OPEN",
			Currency: currency,
			Period:   period,
			Limit:    50,
		})
		requireNoClientError(t, err)
		requireStatus(t, openResp, http.StatusOK)
		if openResp.Body == nil {
			t.Fatal("expected open-filter list body")
		}
		for _, bill := range openResp.Body.Bills {
			if bill.BillID == billID {
				t.Fatalf("closed bill %q appeared in status=OPEN list; status filter was not applied", billID)
			}
		}
	})
}

func TestListBillsFilteringAndPaginationE2E(t *testing.T) {
	if os.Getenv("PAVEBANK_E2E") != "1" {
		t.Skip("set PAVEBANK_E2E=1 with temporal server start-dev and encore run already running")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	client := NewClient("")
	preflight(t, ctx, client)

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	clientA := "e2e-list-a-" + runID
	clientB := "e2e-list-b-" + runID
	fixtures := []listBillFixture{
		{
			ClientID: clientA,
			Currency: "USD",
			Period:   "2099-01",
			Status:   "CLOSED",
			Items:    []string{"1000", "-100"},
		},
		{
			ClientID: clientA,
			Currency: "USD",
			Period:   "2099-02",
			Status:   "OPEN",
			Items:    []string{"250"},
		},
		{
			ClientID: clientA,
			Currency: "GEL",
			Period:   "2099-01",
			Status:   "OPEN",
			Items:    []string{"700", "300"},
		},
		{
			ClientID: clientA,
			Currency: "GEL",
			Period:   "2099-02",
			Status:   "CLOSED",
		},
		{
			ClientID: clientA,
			Currency: "USD",
			Period:   "2099-03",
			Status:   "CLOSED",
			Items:    []string{"5000"},
		},
		{
			ClientID: clientA,
			Currency: "GEL",
			Period:   "2099-03",
			Status:   "OPEN",
			Items:    []string{"-50", "200", "25"},
		},
		{
			ClientID: clientB,
			Currency: "USD",
			Period:   "2099-01",
			Status:   "OPEN",
			Items:    []string{"333"},
		},
		{
			ClientID: clientB,
			Currency: "GEL",
			Period:   "2099-02",
			Status:   "CLOSED",
			Items:    []string{"444", "56"},
		},
	}

	expected := make(map[string]BillResource, len(fixtures))
	for i := range fixtures {
		createListBillFixture(t, ctx, client, runID, &fixtures[i])
		expected[fixtures[i].BillID] = BillResource{
			BillID:           fixtures[i].BillID,
			ClientID:         fixtures[i].ClientID,
			Currency:         fixtures[i].Currency,
			Period:           fixtures[i].Period,
			Status:           fixtures[i].Status,
			TotalMinorAmount: fixtures[i].TotalMinorAmount,
			ItemCount:        len(fixtures[i].Items),
		}
	}

	t.Run("filter combinations", func(t *testing.T) {
		tests := []struct {
			name   string
			params ListBillsParams
			want   []string
		}{
			{
				name:   "client A",
				params: ListBillsParams{ClientID: clientA, Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "", "", ""),
			},
			{
				name:   "client A open",
				params: ListBillsParams{ClientID: clientA, Status: "OPEN", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "OPEN", "", ""),
			},
			{
				name:   "client A closed",
				params: ListBillsParams{ClientID: clientA, Status: "CLOSED", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "CLOSED", "", ""),
			},
			{
				name:   "client A USD",
				params: ListBillsParams{ClientID: clientA, Currency: "USD", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "", "USD", ""),
			},
			{
				name:   "client A open GEL",
				params: ListBillsParams{ClientID: clientA, Status: "OPEN", Currency: "GEL", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "OPEN", "GEL", ""),
			},
			{
				name:   "client A period",
				params: ListBillsParams{ClientID: clientA, Period: "2099-01", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "", "", "2099-01"),
			},
			{
				name:   "client A closed USD period",
				params: ListBillsParams{ClientID: clientA, Status: "CLOSED", Currency: "USD", Period: "2099-03", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientA, "CLOSED", "USD", "2099-03"),
			},
			{
				name:   "client B USD",
				params: ListBillsParams{ClientID: clientB, Currency: "USD", Limit: 50},
				want:   billIDsForFixtures(fixtures, clientB, "", "USD", ""),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := client.ListBills(ctx, tt.params)
				requireNoClientError(t, err)
				requireStatus(t, resp, http.StatusOK)
				if resp.Body == nil {
					t.Fatal("expected list body")
				}
				assertListBillsExact(t, resp.Body.Bills, tt.want, expected)
			})
		}
	})

	t.Run("cursor pagination", func(t *testing.T) {
		var all []BillResource
		cursor := ""
		for page := 1; page <= 3; page++ {
			resp, err := client.ListBills(ctx, ListBillsParams{
				ClientID: clientA,
				Cursor:   cursor,
				Limit:    2,
			})
			requireNoClientError(t, err)
			requireStatus(t, resp, http.StatusOK)
			if resp.Body == nil {
				t.Fatalf("page %d expected list body", page)
			}
			if len(resp.Body.Bills) != 2 {
				t.Fatalf("page %d bills length = %d, want 2: %#v", page, len(resp.Body.Bills), resp.Body.Bills)
			}

			wantHasMore := page < 3
			if resp.Body.HasMore != wantHasMore {
				t.Fatalf("page %d hasMore = %v, want %v", page, resp.Body.HasMore, wantHasMore)
			}
			if wantHasMore && resp.Body.NextCursor == "" {
				t.Fatalf("page %d nextCursor is empty, want cursor", page)
			}
			if !wantHasMore && resp.Body.NextCursor != "" {
				t.Fatalf("page %d nextCursor = %q, want empty", page, resp.Body.NextCursor)
			}

			all = append(all, resp.Body.Bills...)
			cursor = resp.Body.NextCursor
		}
		assertListBillsExact(t, all, billIDsForFixtures(fixtures, clientA, "", "", ""), expected)
	})
}

func preflight(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	resp, err := client.ListBills(ctx, ListBillsParams{})
	if err != nil {
		t.Fatalf("E2E preflight failed: could not reach Fees API at %s. Start Temporal with `temporal server start-dev`, start the app with `encore run`, then rerun with PAVEBANK_E2E=1: %v",
			client.BaseURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("E2E preflight failed: GET /v1/bills returned status %d from %s, want 200. Body: %s",
			resp.StatusCode, client.BaseURL, string(resp.RawBody))
	}
}

func requireNoClientError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("client error: %v", err)
	}
}

func requireStatus[T any](t *testing.T, resp *Response[T], want int) {
	t.Helper()
	if resp == nil {
		t.Fatalf("nil response, want status %d", want)
	}
	if resp.StatusCode == want {
		return
	}

	detail := strings.TrimSpace(string(resp.RawBody))
	if resp.Problem != nil {
		detail = fmt.Sprintf("error code=%q type=%q message=%q",
			resp.Problem.Code, resp.Problem.Details.Type, resp.Problem.Message)
	}
	t.Fatalf("status = %d, want %d. %s", resp.StatusCode, want, detail)
}

func requireLocationForBill(t *testing.T, got, wantBillID string) {
	t.Helper()
	if got == "" {
		t.Fatalf("Location header missing, want /v1/bills/%s", wantBillID)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("Location %q not parseable: %v", got, err)
	}
	want := "/v1/bills/" + wantBillID
	if u.Path != want {
		t.Fatalf("Location path = %q, want %q (raw header = %q)", u.Path, want, got)
	}
}

func sameInvoiceFacts(got, want BillResource) bool {
	return got.BillID == want.BillID &&
		got.Status == want.Status &&
		got.Currency == want.Currency &&
		got.TotalMinorAmount == want.TotalMinorAmount &&
		got.ItemCount == want.ItemCount &&
		equalStringPtr(got.ClosedAt, want.ClosedAt) &&
		sameLineItemsByReference(got.LineItems, want.LineItems)
}

func equalStringPtr(got, want *string) bool {
	if got == nil || want == nil {
		return got == want
	}
	return *got == *want
}

func sameLineItemsByReference(got, want []LineItemResource) bool {
	if len(got) != len(want) {
		return false
	}
	gotSorted := append([]LineItemResource(nil), got...)
	wantSorted := append([]LineItemResource(nil), want...)
	sort.Slice(gotSorted, func(i, j int) bool {
		return gotSorted[i].Reference < gotSorted[j].Reference
	})
	sort.Slice(wantSorted, func(i, j int) bool {
		return wantSorted[i].Reference < wantSorted[j].Reference
	})
	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			return false
		}
	}
	return true
}

func containsBillWithTotal(bills []BillResource, billID, totalMinorAmount string) bool {
	for _, bill := range bills {
		if bill.BillID == billID && bill.TotalMinorAmount == totalMinorAmount {
			return true
		}
	}
	return false
}

type listBillFixture struct {
	ClientID         string
	Currency         string
	Period           string
	Status           string
	Items            []string
	BillID           string
	TotalMinorAmount string
}

func createListBillFixture(t *testing.T, ctx context.Context, client *Client, runID string, fixture *listBillFixture) {
	t.Helper()

	resp, err := client.OpenBill(ctx, OpenBillRequest{
		ClientID: fixture.ClientID,
		Currency: fixture.Currency,
		Period:   fixture.Period,
	})
	requireNoClientError(t, err)
	requireStatus(t, resp, http.StatusCreated)
	if resp.Body == nil {
		t.Fatal("expected opened bill body")
	}
	fixture.BillID = resp.Body.BillID
	fixture.TotalMinorAmount = "0"

	var total int64
	for i, amount := range fixture.Items {
		item := LineItemRequest{
			Reference:   fmt.Sprintf("list-%s-%s-%02d", runID, fixture.BillID, i+1),
			MinorAmount: amount,
			Currency:    fixture.Currency,
			FeeType:     "e2e_list",
			Description: "ListBills E2E fixture item",
		}
		itemResp, err := client.AddLineItem(ctx, fixture.BillID, item)
		requireNoClientError(t, err)
		requireStatus(t, itemResp, http.StatusCreated)
		if itemResp.Body == nil {
			t.Fatal("expected line item result body")
		}
		if !itemResp.Body.Applied {
			t.Fatalf("fresh fixture item %q was not applied", item.Reference)
		}

		parsed, err := strconv.ParseInt(amount, 10, 64)
		if err != nil {
			t.Fatalf("invalid fixture amount %q: %v", amount, err)
		}
		total += parsed
	}
	fixture.TotalMinorAmount = strconv.FormatInt(total, 10)

	if fixture.Status == "CLOSED" {
		closeResp, err := client.CloseBill(ctx, fixture.BillID, CloseBillRequest{Reason: "e2e-list-fixture-close"})
		requireNoClientError(t, err)
		requireStatus(t, closeResp, http.StatusOK)
		if closeResp.Body == nil {
			t.Fatal("expected closed bill body")
		}
		if closeResp.Body.Status != "CLOSED" {
			t.Fatalf("closed fixture status = %q, want CLOSED", closeResp.Body.Status)
		}
	}
}

func billIDsForFixtures(fixtures []listBillFixture, clientID, status, currency, period string) []string {
	var ids []string
	for _, fixture := range fixtures {
		if clientID != "" && fixture.ClientID != clientID {
			continue
		}
		if status != "" && fixture.Status != status {
			continue
		}
		if currency != "" && fixture.Currency != currency {
			continue
		}
		if period != "" && fixture.Period != period {
			continue
		}
		ids = append(ids, fixture.BillID)
	}
	return ids
}

func assertListBillsExact(t *testing.T, got []BillResource, wantIDs []string, expected map[string]BillResource) {
	t.Helper()

	gotByID := make(map[string]BillResource, len(got))
	for _, bill := range got {
		if _, exists := gotByID[bill.BillID]; exists {
			t.Fatalf("bill %q appeared more than once in list response: %#v", bill.BillID, got)
		}
		gotByID[bill.BillID] = bill
		if len(bill.LineItems) != 0 {
			t.Fatalf("list response included lineItems for bill %q: %#v", bill.BillID, bill.LineItems)
		}
	}

	if len(gotByID) != len(wantIDs) {
		t.Fatalf("listed bill count = %d, want %d; got=%v want=%v", len(gotByID), len(wantIDs), sortedBillIDs(gotByID), sortedStrings(wantIDs))
	}
	for _, wantID := range wantIDs {
		want, ok := expected[wantID]
		if !ok {
			t.Fatalf("test fixture missing expected facts for bill %q", wantID)
		}
		got, ok := gotByID[wantID]
		if !ok {
			t.Fatalf("list response missing bill %q; got=%v", wantID, sortedBillIDs(gotByID))
		}
		if got.ClientID != want.ClientID ||
			got.Currency != want.Currency ||
			got.Period != want.Period ||
			got.Status != want.Status ||
			got.TotalMinorAmount != want.TotalMinorAmount ||
			got.ItemCount != want.ItemCount {
			t.Fatalf("bill %q facts = %#v, want client=%q currency=%q period=%q status=%q total=%q itemCount=%d",
				wantID, got, want.ClientID, want.Currency, want.Period, want.Status, want.TotalMinorAmount, want.ItemCount)
		}
	}
}

func sortedBillIDs(bills map[string]BillResource) []string {
	ids := make([]string, 0, len(bills))
	for id := range bills {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
