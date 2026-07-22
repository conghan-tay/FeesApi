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
		getResp, err := client.GetBill(ctx, billID, true)
		requireNoClientError(t, err)
		requireStatus(t, getResp, http.StatusOK)
		if getResp.Body == nil {
			t.Fatal("expected bill body")
		}
		if len(getResp.Body.LineItems) != len(items) {
			t.Fatalf("GET lineItems length = %d, want %d", len(getResp.Body.LineItems), len(items))
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
		detail = fmt.Sprintf("problem type=%q title=%q status=%d detail=%q",
			resp.Problem.Type, resp.Problem.Title, resp.Problem.Status, resp.Problem.Detail)
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
