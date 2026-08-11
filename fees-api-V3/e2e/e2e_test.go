package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
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
			requireStatus(t, resp, http.StatusAccepted)
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
		resp := waitForBillFacts(t, ctx, client, billID, expectedTotal, len(items))
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
		requireStatus(t, resp, http.StatusAccepted)
		if resp.Body == nil {
			t.Fatal("expected line item result body")
		}
		if !resp.Body.Applied {
			t.Fatal("applied = false for accepted duplicate signal, want true")
		}

		getResp, err := client.GetBill(ctx, billID, false)
		requireNoClientError(t, err)
		requireStatus(t, getResp, http.StatusOK)
		if getResp.Body.TotalMinorAmount != expectedTotal {
			t.Fatalf("total changed after duplicate: got %q, want %s", getResp.Body.TotalMinorAmount, expectedTotal)
		}
	})

	t.Run("mismatched currency signal is accepted asynchronously", func(t *testing.T) {
		item := items[0]
		item.Reference = "pay-svc-" + runID + "-mismatch"
		item.Currency = "GEL"
		resp, err := client.AddLineItem(ctx, billID, item)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusAccepted)
		if resp.Body == nil || !resp.Body.Applied {
			t.Fatalf("mismatched currency response = %#v, want accepted=true", resp.Body)
		}
	})

	t.Run("close bill returns success", func(t *testing.T) {
		resp, err := client.CloseBill(ctx, billID, CloseBillRequest{Reason: "e2e-explicit-close"})
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil || !resp.Body.Success {
			t.Fatalf("close response = %#v, want success=true", resp.Body)
		}
	})

	t.Run("add after close is unavailable", func(t *testing.T) {
		item := items[0]
		item.Reference = "pay-svc-" + runID + "-after-close"
		resp, err := client.AddLineItem(ctx, billID, item)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusServiceUnavailable)
	})

	t.Run("re-close is idempotent", func(t *testing.T) {
		resp, err := client.CloseBill(ctx, billID, CloseBillRequest{Reason: "e2e-reclose"})
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		if resp.Body == nil || !resp.Body.Success {
			t.Fatalf("re-close response = %#v, want success=true", resp.Body)
		}
	})

	t.Run("get with line items and list find the bill", func(t *testing.T) {
		getResp := waitForBillFacts(t, ctx, client, billID, expectedTotal, len(items))
		if getResp.Body == nil {
			t.Fatal("expected bill body")
		}
		if getResp.Body.Status != "CLOSED" {
			t.Fatalf("GET status = %q, want CLOSED", getResp.Body.Status)
		}
		if len(getResp.Body.LineItems) != len(items) {
			t.Fatalf("GET lineItems length = %d, want %d", len(getResp.Body.LineItems), len(items))
		}
		for _, item := range getResp.Body.LineItems {
			if item.Status != "FINALIZED" {
				t.Fatalf("GET line item %s status = %q, want FINALIZED", item.Reference, item.Status)
			}
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

func waitForBillFacts(
	t *testing.T,
	ctx context.Context,
	client *Client,
	billID string,
	wantTotal string,
	wantCount int,
) *Response[BillResource] {
	t.Helper()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var last *Response[BillResource]
	for {
		resp, err := client.GetBill(ctx, billID, true)
		requireNoClientError(t, err)
		requireStatus(t, resp, http.StatusOK)
		last = resp
		if resp.Body != nil &&
			resp.Body.TotalMinorAmount == wantTotal &&
			resp.Body.ItemCount == wantCount &&
			len(resp.Body.LineItems) == wantCount &&
			allLineItemsHaveStatus(resp.Body.LineItems, "FINALIZED") {
			return resp
		}

		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for bill facts: %v", ctx.Err())
		case <-deadline.C:
			if last == nil || last.Body == nil {
				t.Fatal("timed out waiting for accepted line-item signals to become visible; no bill body received")
			}
			t.Fatalf(
				"timed out waiting for bill facts: total=%q count=%d, want total=%q count=%d",
				last.Body.TotalMinorAmount,
				last.Body.ItemCount,
				wantTotal,
				wantCount,
			)
		case <-ticker.C:
		}
	}
}

func allLineItemsHaveStatus(items []LineItemResource, status string) bool {
	for _, item := range items {
		if item.Status != status {
			return false
		}
	}
	return true
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

func containsBillWithTotal(bills []BillResource, billID, totalMinorAmount string) bool {
	for _, bill := range bills {
		if bill.BillID == billID && bill.TotalMinorAmount == totalMinorAmount {
			return true
		}
	}
	return false
}
