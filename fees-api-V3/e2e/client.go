package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "http://localhost:4000"

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = os.Getenv("PAVEBANK_API_BASE_URL")
	}
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

type OpenBillRequest struct {
	ClientID string `json:"clientId"`
	Currency string `json:"currency"`
	Period   string `json:"period"`
}

type LineItemRequest struct {
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
}

type CloseBillRequest struct {
	Reason string `json:"reason"`
}

type CloseBillResponse struct {
	Success bool `json:"success"`
}

type BillResource struct {
	BillID           string             `json:"billId"`
	ClientID         string             `json:"clientId"`
	Currency         string             `json:"currency"`
	Period           string             `json:"period"`
	Status           string             `json:"status"`
	TotalMinorAmount string             `json:"totalMinorAmount"`
	ItemCount        int                `json:"itemCount"`
	OpenedAt         string             `json:"openedAt"`
	ClosedAt         *string            `json:"closedAt"`
	LineItems        []LineItemResource `json:"lineItems,omitempty"`
}

type LineItemResource struct {
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
	Status      string `json:"status"`
	AppliedAt   string `json:"appliedAt"`
}

type LineItemResult struct {
	Reference string `json:"reference"`
	Applied   bool   `json:"applied"`
}

type ListBillsParams struct {
	ClientID string
	Status   string
	Currency string
	Period   string
	Cursor   string
	Limit    int
}

type ListBillsResponse struct {
	Bills      []BillResource `json:"bills"`
	NextCursor string         `json:"nextCursor"`
	HasMore    bool           `json:"hasMore"`
}

type Problem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details struct {
		Type string `json:"type"`
	} `json:"details"`
}

type Response[T any] struct {
	StatusCode int
	Header     http.Header
	Body       *T
	Problem    *Problem
	RawBody    []byte
}

func (c *Client) OpenBill(ctx context.Context, req OpenBillRequest) (*Response[BillResource], error) {
	return do[BillResource](ctx, c, http.MethodPost, "/v1/bills", req)
}

func (c *Client) AddLineItem(ctx context.Context, billID string, req LineItemRequest) (*Response[LineItemResult], error) {
	return do[LineItemResult](ctx, c, http.MethodPost, "/v1/bills/"+url.PathEscape(billID)+"/line-items", req)
}

func (c *Client) CloseBill(ctx context.Context, billID string, req CloseBillRequest) (*Response[CloseBillResponse], error) {
	return do[CloseBillResponse](ctx, c, http.MethodPost, "/v1/bills/"+url.PathEscape(billID)+"/close", req)
}

func (c *Client) GetBill(ctx context.Context, billID string, includeLineItems bool) (*Response[BillResource], error) {
	path := "/v1/bills/" + url.PathEscape(billID)
	if includeLineItems {
		path += "?includeLineItems=true"
	}
	return do[BillResource](ctx, c, http.MethodGet, path, nil)
}

func (c *Client) ListBills(ctx context.Context, params ListBillsParams) (*Response[ListBillsResponse], error) {
	values := url.Values{}
	if params.ClientID != "" {
		values.Set("clientId", params.ClientID)
	}
	if params.Status != "" {
		values.Set("status", params.Status)
	}
	if params.Currency != "" {
		values.Set("currency", params.Currency)
	}
	if params.Period != "" {
		values.Set("period", params.Period)
	}
	if params.Cursor != "" {
		values.Set("cursor", params.Cursor)
	}
	if params.Limit > 0 {
		values.Set("limit", strconv.Itoa(params.Limit))
	}

	path := "/v1/bills"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return do[ListBillsResponse](ctx, c, http.MethodGet, path, nil)
}

func do[T any](ctx context.Context, c *Client, method, path string, body any) (*Response[T], error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal %s %s request: %w", method, path, err)
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, payload)
	if err != nil {
		return nil, fmt.Errorf("build %s %s request: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, c.BaseURL+path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s %s response: %w", method, path, err)
	}

	out := &Response[T]{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		RawBody:    raw,
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return out, nil
	}

	if resp.StatusCode >= http.StatusBadRequest {
		var problem Problem
		if err := json.Unmarshal(raw, &problem); err == nil {
			out.Problem = &problem
		}
		return out, nil
	}

	var decoded T
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode %s %s response status %d: %w: %s",
			method, path, resp.StatusCode, err, string(raw))
	}
	out.Body = &decoded
	return out, nil
}
