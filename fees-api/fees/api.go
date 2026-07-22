package fees

import (
	"context"
	"time"
)

type BillResource struct {
	BillID           string     `json:"billId"`
	ClientID         string     `json:"clientId"`
	Currency         string     `json:"currency"`
	Period           string     `json:"period"`
	Status           string     `json:"status"`
	TotalMinorAmount string     `json:"total_minor_amount"`
	ItemCount        int        `json:"itemCount"`
	OpenedAt         time.Time  `json:"openedAt"`
	ClosedAt         *time.Time `json:"closedAt"`
}

type ListBillsResponse struct {
	Bills      []BillResource `json:"bills"`
	NextCursor string         `json:"nextCursor"`
	HasMore    bool           `json:"hasMore"`
}

//encore:api public method=GET path=/v1/bills
func (s *Service) ListBills(ctx context.Context) (*ListBillsResponse, error) {
	return &ListBillsResponse{
		Bills:      []BillResource{},
		NextCursor: "",
		HasMore:    false,
	}, nil
}
