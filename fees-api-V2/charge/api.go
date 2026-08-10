package charge

import (
	"context"
	"net/http"
	"regexp"
	"strconv"

	"encore.app/internal/chargecontract"
	"encore.dev/beta/errs"
	"encore.dev/rlog"
)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

const (
	LineItemStatusPending   = "PENDING"
	LineItemStatusFinalized = "FINALIZED"
	LineItemStatusFailed    = "FAILED"
)

type AddLineItemRequest struct {
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
}

type AddLineItemResponse struct {
	Reference  string `json:"reference"`
	Applied    bool   `json:"applied"`
	HTTPStatus int    `encore:"httpstatus"`
}

type PublishLineItemStatusRequest struct {
	BillID      string `json:"billId"`
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

type APIErrorDetails struct {
	Type string `json:"type"`
}

func (APIErrorDetails) ErrDetails() {}

//encore:api public method=POST path=/v1/bills/:billId/line-items
func (s *Service) AddLineItem(ctx context.Context, billId string, req *AddLineItemRequest) (*AddLineItemResponse, error) {
	if billId == "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "billId is required")
	}
	if req == nil {
		return nil, apiError(errs.InvalidArgument, "invalid-request", "request body is required")
	}

	lineItem, validationErr := validateAddLineItemRequest(*req)
	if validationErr != "" {
		return nil, apiError(errs.InvalidArgument, "invalid-request", validationErr)
	}
	if s == nil || s.temporalClient == nil {
		return nil, addLineItemUnavailable()
	}

	if err := s.temporalClient.SignalWorkflow(
		ctx,
		billId,
		"",
		chargecontract.SignalAddLineItem,
		lineItem,
	); err != nil {
		rlog.Error(
			"add line item: temporal signal failed",
			"billID", billId,
			"reference", lineItem.Reference,
			"problemType", "add-line-item-unavailable",
			"err", err,
		)
		return nil, addLineItemUnavailable()
	}

	return &AddLineItemResponse{
		Reference:  lineItem.Reference,
		Applied:    true,
		HTTPStatus: http.StatusAccepted,
	}, nil
}

//encore:api public method=POST path=/v1/line-item-status
func (s *Service) PublishLineItemStatus(ctx context.Context, req *PublishLineItemStatusRequest) error {
	if req == nil {
		return apiError(errs.InvalidArgument, "invalid-request", "request body is required")
	}
	if validationErr := validatePublishLineItemStatusRequest(*req); validationErr != "" {
		return apiError(errs.InvalidArgument, "invalid-request", validationErr)
	}
	if s == nil || s.lineItemEvents == nil {
		return lineItemStatusUnavailable()
	}

	messageID, err := s.lineItemEvents.Publish(ctx, &LineItemEvent{
		BillID:      req.BillID,
		Reference:   req.Reference,
		MinorAmount: req.MinorAmount,
		Currency:    req.Currency,
		FeeType:     req.FeeType,
		Description: req.Description,
		Status:      req.Status,
	})
	if err != nil {
		rlog.Error(
			"publish line item status: pubsub publish failed",
			"billID", req.BillID,
			"reference", req.Reference,
			"status", req.Status,
			"problemType", "line-item-status-unavailable",
			"err", err,
		)
		return lineItemStatusUnavailable()
	}

	rlog.Info(
		"publish line item status: event published",
		"billID", req.BillID,
		"reference", req.Reference,
		"status", req.Status,
		"messageID", messageID,
	)
	return nil
}

func validateAddLineItemRequest(input AddLineItemRequest) (chargecontract.LineItem, string) {
	if input.Reference == "" {
		return chargecontract.LineItem{}, "reference is required"
	}
	if input.MinorAmount == "" {
		return chargecontract.LineItem{}, "minorAmount is required"
	}
	amountMinor, err := strconv.ParseInt(input.MinorAmount, 10, 64)
	if err != nil {
		return chargecontract.LineItem{}, "minorAmount must be an integer minor-unit amount encoded as a string"
	}
	if !currencyPattern.MatchString(input.Currency) {
		return chargecontract.LineItem{}, "currency must be a three-letter uppercase ISO-4217 code"
	}
	if input.FeeType == "" {
		return chargecontract.LineItem{}, "feeType is required"
	}
	return chargecontract.LineItem{
		Reference:   input.Reference,
		AmountMinor: amountMinor,
		Currency:    input.Currency,
		FeeType:     input.FeeType,
		Description: input.Description,
	}, ""
}

func validatePublishLineItemStatusRequest(input PublishLineItemStatusRequest) string {
	if input.BillID == "" {
		return "billId is required"
	}
	if input.Reference == "" {
		return "reference is required"
	}
	if input.MinorAmount == "" {
		return "minorAmount is required"
	}
	if _, err := strconv.ParseInt(input.MinorAmount, 10, 64); err != nil {
		return "minorAmount must be an integer minor-unit amount encoded as a string"
	}
	if !currencyPattern.MatchString(input.Currency) {
		return "currency must be a three-letter uppercase ISO-4217 code"
	}
	if input.FeeType == "" {
		return "feeType is required"
	}
	switch input.Status {
	case LineItemStatusPending, LineItemStatusFinalized, LineItemStatusFailed:
		return ""
	case "":
		return "status is required"
	default:
		return "status must be one of PENDING, FINALIZED, or FAILED"
	}
}

func addLineItemUnavailable() error {
	return apiError(
		errs.Unavailable,
		"add-line-item-unavailable",
		"add-line-item signal was not accepted; retry after a short delay",
	)
}

func lineItemStatusUnavailable() error {
	return apiError(
		errs.Unavailable,
		"line-item-status-unavailable",
		"line item status was not published; retry after a short delay",
	)
}

func apiError(code errs.ErrCode, problemType, message string, metaPairs ...interface{}) error {
	return errs.B().
		Code(code).
		Msg(message).
		Details(APIErrorDetails{Type: problemType}).
		Meta(metaPairs...).
		Err()
}
