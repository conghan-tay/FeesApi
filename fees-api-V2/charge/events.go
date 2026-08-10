package charge

import "encore.dev/pubsub"

// LineItemEvent is the event contract emitted after a line-item status request
// has passed the Charge API's validation.
type LineItemEvent struct {
	BillID      string `json:"billId"`
	Reference   string `json:"reference"`
	MinorAmount string `json:"minorAmount"`
	Currency    string `json:"currency"`
	FeeType     string `json:"feeType"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

var UpdateLineItems = pubsub.NewTopic[*LineItemEvent]("update-line-items", pubsub.TopicConfig{
	DeliveryGuarantee: pubsub.AtLeastOnce,
})
