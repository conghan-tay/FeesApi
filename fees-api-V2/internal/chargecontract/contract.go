package chargecontract

const SignalAddLineItem = "addLineItem"

// LineItem is the payload sent from the Charge API to BillWorkflow.
// It intentionally has no HTTP serialization tags; the Charge service owns
// the wire contract and translates into this Temporal-boundary type.
type LineItem struct {
	Reference   string
	AmountMinor int64
	Currency    string
	FeeType     string
	Description string
}
