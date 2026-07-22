package fees

type BillStatus int

const (
	OPEN BillStatus = iota
	DRAINING
	CLOSING
	CLOSED
)

func (s BillStatus) String() string {
	switch s {
	case OPEN:
		return "OPEN"
	case DRAINING:
		return "DRAINING"
	case CLOSING:
		return "CLOSING"
	case CLOSED:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

func (s BillStatus) acceptsAccruals() bool {
	return s == OPEN || s == DRAINING
}

type LineItem struct {
	Reference   string
	AmountMinor int64
	Currency    string
	FeeType     string
	Description string
}

type LineItemResult struct {
	Reference string
	Applied   bool
}

type CloseSignal struct {
	Reason string
}

type BillInput struct {
	ClientID      string
	Currency      string
	Period        string
	CarriedStatus BillStatus
}

type BillView struct {
	ClientID string
	Currency string
	Period   string
	Status   string
}

type BillState struct {
	clientID string
	currency string
	period   string
	status   BillStatus
}

type LedgerRow struct {
	BillID      string
	Reference   string
	AmountMinor int64
	Currency    string
	FeeType     string
	Description string
}
