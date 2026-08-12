package feesworkflowcontract

import (
	"fmt"
	"time"
)

const (
	TaskQueue        = "feeworker"
	BillWorkflowName = "BillWorkflow"
	SignalCloseBill  = "closeBill"
	QueryGetBill     = "getBill"
)

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

func (s BillStatus) AcceptsAccruals() bool {
	return s == OPEN
}

type CloseSignal struct {
	Reason string
}

type BillInput struct {
	ClientID      string
	Currency      string
	Period        string
	HasCarry      bool
	CarriedStatus BillStatus
}

type BillView struct {
	ClientID string
	Currency string
	Period   string
	Status   string
}

func BillID(clientID, currency, period string) string {
	return fmt.Sprintf("bill-%s-%s-%s", clientID, currency, period)
}

func ResolvePeriodEnd(period string) time.Time {
	start, err := time.ParseInLocation("2006-01", period, time.UTC)
	if err != nil {
		panic(fmt.Errorf("invalid period identifier %q: %w", period, err))
	}
	return start.AddDate(0, 1, 0)
}
