package feeworker

import "encore.app/internal/feesworkflowcontract"

type BillStatus = feesworkflowcontract.BillStatus

const (
	BillWorkflowName = feesworkflowcontract.BillWorkflowName
	SignalCloseBill  = feesworkflowcontract.SignalCloseBill
	QueryGetBill     = feesworkflowcontract.QueryGetBill

	OPEN     = feesworkflowcontract.OPEN
	DRAINING = feesworkflowcontract.DRAINING
	CLOSING  = feesworkflowcontract.CLOSING
	CLOSED   = feesworkflowcontract.CLOSED
)

type CloseSignal = feesworkflowcontract.CloseSignal
type BillInput = feesworkflowcontract.BillInput
type BillView = feesworkflowcontract.BillView

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
