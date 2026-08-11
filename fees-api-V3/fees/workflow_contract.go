package fees

import (
	"time"

	"encore.app/internal/feesworkflowcontract"
)

const (
	BillWorkflowName = feesworkflowcontract.BillWorkflowName
	SignalCloseBill  = feesworkflowcontract.SignalCloseBill
	QueryGetBill     = feesworkflowcontract.QueryGetBill

	OPEN     = feesworkflowcontract.OPEN
	DRAINING = feesworkflowcontract.DRAINING
	CLOSING  = feesworkflowcontract.CLOSING
	CLOSED   = feesworkflowcontract.CLOSED
)

type BillStatus = feesworkflowcontract.BillStatus
type BillInput = feesworkflowcontract.BillInput
type CloseSignal = feesworkflowcontract.CloseSignal
type BillView = feesworkflowcontract.BillView

func billID(clientID, currency, period string) string {
	return feesworkflowcontract.BillID(clientID, currency, period)
}

func resolvePeriodEnd(period string) time.Time {
	return feesworkflowcontract.ResolvePeriodEnd(period)
}
