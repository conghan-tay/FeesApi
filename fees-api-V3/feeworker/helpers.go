package feeworker

import (
	"time"

	"encore.app/internal/chargecontract"
	"encore.app/internal/feesworkflowcontract"
)

func billID(clientID, currency, period string) string {
	return feesworkflowcontract.BillID(clientID, currency, period)
}

func resolvePeriodEnd(period string) time.Time {
	return feesworkflowcontract.ResolvePeriodEnd(period)
}

func newBillState(in BillInput) *BillState {
	status := OPEN
	if in.HasCarry {
		status = in.CarriedStatus
	}
	return &BillState{
		clientID: in.ClientID,
		currency: in.Currency,
		period:   in.Period,
		status:   status,
	}
}

func (s *BillState) toView() BillView {
	return BillView{
		ClientID: s.clientID,
		Currency: s.currency,
		Period:   s.period,
		Status:   s.status.String(),
	}
}

func (s *BillState) carryForward() BillInput {
	return BillInput{
		ClientID:      s.clientID,
		Currency:      s.currency,
		Period:        s.period,
		HasCarry:      true,
		CarriedStatus: s.status,
	}
}

func ledgerRow(s *BillState, li chargecontract.LineItem) LedgerRow {
	return LedgerRow{
		BillID:      billID(s.clientID, s.currency, s.period),
		Reference:   li.Reference,
		AmountMinor: li.AmountMinor,
		Currency:    li.Currency,
		FeeType:     li.FeeType,
		Description: li.Description,
	}
}
