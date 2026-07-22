package fees

import (
	"fmt"
	"time"
)

func billID(clientID, currency, period string) string {
	return fmt.Sprintf("bill-%s-%s-%s", clientID, currency, period)
}

func resolvePeriodEnd(period string) time.Time {
	start, err := time.ParseInLocation("2006-01", period, time.UTC)
	if err != nil {
		panic(fmt.Sprintf("invalid period identifier %q: %v", period, err))
	}
	return start.AddDate(0, 1, 0)
}

func newBillState(in BillInput) *BillState {
	status := OPEN
	if in.CarriedStatus != 0 {
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
		CarriedStatus: s.status,
	}
}

func ledgerRow(s *BillState, li LineItem) LedgerRow {
	return LedgerRow{
		BillID:      billID(s.clientID, s.currency, s.period),
		Reference:   li.Reference,
		AmountMinor: li.AmountMinor,
		Currency:    li.Currency,
		FeeType:     li.FeeType,
		Description: li.Description,
	}
}
