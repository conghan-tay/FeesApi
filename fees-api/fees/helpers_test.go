package fees

import (
	"errors"
	"testing"
	"time"
)

func TestResolvePeriodEnd(t *testing.T) {
	tests := []struct {
		name   string
		period string
		want   time.Time
	}{
		{
			name:   "normal month rollover",
			period: "2026-07",
			want:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "december year rollover",
			period: "2026-12",
			want:   time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "leap year february",
			period: "2024-02",
			want:   time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePeriodEnd(tt.period)
			if !got.Equal(tt.want) {
				t.Fatalf("resolvePeriodEnd(%q) = %s, want %s", tt.period, got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
			if got.Location() != time.UTC {
				t.Fatalf("resolvePeriodEnd(%q) location = %v, want UTC", tt.period, got.Location())
			}
		})
	}
}

func TestResolvePeriodEndPanicsForMalformedPeriod(t *testing.T) {
	tests := []string{
		"2026-7",
		"2026-13",
		"2026-00",
		"2026-07-01",
		"26-07",
		"",
	}

	for _, period := range tests {
		t.Run(period, func(t *testing.T) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("resolvePeriodEnd(%q) did not panic", period)
				}

				err, ok := recovered.(error)
				if !ok {
					t.Fatalf("panic payload = %#v (%T), want error", recovered, recovered)
				}
				var parseErr *time.ParseError
				if !errors.As(err, &parseErr) {
					t.Fatalf("panic error = %v, want wrapped *time.ParseError", err)
				}
			}()

			resolvePeriodEnd(period)
		})
	}
}

func TestBillID(t *testing.T) {
	got := billID("acme", "USD", "2026-07")
	want := "bill-acme-USD-2026-07"
	if got != want {
		t.Fatalf("billID() = %q, want %q", got, want)
	}
}

func TestBillStatusStringAndAcceptsAccruals(t *testing.T) {
	tests := []struct {
		status  BillStatus
		want    string
		accepts bool
	}{
		{status: OPEN, want: "OPEN", accepts: true},
		{status: DRAINING, want: "DRAINING", accepts: true},
		{status: CLOSING, want: "CLOSING", accepts: false},
		{status: CLOSED, want: "CLOSED", accepts: false},
		{status: BillStatus(99), want: "UNKNOWN", accepts: false},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
			if got := tt.status.acceptsAccruals(); got != tt.accepts {
				t.Fatalf("acceptsAccruals() = %v, want %v", got, tt.accepts)
			}
		})
	}
}

func TestNewBillStateDefaultsToOpen(t *testing.T) {
	state := newBillState(BillInput{
		ClientID: "acme",
		Currency: "USD",
		Period:   "2026-07",
	})

	if state.clientID != "acme" || state.currency != "USD" || state.period != "2026-07" {
		t.Fatalf("state identity = %#v", state)
	}
	if state.status != OPEN {
		t.Fatalf("state status = %v, want OPEN", state.status)
	}
}

func TestNewBillStateIgnoresCarriedStatusWithoutHasCarry(t *testing.T) {
	state := newBillState(BillInput{
		ClientID:      "acme",
		Currency:      "USD",
		Period:        "2026-07",
		CarriedStatus: CLOSED,
	})

	if state.status != OPEN {
		t.Fatalf("state status = %v, want OPEN", state.status)
	}
}

func TestNewBillStateUsesCarriedStatus(t *testing.T) {
	state := newBillState(BillInput{
		ClientID:      "acme",
		Currency:      "USD",
		Period:        "2026-07",
		HasCarry:      true,
		CarriedStatus: DRAINING,
	})

	if state.status != DRAINING {
		t.Fatalf("state status = %v, want DRAINING", state.status)
	}
}

func TestBillStateToView(t *testing.T) {
	state := &BillState{
		clientID: "acme",
		currency: "USD",
		period:   "2026-07",
		status:   CLOSING,
	}

	got := state.toView()
	want := BillView{
		ClientID: "acme",
		Currency: "USD",
		Period:   "2026-07",
		Status:   "CLOSING",
	}
	if got != want {
		t.Fatalf("toView() = %#v, want %#v", got, want)
	}
}

func TestBillStateCarryForward(t *testing.T) {
	state := &BillState{
		clientID: "acme",
		currency: "USD",
		period:   "2026-07",
		status:   DRAINING,
	}

	got := state.carryForward()
	want := BillInput{
		ClientID:      "acme",
		Currency:      "USD",
		Period:        "2026-07",
		HasCarry:      true,
		CarriedStatus: DRAINING,
	}
	if got != want {
		t.Fatalf("carryForward() = %#v, want %#v", got, want)
	}
}

func TestLedgerRow(t *testing.T) {
	state := &BillState{
		clientID: "acme",
		currency: "USD",
		period:   "2026-07",
		status:   OPEN,
	}
	item := LineItem{
		Reference:   "pay-svc-evt-98213",
		AmountMinor: -1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire reversal",
	}

	got := ledgerRow(state, item)
	want := LedgerRow{
		BillID:      "bill-acme-USD-2026-07",
		Reference:   "pay-svc-evt-98213",
		AmountMinor: -1500,
		Currency:    "USD",
		FeeType:     "wire_transfer",
		Description: "Outbound USD wire reversal",
	}
	if got != want {
		t.Fatalf("ledgerRow() = %#v, want %#v", got, want)
	}
}
