package fees

import (
	"context"
	"errors"
	"testing"
	"time"

	"encore.app/internal/chargecontract"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/stretchr/testify/mock"
)

func TestBillWorkflowQueryStartsOpen(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	queried := false
	env.RegisterDelayedCallback(func() {
		defer env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})

		value, err := env.QueryWorkflow(QueryGetBill)
		if err != nil {
			t.Errorf("QueryWorkflow returned error: %v", err)
			return
		}

		var got BillView
		if err := value.Get(&got); err != nil {
			t.Errorf("decode BillView query result: %v", err)
			return
		}

		want := BillView{
			ClientID: in.ClientID,
			Currency: in.Currency,
			Period:   in.Period,
			Status:   "OPEN",
		}
		if got != want {
			t.Errorf("QueryGetBill = %#v, want %#v", got, want)
			return
		}
		queried = true
	}, time.Second)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !queried {
		t.Fatal("QueryGetBill callback did not run")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowHandlesAddLineItemWithoutStartupActivity(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-no-startup-activity", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, 2*time.Hour)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAddLineItemSignalPersistsFreshItem(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-fresh", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAddLineItemSignalHandlesDuplicateNoop(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-duplicate", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(false, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowProcessesSignalsConcurrentlyAndDrainsBeforeSeal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	first := testLineItem("ref-concurrent-1", "USD")
	second := testLineItem("ref-concurrent-2", "USD")
	firstRow := testLedgerRow(in, first)
	secondRow := testLedgerRow(in, second)

	firstCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, firstRow).
		After(time.Hour).
		Return(true, nil).
		Once()
	secondCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, secondRow).
		After(time.Hour).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, firstRow.BillID).
		Return(testClosedBillView(in), nil).
		NotBefore(firstCall, secondCall).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, first)
		env.SignalWorkflow(chargecontract.SignalAddLineItem, second)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-with-concurrent-signals"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowActivityRejectionDoesNotFailWorkflow(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-rejected-by-ledger", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(false, temporal.NewNonRetryableApplicationError("bill not open", "BillNotOpen", nil)).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-after-rejection"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowDropsCurrencyMismatchSignal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-mismatch", "GEL"))
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Second)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 0)
}

func TestBillWorkflowRejectsAddDuringClosing(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-after-close", "USD"))
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 0)
}

func TestBillWorkflowRejectsAddDuringDraining(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	inFlightItem := testLineItem("ref-in-flight", "USD")
	inFlightRow := testLedgerRow(in, inFlightItem)
	rejectedItem := testLineItem("ref-during-draining", "USD")
	rejectedRow := testLedgerRow(in, rejectedItem)

	lineItemCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, inFlightRow).
		After(time.Hour).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		NotBefore(lineItemCall).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, inFlightItem)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, rejectedItem)
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 1)
	env.AssertNotCalled(t, ActivityPersistLineItem, mock.Anything, rejectedRow)
}

func TestBillWorkflowExplicitCloseSealsBill(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "explicit-close"})
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAutoCloseTimerSealsBill(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAutoCloseAtPeriodEndSealsImmediately(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(resolvePeriodEnd(in.Period))

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAutoCloseRejectsStragglerDuringSeal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(testClosedBillView(in), nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-auto-close-straggler", "USD"))
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 0)
}

func TestBillWorkflowDrainsInFlightSignalBeforeSeal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-drain", "USD")
	row := testLedgerRow(in, item)

	lineItemCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		After(time.Hour).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		NotBefore(lineItemCall).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-during-update"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowContinueAsNewCarriesStatus(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetContinueAsNewSuggested(true)

	env.ExecuteWorkflow(BillWorkflow, in)

	assertContinueAsNewCarriesStatus(t, env, in)
}

func assertContinueAsNewCarriesStatus(t *testing.T, env *testsuite.TestWorkflowEnvironment, in BillInput) {
	t.Helper()

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected Continue-As-New workflow error, got nil")
	}
	var workflowErr *temporal.WorkflowExecutionError
	if !errors.As(err, &workflowErr) {
		t.Fatalf("workflow error = %T %v, want WorkflowExecutionError", err, err)
	}

	var continueAsNewErr *sdkworkflow.ContinueAsNewError
	if !errors.As(errors.Unwrap(workflowErr), &continueAsNewErr) {
		t.Fatalf("workflow error cause = %T %v, want ContinueAsNewError", errors.Unwrap(workflowErr), errors.Unwrap(workflowErr))
	}

	var carried BillInput
	if err := converter.GetDefaultDataConverter().FromPayloads(continueAsNewErr.Input, &carried); err != nil {
		t.Fatalf("decode Continue-As-New input: %v", err)
	}

	want := in
	want.HasCarry = true
	want.CarriedStatus = OPEN
	if carried != want {
		t.Fatalf("Continue-As-New input = %#v, want %#v", carried, want)
	}
}

func TestBillWorkflowCANWakesFromSignalActivity(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-can", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Run(func(mock.Arguments) {
			env.SetContinueAsNewSuggested(true)
		}).
		Return(true, nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)

	assertContinueAsNewCarriesStatus(t, env, in)
	env.AssertExpectations(t)
}

func newBillWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(mockPersistLineItemActivity, activity.RegisterOptions{Name: ActivityPersistLineItem})
	env.RegisterActivityWithOptions(mockPersistInvoiceActivity, activity.RegisterOptions{Name: ActivityPersistInvoice})
	return env
}

func mockPersistLineItemActivity(context.Context, LedgerRow) (bool, error) {
	panic("mockPersistLineItemActivity should be mocked in workflow tests")
}

func mockPersistInvoiceActivity(context.Context, string) (BillView, error) {
	panic("mockPersistInvoiceActivity should be mocked in workflow tests")
}

func testBillInput() BillInput {
	return BillInput{
		ClientID: "workflow-acme",
		Currency: "USD",
		Period:   "2099-01",
	}
}

func testLineItem(reference, currency string) chargecontract.LineItem {
	return chargecontract.LineItem{
		Reference:   reference,
		AmountMinor: 1500,
		Currency:    currency,
		FeeType:     "wire_transfer",
		Description: "Outbound wire",
	}
}

func testLedgerRow(in BillInput, item chargecontract.LineItem) LedgerRow {
	return LedgerRow{
		BillID:      billID(in.ClientID, in.Currency, in.Period),
		Reference:   item.Reference,
		AmountMinor: item.AmountMinor,
		Currency:    item.Currency,
		FeeType:     item.FeeType,
		Description: item.Description,
	}
}

func testClosedBillView(in BillInput) BillView {
	return BillView{
		ClientID: in.ClientID,
		Currency: in.Currency,
		Period:   in.Period,
		Status:   "CLOSED",
	}
}

func assertBillWorkflowCompleted(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow completed with error: %v", err)
	}
}
