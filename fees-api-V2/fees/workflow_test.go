package fees

import (
	"context"
	"errors"
	"sync/atomic"
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

	env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(nil).
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

func TestBillWorkflowAddLineItemSignalRunsLongTransaction(t *testing.T) {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	in := testBillInput()
	item := testLineItem("ref-fresh", "USD")
	row := testLedgerRow(in, item)

	publishCall := env.OnActivity(ActivityPublishPending, mock.Anything, row).
		Return(nil).
		Once()
	longRunningCall := env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(nil).
		NotBefore(publishCall).
		Once()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, row).
		Return(nil).
		NotBefore(longRunningCall).
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

func TestBillWorkflowPublishesStatusesForEveryDuplicateSignal(t *testing.T) {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	in := testBillInput()
	item := testLineItem("ref-duplicate-signal", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPublishPending, mock.Anything, row).
		Return(nil).
		Twice()
	env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(nil).
		Twice()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, row).
		Return(nil).
		Twice()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-after-duplicates"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowPublishPendingRetriesFiveTimesThenSkipsPersistence(t *testing.T) {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	in := testBillInput()
	item := testLineItem("ref-publish-failure", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityPublishPending, mock.Anything, row).
		Return(errors.New("charge callback unavailable")).
		Times(5)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-after-publish-failure"})
	}, time.Second)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPublishPending, 5)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 0)
	env.AssertNumberOfCalls(t, ActivityPublishFinalized, 0)
}

func TestBillWorkflowPublishFinalizedRetriesFiveTimesThenAllowsClose(t *testing.T) {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	in := testBillInput()
	item := testLineItem("ref-finalized-failure", "USD")
	row := testLedgerRow(in, item)

	publishCall := env.OnActivity(ActivityPublishPending, mock.Anything, row).
		Return(nil).
		Once()
	longRunningCall := env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(nil).
		NotBefore(publishCall).
		Once()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, row).
		Return(errors.New("charge callback unavailable")).
		NotBefore(longRunningCall).
		Times(5)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-after-finalized-failure"})
	}, time.Second)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 1)
	env.AssertNumberOfCalls(t, ActivityPublishFinalized, 5)
}

func TestBillWorkflowAddLineItemSignalHandlesDuplicate(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-duplicate", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(nil).
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
	env.AssertNumberOfCalls(t, ActivityPublishFinalized, 1)
}

func TestBillWorkflowProcessesSignalsConcurrentlyAndDrainsThroughFinalizedBeforeCompletion(t *testing.T) {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	in := testBillInput()
	first := testLineItem("ref-concurrent-1", "USD")
	second := testLineItem("ref-concurrent-2", "USD")
	firstRow := testLedgerRow(in, first)
	secondRow := testLedgerRow(in, second)

	var longRunningStarted atomic.Int32
	var longRunningCompleted atomic.Int32
	var finalizedStarted atomic.Int32
	var finalizedCompleted atomic.Int32
	var longRunningDrainChecked atomic.Bool
	var longRunningDrainValid atomic.Bool
	var finalizedDrainChecked atomic.Bool
	var finalizedDrainValid atomic.Bool

	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		switch info.ActivityType.Name {
		case ActivityLongRunning:
			longRunningStarted.Add(1)
		case ActivityPublishFinalized:
			finalizedStarted.Add(1)
		}
	})

	env.OnActivity(ActivityPublishPending, mock.Anything, firstRow).
		Return(nil).
		Once()
	env.OnActivity(ActivityPublishPending, mock.Anything, secondRow).
		Return(nil).
		Once()

	firstCall := env.OnActivity(ActivityLongRunning, mock.Anything, firstRow).
		After(time.Hour).
		Run(func(mock.Arguments) {
			longRunningCompleted.Add(1)
		}).
		Return(nil).
		Once()
	secondCall := env.OnActivity(ActivityLongRunning, mock.Anything, secondRow).
		After(time.Hour).
		Run(func(mock.Arguments) {
			longRunningCompleted.Add(1)
		}).
		Return(nil).
		Once()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, firstRow).
		After(time.Hour).
		Run(func(mock.Arguments) {
			finalizedCompleted.Add(1)
		}).
		Return(nil).
		NotBefore(firstCall).
		Once()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, secondRow).
		After(time.Hour).
		Run(func(mock.Arguments) {
			finalizedCompleted.Add(1)
		}).
		Return(nil).
		NotBefore(secondCall).
		Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, first)
		env.SignalWorkflow(chargecontract.SignalAddLineItem, second)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-with-concurrent-signals"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		longRunningDrainChecked.Store(true)
		longRunningDrainValid.Store(
			longRunningStarted.Load() == 2 &&
				longRunningCompleted.Load() == 0 &&
				finalizedStarted.Load() == 0 &&
				!env.IsWorkflowCompleted(),
		)
	}, 30*time.Minute)
	env.RegisterDelayedCallback(func() {
		finalizedDrainChecked.Store(true)
		finalizedDrainValid.Store(
			longRunningCompleted.Load() == 2 &&
				finalizedStarted.Load() == 2 &&
				finalizedCompleted.Load() == 0 &&
				!env.IsWorkflowCompleted(),
		)
	}, 90*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !longRunningDrainChecked.Load() {
		t.Fatal("long-running-drain assertion did not run")
	}
	if !longRunningDrainValid.Load() {
		t.Error("signals did not overlap in long-running work, or workflow completed before it drained")
	}
	if !finalizedDrainChecked.Load() {
		t.Fatal("finalized-drain assertion did not run")
	}
	if !finalizedDrainValid.Load() {
		t.Error("FINALIZED activities were not in flight together, or workflow completed before they drained")
	}
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPublishPending, 2)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 2)
	env.AssertNumberOfCalls(t, ActivityPublishFinalized, 2)
}

func TestBillWorkflowLongRunningFailureDoesNotFailWorkflow(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-rejected-by-ledger", "USD")
	row := testLedgerRow(in, item)

	env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Return(temporal.NewNonRetryableApplicationError("external transaction rejected", "TransactionRejected", nil)).
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
	env.AssertNumberOfCalls(t, ActivityPublishFinalized, 0)
}

func TestBillWorkflowDropsCurrencyMismatchSignal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-mismatch", "GEL"))
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Second)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPublishPending, 0)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 0)
}

func TestBillWorkflowAutoCloseRejectsAddDuringClosing(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityAutoCloseBill, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-after-close", "USD"))
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPublishPending, 0)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 0)
}

func TestBillWorkflowRejectsAddDuringDraining(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	inFlightItem := testLineItem("ref-in-flight", "USD")
	inFlightRow := testLedgerRow(in, inFlightItem)
	rejectedItem := testLineItem("ref-during-draining", "USD")
	rejectedRow := testLedgerRow(in, rejectedItem)

	env.OnActivity(ActivityLongRunning, mock.Anything, inFlightRow).
		After(time.Hour).
		Return(nil).
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
	env.AssertNumberOfCalls(t, ActivityPublishPending, 1)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 1)
	env.AssertNotCalled(t, ActivityLongRunning, mock.Anything, rejectedRow)
}

func TestBillWorkflowExplicitCloseCompletesWithoutAutoCloseActivity(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "explicit-close"})
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityAutoCloseBill, 0)
}

func TestBillWorkflowAutoCloseTimerSealsBill(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityAutoCloseBill, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(nil).
		Once()

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAutoCloseAtPeriodEndSealsImmediately(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(resolvePeriodEnd(in.Period))

	env.OnActivity(ActivityAutoCloseBill, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(nil).
		Once()

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
}

func TestBillWorkflowAutoCloseRejectsStragglerDuringSeal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityAutoCloseBill, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, testLineItem("ref-auto-close-straggler", "USD"))
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPublishPending, 0)
	env.AssertNumberOfCalls(t, ActivityLongRunning, 0)
}

func TestBillWorkflowAutoCloseRetriesAndStaysClosingUntilSealSucceeds(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	id := billID(in.ClientID, in.Currency, in.Period)
	env.SetStartTime(time.Date(2099, 1, 31, 23, 59, 0, 0, time.UTC))

	env.OnActivity(ActivityAutoCloseBill, mock.Anything, id).
		After(time.Hour).
		Return(errors.New("seal endpoint unavailable")).
		Twice()
	env.OnActivity(ActivityAutoCloseBill, mock.Anything, id).
		Return(nil).
		Once()

	checkedClosing := false
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(QueryGetBill)
		if err != nil {
			t.Errorf("QueryWorkflow during auto-close retry returned error: %v", err)
			return
		}
		var got BillView
		if err := value.Get(&got); err != nil {
			t.Errorf("decode BillView during auto-close retry: %v", err)
			return
		}
		if got.Status != CLOSING.String() {
			t.Errorf("status during auto-close retry = %q, want CLOSING", got.Status)
			return
		}
		checkedClosing = true
	}, 30*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !checkedClosing {
		t.Fatal("CLOSING query assertion did not run")
	}
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityAutoCloseBill, 3)
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

	env.OnActivity(ActivityLongRunning, mock.Anything, row).
		Run(func(mock.Arguments) {
			env.SetContinueAsNewSuggested(true)
		}).
		Return(nil).
		Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chargecontract.SignalAddLineItem, item)
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)

	assertContinueAsNewCarriesStatus(t, env, in)
	env.AssertExpectations(t)
}

func newBillWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	env := newBillWorkflowTestEnvWithoutDefaultPublisher()
	env.OnActivity(ActivityPublishPending, mock.Anything, mock.Anything).
		Return(nil).
		Maybe()
	env.OnActivity(ActivityPublishFinalized, mock.Anything, mock.Anything).
		Return(nil).
		Maybe()
	return env
}

func newBillWorkflowTestEnvWithoutDefaultPublisher() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(mockPublishPendingActivity, activity.RegisterOptions{Name: ActivityPublishPending})
	env.RegisterActivityWithOptions(mockPublishFinalizedActivity, activity.RegisterOptions{Name: ActivityPublishFinalized})
	env.RegisterActivityWithOptions(mockLongRunningActivity, activity.RegisterOptions{Name: ActivityLongRunning})
	env.RegisterActivityWithOptions(mockAutoCloseBillActivity, activity.RegisterOptions{Name: ActivityAutoCloseBill})
	return env
}

func mockPublishPendingActivity(context.Context, LedgerRow) error {
	panic("mockPublishPendingActivity should be mocked in workflow tests")
}

func mockPublishFinalizedActivity(context.Context, LedgerRow) error {
	panic("mockPublishFinalizedActivity should be mocked in workflow tests")
}

func mockLongRunningActivity(context.Context, LedgerRow) error {
	panic("mockLongRunningActivity should be mocked in workflow tests")
}

func mockAutoCloseBillActivity(context.Context, string) error {
	panic("mockAutoCloseBillActivity should be mocked in workflow tests")
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

func assertBillWorkflowCompleted(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow completed with error: %v", err)
	}
}
