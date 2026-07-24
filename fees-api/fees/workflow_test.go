package fees

import (
	"context"
	"errors"
	"testing"
	"time"

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

	expectPersistBill(env, in)
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

func TestBillWorkflowAwaitOpenCompletesAfterPersistBill(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	persistCompleted := false

	env.OnActivity(ActivityPersistBill, mock.Anything, in).
		After(time.Hour).
		Run(func(mock.Arguments) {
			persistCompleted = true
		}).
		Return(nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAwaitOpen, "update-await-open", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("await-open update rejected: %v", err)
			},
			OnComplete: func(result interface{}, err error) {
				if err != nil {
					t.Errorf("await-open update completed with error: %v", err)
					return
				}
				if !persistCompleted {
					t.Error("await-open completed before ActivityPersistBill")
					return
				}
				got := result.(BillView)
				want := BillView{
					ClientID: in.ClientID,
					Currency: in.Currency,
					Period:   in.Period,
					Status:   "OPEN",
				}
				if got != want {
					t.Errorf("await-open result = %#v, want %#v", got, want)
					return
				}
				updateCompleted = true
			},
		})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, 2*time.Hour)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !updateCompleted {
		t.Fatal("await-open update did not complete")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowBuffersAddLineItemDuringStartup(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-startup-buffered", "USD")
	row := testLedgerRow(in, item)

	persistCall := env.OnActivity(ActivityPersistBill, mock.Anything, in).
		After(time.Hour).
		Return(nil).
		Once()
	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(true, nil).
		NotBefore(persistCall).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-startup-buffered", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("startup add-line-item update rejected: %v", err)
			},
			OnComplete: func(result interface{}, err error) {
				if err != nil {
					t.Errorf("startup add-line-item update completed with error: %v", err)
					return
				}
				got := result.(LineItemResult)
				want := LineItemResult{Reference: item.Reference, Applied: true}
				if got != want {
					t.Errorf("LineItemResult = %#v, want %#v", got, want)
					return
				}
				updateCompleted = true
			},
		}, item)
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, 2*time.Hour)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !updateCompleted {
		t.Fatal("startup add-line-item update did not complete")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowAddLineItemReturnsApplied(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-fresh", "USD")
	row := testLedgerRow(in, item)

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-fresh", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("add-line-item update rejected: %v", err)
			},
			OnComplete: func(result interface{}, err error) {
				if err != nil {
					t.Errorf("add-line-item update completed with error: %v", err)
					return
				}
				got := result.(LineItemResult)
				want := LineItemResult{Reference: item.Reference, Applied: true}
				if got != want {
					t.Errorf("LineItemResult = %#v, want %#v", got, want)
					return
				}
				updateCompleted = true
			},
		}, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !updateCompleted {
		t.Fatal("add-line-item update did not complete")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowAddLineItemReturnsDuplicateNoop(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-duplicate", "USD")
	row := testLedgerRow(in, item)

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Return(false, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-duplicate", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("duplicate update rejected: %v", err)
			},
			OnComplete: func(result interface{}, err error) {
				if err != nil {
					t.Errorf("duplicate update completed with error: %v", err)
					return
				}
				got := result.(LineItemResult)
				want := LineItemResult{Reference: item.Reference, Applied: false}
				if got != want {
					t.Errorf("LineItemResult = %#v, want %#v", got, want)
					return
				}
				updateCompleted = true
			},
		}, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !updateCompleted {
		t.Fatal("duplicate update did not complete")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowRejectsCurrencyMismatchInValidator(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		Once()

	var rejected bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-currency-mismatch", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				assertApplicationErrorType(t, err, "CurrencyMismatch")
				rejected = true
			},
			OnAccept: func() {
				t.Error("currency-mismatch update was accepted, want validator reject")
			},
		}, testLineItem("ref-mismatch", "GEL"))
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !rejected {
		t.Fatal("currency-mismatch update was not rejected")
	}
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 0)
}

func TestBillWorkflowRejectsAddDuringClosing(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(testClosedBillView(in), nil).
		Once()

	var rejected bool
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-after-close", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				assertApplicationErrorType(t, err, "BillNotOpen")
				rejected = true
			},
			OnAccept: func() {
				t.Error("add-after-close update was accepted, want validator reject")
			},
		}, testLineItem("ref-after-close", "USD"))
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !rejected {
		t.Fatal("add-after-close update was not rejected")
	}
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

	expectPersistBill(env, in)
	lineItemCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, inFlightRow).
		After(time.Hour).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		Return(testClosedBillView(in), nil).
		NotBefore(lineItemCall).
		Once()

	var rejected bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-in-flight", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("in-flight update rejected: %v", err)
			},
			OnComplete: func(_ interface{}, err error) {
				if err != nil {
					t.Errorf("in-flight update completed with error: %v", err)
				}
			},
		}, inFlightItem)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "test-close"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-during-draining", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				assertApplicationErrorType(t, err, "BillNotOpen")
				rejected = true
			},
			OnAccept: func() {
				t.Error("add-during-draining update was accepted, want validator reject")
			},
		}, rejectedItem)
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !rejected {
		t.Fatal("add-during-draining update was not rejected")
	}
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 1)
	env.AssertNotCalled(t, ActivityPersistLineItem, mock.Anything, rejectedRow)
}

func TestBillWorkflowExplicitCloseSealsBill(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()

	expectPersistBill(env, in)
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

	expectPersistBill(env, in)
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

	expectPersistBill(env, in)
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

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistInvoice, mock.Anything, billID(in.ClientID, in.Currency, in.Period)).
		After(time.Hour).
		Return(testClosedBillView(in), nil).
		Once()

	var rejected bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-auto-close-straggler", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				assertApplicationErrorType(t, err, "BillNotOpen")
				rejected = true
			},
			OnAccept: func() {
				t.Error("auto-close straggler update was accepted, want validator reject")
			},
		}, testLineItem("ref-auto-close-straggler", "USD"))
	}, 2*time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !rejected {
		t.Fatal("auto-close straggler update was not rejected")
	}
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, ActivityPersistLineItem, 0)
}

func TestBillWorkflowDrainsInFlightUpdateBeforeSeal(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-drain", "USD")
	row := testLedgerRow(in, item)

	expectPersistBill(env, in)
	lineItemCall := env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		After(time.Hour).
		Return(true, nil).
		Once()
	env.OnActivity(ActivityPersistInvoice, mock.Anything, row.BillID).
		Return(testClosedBillView(in), nil).
		NotBefore(lineItemCall).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-drain", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("drain update rejected: %v", err)
			},
			OnComplete: func(_ interface{}, err error) {
				if err != nil {
					t.Errorf("drain update completed with error: %v", err)
					return
				}
				updateCompleted = true
			},
		}, item)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCloseBill, CloseSignal{Reason: "close-during-update"})
	}, time.Minute)

	env.ExecuteWorkflow(BillWorkflow, in)
	assertBillWorkflowCompleted(t, env)
	if !updateCompleted {
		t.Fatal("in-flight update did not complete before workflow close")
	}
	env.AssertExpectations(t)
}

func TestBillWorkflowContinueAsNewCarriesStatus(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	env.SetContinueAsNewSuggested(true)

	expectPersistBill(env, in)
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

func TestBillWorkflowCANWakesFromUpdateHandler(t *testing.T) {
	env := newBillWorkflowTestEnv()
	in := testBillInput()
	item := testLineItem("ref-can", "USD")
	row := testLedgerRow(in, item)

	expectPersistBill(env, in)
	env.OnActivity(ActivityPersistLineItem, mock.Anything, row).
		Run(func(mock.Arguments) {
			env.SetContinueAsNewSuggested(true)
		}).
		Return(true, nil).
		Once()

	var updateCompleted bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, "update-triggers-can", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				t.Errorf("CAN-triggering update rejected: %v", err)
			},
			OnComplete: func(result interface{}, err error) {
				if err != nil {
					t.Errorf("CAN-triggering update completed with error: %v", err)
					return
				}
				got := result.(LineItemResult)
				want := LineItemResult{Reference: item.Reference, Applied: true}
				if got != want {
					t.Errorf("LineItemResult = %#v, want %#v", got, want)
					return
				}
				updateCompleted = true
			},
		}, item)
	}, time.Millisecond)

	env.ExecuteWorkflow(BillWorkflow, in)

	if !updateCompleted {
		t.Fatal("CAN-triggering update did not complete")
	}
	assertContinueAsNewCarriesStatus(t, env, in)
	env.AssertExpectations(t)
}

func newBillWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(mockPersistBillActivity, activity.RegisterOptions{Name: ActivityPersistBill})
	env.RegisterActivityWithOptions(mockPersistLineItemActivity, activity.RegisterOptions{Name: ActivityPersistLineItem})
	env.RegisterActivityWithOptions(mockPersistInvoiceActivity, activity.RegisterOptions{Name: ActivityPersistInvoice})
	return env
}

func expectPersistBill(env *testsuite.TestWorkflowEnvironment, in BillInput) {
	env.OnActivity(ActivityPersistBill, mock.Anything, in).
		Return(nil).
		Once()
}

func mockPersistBillActivity(context.Context, BillInput) error {
	panic("mockPersistBillActivity should be mocked in workflow tests")
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

func testLineItem(reference, currency string) LineItem {
	return LineItem{
		Reference:   reference,
		AmountMinor: 1500,
		Currency:    currency,
		FeeType:     "wire_transfer",
		Description: "Outbound wire",
	}
}

func testLedgerRow(in BillInput, item LineItem) LedgerRow {
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

func assertApplicationErrorType(t *testing.T, err error, wantType string) {
	t.Helper()

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T %v, want temporal ApplicationError", err, err)
	}
	if appErr.Type() != wantType {
		t.Fatalf("application error type = %q, want %q", appErr.Type(), wantType)
	}
}
