package fees

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestDefaultTemporalConfig(t *testing.T) {
	cfg := defaultTemporalConfig()
	if cfg.Target != "127.0.0.1:7233" {
		t.Fatalf("Target = %q, want 127.0.0.1:7233", cfg.Target)
	}
	if cfg.Namespace != "default" {
		t.Fatalf("Namespace = %q, want default", cfg.Namespace)
	}
	if cfg.TaskQueue != "fees" {
		t.Fatalf("TaskQueue = %q, want fees", cfg.TaskQueue)
	}
}

type fakeTemporalClient struct {
	closeCount int
}

func (c *fakeTemporalClient) Close() {
	c.closeCount++
}

func (c *fakeTemporalClient) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
	return fakeWorkflowRun{}, nil
}

func (c *fakeTemporalClient) GetWorkflow(context.Context, string, string) client.WorkflowRun {
	return fakeWorkflowRun{}
}

func (c *fakeTemporalClient) SignalWorkflow(context.Context, string, string, string, interface{}) error {
	return nil
}

type fakeWorkflowRun struct{}

func (fakeWorkflowRun) Get(context.Context, interface{}) error {
	return nil
}

func (fakeWorkflowRun) GetID() string {
	return ""
}

func (fakeWorkflowRun) GetRunID() string {
	return ""
}

func (fakeWorkflowRun) GetWithOptions(context.Context, interface{}, client.WorkflowRunGetOptions) error {
	return nil
}

type fakeTemporalWorker struct {
	startErr error

	startCount int
	stopCount  int

	workflowNames []string
	activityNames []string
	activities    []interface{}

	stopStarted chan struct{}
	stopRelease chan struct{}
}

func (w *fakeTemporalWorker) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	w.workflowNames = append(w.workflowNames, options.Name)
}

func (w *fakeTemporalWorker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	w.activityNames = append(w.activityNames, options.Name)
	w.activities = append(w.activities, a)
}

func (w *fakeTemporalWorker) Start() error {
	w.startCount++
	return w.startErr
}

func (w *fakeTemporalWorker) Stop() {
	w.stopCount++
	if w.stopStarted != nil {
		close(w.stopStarted)
	}
	if w.stopRelease != nil {
		<-w.stopRelease
	}
}

func resetTemporalFactories(t *testing.T) {
	t.Helper()

	originalDial := dialTemporal
	originalCreateWorker := createTemporalWorker
	originalNewRuntime := newTemporalRuntime

	t.Cleanup(func() {
		dialTemporal = originalDial
		createTemporalWorker = originalCreateWorker
		newTemporalRuntime = originalNewRuntime
	})
	newTemporalRuntime = productionTemporalRuntime
}

func TestInitServiceDialFailureReturnsError(t *testing.T) {
	resetTemporalFactories(t)

	expectedErr := errors.New("dial failed")
	dialTemporal = func(context.Context, client.Options) (temporalClient, error) {
		return nil, expectedErr
	}
	createTemporalWorker = func(temporalClient, string, worker.Options) (temporalWorker, error) {
		t.Fatal("worker factory should not be called when dial fails")
		return nil, nil
	}

	svc, err := initService()
	if err == nil {
		t.Fatal("expected initService error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected dial error to wrap %v, got %v", expectedErr, err)
	}
	if svc != nil {
		t.Fatalf("expected nil service on dial failure, got %#v", svc)
	}
}

func TestInitServiceWorkerStartFailureClosesClient(t *testing.T) {
	resetTemporalFactories(t)

	fakeClient := &fakeTemporalClient{}
	fakeWorker := &fakeTemporalWorker{startErr: errors.New("start failed")}
	dialTemporal = func(context.Context, client.Options) (temporalClient, error) {
		return fakeClient, nil
	}
	createTemporalWorker = func(temporalClient, string, worker.Options) (temporalWorker, error) {
		return fakeWorker, nil
	}

	svc, err := initService()
	if err == nil {
		t.Fatal("expected initService error, got nil")
	}
	if svc != nil {
		t.Fatalf("expected nil service on worker start failure, got %#v", svc)
	}
	if fakeClient.closeCount != 1 {
		t.Fatalf("client close count = %d, want 1", fakeClient.closeCount)
	}
	if fakeWorker.startCount != 1 {
		t.Fatalf("worker start count = %d, want 1", fakeWorker.startCount)
	}
}

func TestInitServiceSuccessRegistersAndStartsWorker(t *testing.T) {
	resetTemporalFactories(t)

	fakeClient := &fakeTemporalClient{}
	fakeWorker := &fakeTemporalWorker{}
	dialTemporal = func(context.Context, client.Options) (temporalClient, error) {
		return fakeClient, nil
	}
	createTemporalWorker = func(temporalClient, string, worker.Options) (temporalWorker, error) {
		return fakeWorker, nil
	}

	svc, err := initService()
	if err != nil {
		t.Fatalf("initService returned error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected service, got nil")
	}
	if fakeWorker.startCount != 1 {
		t.Fatalf("worker start count = %d, want 1", fakeWorker.startCount)
	}
	if len(fakeWorker.workflowNames) != 1 || fakeWorker.workflowNames[0] != BillWorkflowName {
		t.Fatalf("registered workflow names = %#v, want [%q]", fakeWorker.workflowNames, BillWorkflowName)
	}
	if len(fakeWorker.activityNames) != 1 {
		t.Fatalf("registered activity names = %#v, want Activities struct", fakeWorker.activityNames)
	}
	if fakeWorker.activityNames[0] != "" {
		t.Fatalf("activity registration name = %q, want empty name for unprefixed method activities", fakeWorker.activityNames[0])
	}
	if len(fakeWorker.activities) != 1 {
		t.Fatalf("registered activity values = %d, want 1", len(fakeWorker.activities))
	}
	if got := reflect.TypeOf(fakeWorker.activities[0]).String(); got != "*fees.Activities" {
		t.Fatalf("registered activity type = %q, want *fees.Activities", got)
	}
	if svc.temporalConfig != defaultTemporalConfig() {
		t.Fatalf("service config = %#v, want %#v", svc.temporalConfig, defaultTemporalConfig())
	}

	svc.Shutdown(context.Background())
	if fakeWorker.stopCount != 1 {
		t.Fatalf("worker stop count = %d, want 1", fakeWorker.stopCount)
	}
	if fakeClient.closeCount != 1 {
		t.Fatalf("client close count = %d, want 1", fakeClient.closeCount)
	}
}

func TestShutdownHonorsForceContext(t *testing.T) {
	stopStarted := make(chan struct{})
	stopRelease := make(chan struct{})
	fakeClient := &fakeTemporalClient{}
	fakeWorker := &fakeTemporalWorker{
		stopStarted: stopStarted,
		stopRelease: stopRelease,
	}
	svc := &Service{
		temporalClient: fakeClient,
		temporalWorker: fakeWorker,
	}

	force, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		svc.Shutdown(force)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Shutdown did not return after force context cancellation")
	}
	if fakeClient.closeCount != 1 {
		t.Fatalf("client close count = %d, want 1", fakeClient.closeCount)
	}

	select {
	case <-stopStarted:
		close(stopRelease)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("worker Stop was not called")
	}
}
