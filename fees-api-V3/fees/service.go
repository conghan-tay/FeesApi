package fees

import (
	"context"
	"fmt"

	"encore.app/internal/feesworkflowcontract"
	"go.temporal.io/sdk/client"
)

const (
	defaultTemporalTarget    = "127.0.0.1:7233"
	defaultTemporalNamespace = "default"
	temporalTaskQueue        = feesworkflowcontract.TaskQueue
)

type temporalConfig struct {
	Target    string
	Namespace string
	TaskQueue string
}

type temporalClient interface {
	Close()
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
}

type temporalDialer func(context.Context, client.Options) (temporalClient, error)

var dialTemporal temporalDialer = func(ctx context.Context, options client.Options) (temporalClient, error) {
	return client.DialContext(ctx, options)
}

func defaultTemporalConfig() temporalConfig {
	return temporalConfig{
		Target:    defaultTemporalTarget,
		Namespace: defaultTemporalNamespace,
		TaskQueue: temporalTaskQueue,
	}
}

//encore:service
type Service struct {
	temporalClient temporalClient
	temporalConfig temporalConfig
}

func initService() (*Service, error) {
	cfg := defaultTemporalConfig()
	temporalClient, err := dialTemporal(context.Background(), client.Options{
		HostPort:  cfg.Target,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("connect temporal at %s namespace %s: %w", cfg.Target, cfg.Namespace, err)
	}

	return &Service{
		temporalClient: temporalClient,
		temporalConfig: cfg,
	}, nil
}

func (s *Service) Shutdown(context.Context) {
	if s != nil && s.temporalClient != nil {
		s.temporalClient.Close()
	}
}
