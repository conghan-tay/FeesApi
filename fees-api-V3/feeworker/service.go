package feeworker

import (
	"context"
	"fmt"

	"encore.app/internal/feesworkflowcontract"
	"encore.app/internal/temporalconnection"
	"encore.dev/config"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const temporalTaskQueue = feesworkflowcontract.TaskQueue

type temporalServiceConfig struct {
	Target        config.String
	Namespace     config.String
	UseAPIKeyAuth config.Bool
}

var temporalCfg = config.Load[*temporalServiceConfig]()

var secrets struct {
	TemporalAPIKey string
}

type temporalConfig struct {
	Target        string
	Namespace     string
	TaskQueue     string
	UseAPIKeyAuth bool
}

type temporalClient interface {
	Close()
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
}

type temporalWorker interface {
	workerRegistrar
	Start() error
	Stop()
}

type temporalRuntime struct {
	client temporalClient
	worker temporalWorker
	config temporalConfig
}

type temporalRuntimeFactory func(temporalConfig) (*temporalRuntime, error)
type temporalDialer func(context.Context, client.Options) (temporalClient, error)
type temporalWorkerFactory func(temporalClient, string, worker.Options) (temporalWorker, error)

var (
	dialTemporal temporalDialer = func(ctx context.Context, options client.Options) (temporalClient, error) {
		return client.DialContext(ctx, options)
	}
	createTemporalWorker temporalWorkerFactory = func(temporalClient temporalClient, taskQueue string, options worker.Options) (temporalWorker, error) {
		client, ok := temporalClient.(client.Client)
		if !ok {
			return nil, fmt.Errorf("temporal client does not implement client.Client")
		}
		return worker.New(client, taskQueue, options), nil
	}
	newTemporalRuntime temporalRuntimeFactory = productionTemporalRuntime
)

func defaultTemporalConfig() temporalConfig {
	return temporalConfig{
		Target:        temporalCfg.Target(),
		Namespace:     temporalCfg.Namespace(),
		TaskQueue:     temporalTaskQueue,
		UseAPIKeyAuth: temporalCfg.UseAPIKeyAuth(),
	}
}

//encore:service
type Service struct {
	temporalClient temporalClient
	temporalWorker temporalWorker
	temporalConfig temporalConfig
}

func initService() (*Service, error) {
	cfg := defaultTemporalConfig()

	runtime, err := newTemporalRuntime(cfg)
	if err != nil {
		return nil, err
	}

	if err := runtime.worker.Start(); err != nil {
		runtime.client.Close()
		return nil, fmt.Errorf("start temporal worker on task queue %s: %w", cfg.TaskQueue, err)
	}

	return &Service{
		temporalClient: runtime.client,
		temporalWorker: runtime.worker,
		temporalConfig: runtime.config,
	}, nil
}

func productionTemporalRuntime(cfg temporalConfig) (*temporalRuntime, error) {
	options, err := temporalconnection.ClientOptions(temporalconnection.Config{
		Target:        cfg.Target,
		Namespace:     cfg.Namespace,
		UseAPIKeyAuth: cfg.UseAPIKeyAuth,
	}, temporalAPIKey(cfg))
	if err != nil {
		return nil, fmt.Errorf("configure temporal client: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), temporalconnection.DialTimeout)
	defer cancel()
	temporalClient, err := dialTemporal(dialCtx, options)
	if err != nil {
		return nil, fmt.Errorf("connect temporal at %s namespace %s: %w", cfg.Target, cfg.Namespace, err)
	}

	temporalWorker, err := createTemporalWorker(temporalClient, cfg.TaskQueue, worker.Options{})
	if err != nil {
		temporalClient.Close()
		return nil, fmt.Errorf("create temporal worker on task queue %s: %w", cfg.TaskQueue, err)
	}
	registerWorkflows(temporalWorker)
	registerActivities(temporalWorker, NewActivities())

	return &temporalRuntime{
		client: temporalClient,
		worker: temporalWorker,
		config: cfg,
	}, nil
}

func temporalAPIKey(cfg temporalConfig) string {
	if !cfg.UseAPIKeyAuth {
		return ""
	}
	return secrets.TemporalAPIKey
}

func (s *Service) Shutdown(force context.Context) {
	if force == nil {
		force = context.Background()
	}
	if s.temporalWorker != nil {
		done := make(chan struct{})
		go func() {
			s.temporalWorker.Stop()
			close(done)
		}()
		select {
		case <-done:
		case <-force.Done():
		}
	}
	if s.temporalClient != nil {
		s.temporalClient.Close()
	}
}
