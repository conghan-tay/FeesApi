package fees

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const (
	defaultTemporalTarget    = "127.0.0.1:7233"
	defaultTemporalNamespace = "default"
	temporalTaskQueue        = "fees"
)

type temporalConfig struct {
	Target    string
	Namespace string
	TaskQueue string
}

type temporalClient interface {
	Close()
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
		Target:    defaultTemporalTarget,
		Namespace: defaultTemporalNamespace,
		TaskQueue: temporalTaskQueue,
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
	temporalClient, err := dialTemporal(context.Background(), client.Options{
		HostPort:  cfg.Target,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("connect temporal at %s namespace %s: %w", cfg.Target, cfg.Namespace, err)
	}

	temporalWorker, err := createTemporalWorker(temporalClient, cfg.TaskQueue, worker.Options{})
	if err != nil {
		temporalClient.Close()
		return nil, fmt.Errorf("create temporal worker on task queue %s: %w", cfg.TaskQueue, err)
	}
	registerScaffoldWorker(temporalWorker)
	registerActivities(temporalWorker, NewActivities(db))

	return &temporalRuntime{
		client: temporalClient,
		worker: temporalWorker,
		config: cfg,
	}, nil
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
