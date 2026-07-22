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

func defaultTemporalConfig() temporalConfig {
	return temporalConfig{
		Target:    defaultTemporalTarget,
		Namespace: defaultTemporalNamespace,
		TaskQueue: temporalTaskQueue,
	}
}

//encore:service
type Service struct {
	temporalClient client.Client
	temporalWorker worker.Worker
	temporalConfig temporalConfig
}

func initService() (*Service, error) {
	cfg := defaultTemporalConfig()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Target,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("connect temporal at %s namespace %s: %w", cfg.Target, cfg.Namespace, err)
	}

	temporalWorker := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	registerScaffoldWorker(temporalWorker)

	if err := temporalWorker.Start(); err != nil {
		temporalClient.Close()
		return nil, fmt.Errorf("start temporal worker on task queue %s: %w", cfg.TaskQueue, err)
	}

	return &Service{
		temporalClient: temporalClient,
		temporalWorker: temporalWorker,
		temporalConfig: cfg,
	}, nil
}

func (s *Service) Shutdown(force context.Context) {
	if s.temporalWorker != nil {
		s.temporalWorker.Stop()
	}
	if s.temporalClient != nil {
		s.temporalClient.Close()
	}
}
