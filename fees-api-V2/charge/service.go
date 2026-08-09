package charge

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

const (
	defaultTemporalTarget    = "127.0.0.1:7233"
	defaultTemporalNamespace = "default"
)

type temporalConfig struct {
	Target    string
	Namespace string
}

type temporalClient interface {
	Close()
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
