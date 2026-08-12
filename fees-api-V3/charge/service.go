package charge

import (
	"context"
	"fmt"

	"encore.app/internal/temporalconnection"
	"encore.dev/config"
	"encore.dev/pubsub"
	"go.temporal.io/sdk/client"
)

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
	UseAPIKeyAuth bool
}

type temporalClient interface {
	Close()
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
}

type lineItemEventPublisher interface {
	Publish(context.Context, *LineItemEvent) (string, error)
}

type temporalDialer func(context.Context, client.Options) (temporalClient, error)

var dialTemporal temporalDialer = func(ctx context.Context, options client.Options) (temporalClient, error) {
	return client.DialContext(ctx, options)
}

func defaultTemporalConfig() temporalConfig {
	return temporalConfig{
		Target:        temporalCfg.Target(),
		Namespace:     temporalCfg.Namespace(),
		UseAPIKeyAuth: temporalCfg.UseAPIKeyAuth(),
	}
}

//encore:service
type Service struct {
	temporalClient temporalClient
	temporalConfig temporalConfig
	lineItemEvents lineItemEventPublisher
}

func initService() (*Service, error) {
	cfg := defaultTemporalConfig()
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

	return &Service{
		temporalClient: temporalClient,
		temporalConfig: cfg,
		lineItemEvents: pubsub.TopicRef[pubsub.Publisher[*LineItemEvent]](UpdateLineItems),
	}, nil
}

func temporalAPIKey(cfg temporalConfig) string {
	if !cfg.UseAPIKeyAuth {
		return ""
	}
	return secrets.TemporalAPIKey
}

func (s *Service) Shutdown(context.Context) {
	if s != nil && s.temporalClient != nil {
		s.temporalClient.Close()
	}
}
