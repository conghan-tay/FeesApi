package temporalconnection

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
)

// DialTimeout bounds service startup when Temporal is unreachable or
// misconfigured. Encore can then fail the deployment instead of hanging.
const DialTimeout = 15 * time.Second

// Config describes the Temporal endpoint shared by API clients and workers.
type Config struct {
	Target        string
	Namespace     string
	UseAPIKeyAuth bool
}

// ClientOptions converts environment-specific settings into Temporal SDK
// options. Supplying API-key credentials also enables TLS in the Go SDK.
func ClientOptions(cfg Config, apiKey string) (client.Options, error) {
	target := strings.TrimSpace(cfg.Target)
	if target == "" {
		return client.Options{}, fmt.Errorf("temporal target is required")
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		return client.Options{}, fmt.Errorf("temporal namespace is required")
	}

	options := client.Options{
		HostPort:  target,
		Namespace: namespace,
	}
	if cfg.UseAPIKeyAuth {
		apiKey = strings.TrimSpace(apiKey)
		if apiKey == "" {
			return client.Options{}, fmt.Errorf("temporal API key is required when API-key authentication is enabled")
		}
		options.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}

	return options, nil
}
