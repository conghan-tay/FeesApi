package charge

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/client"
)

func resetTemporalDialer(t *testing.T) {
	t.Helper()
	original := dialTemporal
	t.Cleanup(func() { dialTemporal = original })
}

func TestDefaultTemporalConfig(t *testing.T) {
	cfg := defaultTemporalConfig()
	if cfg.Target != "127.0.0.1:7233" {
		t.Fatalf("Target = %q, want 127.0.0.1:7233", cfg.Target)
	}
	if cfg.Namespace != "default" {
		t.Fatalf("Namespace = %q, want default", cfg.Namespace)
	}
	if cfg.UseAPIKeyAuth {
		t.Fatal("UseAPIKeyAuth = true, want false for tests")
	}
}

func TestInitServiceSuccessAndShutdown(t *testing.T) {
	resetTemporalDialer(t)
	fakeClient := &recordingTemporalClient{}
	var gotOptions client.Options
	dialTemporal = func(_ context.Context, options client.Options) (temporalClient, error) {
		gotOptions = options
		return fakeClient, nil
	}

	svc, err := initService()
	if err != nil {
		t.Fatalf("initService returned error: %v", err)
	}
	if gotOptions.HostPort != "127.0.0.1:7233" || gotOptions.Namespace != "default" {
		t.Fatalf("Temporal options = %#v, want default target and namespace", gotOptions)
	}
	if gotOptions.Credentials != nil {
		t.Fatal("Temporal credentials configured for local test environment")
	}
	if svc.temporalConfig != defaultTemporalConfig() {
		t.Fatalf("service config = %#v, want %#v", svc.temporalConfig, defaultTemporalConfig())
	}

	svc.Shutdown(context.Background())
	if fakeClient.closeCount != 1 {
		t.Fatalf("client close count = %d, want 1", fakeClient.closeCount)
	}
}

func TestInitServiceDialFailure(t *testing.T) {
	resetTemporalDialer(t)
	expectedErr := errors.New("dial failed")
	dialTemporal = func(context.Context, client.Options) (temporalClient, error) {
		return nil, expectedErr
	}

	svc, err := initService()
	if !errors.Is(err, expectedErr) {
		t.Fatalf("initService error = %v, want wrapped %v", err, expectedErr)
	}
	if svc != nil {
		t.Fatalf("service = %#v, want nil", svc)
	}
}
