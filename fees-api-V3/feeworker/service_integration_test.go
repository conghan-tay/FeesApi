package feeworker

import (
	"context"
	"os"
	"testing"
)

func TestInitServiceWithLiveTemporal(t *testing.T) {
	if os.Getenv("PAVEBANK_LIVE_TEMPORAL") != "1" {
		t.Skip("set PAVEBANK_LIVE_TEMPORAL=1 with temporal server start-dev running")
	}

	svc, err := initService()
	if err != nil {
		t.Fatalf("initService with live Temporal returned error: %v", err)
	}
	if svc.temporalClient == nil {
		t.Fatal("expected Temporal client to be initialized")
	}
	if svc.temporalWorker == nil {
		t.Fatal("expected Temporal worker to be initialized")
	}
	svc.Shutdown(context.Background())
}
