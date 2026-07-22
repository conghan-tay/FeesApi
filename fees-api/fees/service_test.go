package fees

import "testing"

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
