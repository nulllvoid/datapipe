package datapipe_test

import (
	"testing"
	"time"

	"github.com/nulllvoid/datapipe"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := datapipe.DefaultConfig()

	if cfg.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", cfg.Timeout)
	}
	if cfg.FailFast != false {
		t.Error("FailFast should be false by default")
	}
	if cfg.MaxRetries != 0 {
		t.Errorf("MaxRetries = %v, want 0", cfg.MaxRetries)
	}
	if cfg.RetryDelay != 100*time.Millisecond {
		t.Errorf("RetryDelay = %v, want 100ms", cfg.RetryDelay)
	}
}

func TestConfig_CustomValues(t *testing.T) {
	t.Parallel()

	cfg := datapipe.Config{
		Timeout:    5 * time.Second,
		FailFast:   true,
		MaxRetries: 3,
		RetryDelay: 500 * time.Millisecond,
	}

	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", cfg.Timeout)
	}
	if !cfg.FailFast {
		t.Error("FailFast should be true")
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %v, want 3", cfg.MaxRetries)
	}
	if cfg.RetryDelay != 500*time.Millisecond {
		t.Errorf("RetryDelay = %v, want 500ms", cfg.RetryDelay)
	}
}
