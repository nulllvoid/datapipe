package datapipe_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestPipelineError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		pipeline      string
		stage         string
		op            string
		err           error
		wantContains  []string
		wantUnwrapped error
	}{
		{
			name:         "with stage",
			pipeline:     "test_pipeline",
			stage:        "fetch",
			op:           "execute",
			err:          errors.New("underlying error"),
			wantContains: []string{"test_pipeline", "fetch", "execute", "underlying error"},
		},
		{
			name:         "without stage",
			pipeline:     "test_pipeline",
			stage:        "",
			op:           "init",
			err:          errors.New("init error"),
			wantContains: []string{"test_pipeline", "init", "init error"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := datapipe.NewPipelineError(tt.pipeline, tt.stage, tt.op, tt.err)

			for _, want := range tt.wantContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Error() = %v, want to contain %v", err.Error(), want)
				}
			}

			if unwrapped := errors.Unwrap(err); unwrapped != tt.err {
				t.Errorf("Unwrap() = %v, want %v", unwrapped, tt.err)
			}
		})
	}
}

func TestValidationError(t *testing.T) {
	t.Parallel()

	err := datapipe.NewValidationError("email", "invalid format")

	if !strings.Contains(err.Error(), "email") {
		t.Error("Error should contain field name")
	}
	if !strings.Contains(err.Error(), "invalid format") {
		t.Error("Error should contain message")
	}
	if !strings.Contains(err.Error(), "validation") {
		t.Error("Error should indicate validation error")
	}
}

func TestFetchError(t *testing.T) {
	t.Parallel()

	t.Run("with underlying error", func(t *testing.T) {
		t.Parallel()
		underlying := errors.New("connection refused")
		err := datapipe.NewFetchError("database", "failed to connect", underlying)

		if !strings.Contains(err.Error(), "database") {
			t.Error("Error should contain source")
		}
		if !strings.Contains(err.Error(), "failed to connect") {
			t.Error("Error should contain message")
		}
		if errors.Unwrap(err) != underlying {
			t.Error("Unwrap should return underlying error")
		}
	})

	t.Run("without underlying error", func(t *testing.T) {
		t.Parallel()
		err := datapipe.NewFetchError("cache", "key not found", nil)

		if !strings.Contains(err.Error(), "cache") {
			t.Error("Error should contain source")
		}
		if errors.Unwrap(err) != nil {
			t.Error("Unwrap should return nil for no underlying error")
		}
	})
}

func TestEnrichmentError(t *testing.T) {
	t.Parallel()

	t.Run("with underlying error", func(t *testing.T) {
		t.Parallel()
		underlying := errors.New("service unavailable")
		err := datapipe.NewEnrichmentError("user_enricher", "failed to fetch users", underlying)

		if !strings.Contains(err.Error(), "user_enricher") {
			t.Error("Error should contain enricher name")
		}
		if !strings.Contains(err.Error(), "failed to fetch users") {
			t.Error("Error should contain message")
		}
		if errors.Unwrap(err) != underlying {
			t.Error("Unwrap should return underlying error")
		}
	})

	t.Run("without underlying error", func(t *testing.T) {
		t.Parallel()
		err := datapipe.NewEnrichmentError("data_enricher", "no data available", nil)

		if errors.Unwrap(err) != nil {
			t.Error("Unwrap should return nil for no underlying error")
		}
	})
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNoFetcherAvailable", datapipe.ErrNoFetcherAvailable, "no fetcher"},
		{"ErrValidationFailed", datapipe.ErrValidationFailed, "validation"},
		{"ErrFetchFailed", datapipe.ErrFetchFailed, "fetch"},
		{"ErrEnrichmentFailed", datapipe.ErrEnrichmentFailed, "enrichment"},
		{"ErrTimeout", datapipe.ErrTimeout, "timed out"},
		{"ErrCanceled", datapipe.ErrCanceled, "canceled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if !strings.Contains(tt.err.Error(), tt.want) {
				t.Errorf("%s.Error() = %v, want to contain %v", tt.name, tt.err.Error(), tt.want)
			}
		})
	}
}
