package datapipe_test

import (
	"context"
	"testing"

	"github.com/nulllvoid/datapipe"
)

type mockEnricher[T datapipe.Entity] struct {
	datapipe.BaseEnricher[T]
	enrichFunc  func(ctx context.Context, items []T) error
	enrichCalls int
}

func newMockEnricher[T datapipe.Entity](name string, opts ...datapipe.EnricherOption[T]) *mockEnricher[T] {
	return &mockEnricher[T]{
		BaseEnricher: datapipe.NewBaseEnricher[T](name, opts...),
	}
}

func (e *mockEnricher[T]) Enrich(ctx context.Context, items []T) error {
	e.enrichCalls++
	if e.enrichFunc != nil {
		return e.enrichFunc(ctx, items)
	}
	return nil
}

func TestBaseEnricher_Name(t *testing.T) {
	t.Parallel()

	enricher := datapipe.NewBaseEnricher[testEntity]("test_enricher")

	if enricher.Name() != "test_enricher" {
		t.Errorf("Name() = %v, want test_enricher", enricher.Name())
	}
}

func TestBaseEnricher_Required(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		required bool
	}{
		{"not required by default", false},
		{"required when set", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var enricher datapipe.BaseEnricher[testEntity]
			if tt.required {
				enricher = datapipe.NewBaseEnricher[testEntity]("e",
					datapipe.EnricherWithRequired[testEntity](true),
				)
			} else {
				enricher = datapipe.NewBaseEnricher[testEntity]("e")
			}
			if enricher.Required() != tt.required {
				t.Errorf("Required() = %v, want %v", enricher.Required(), tt.required)
			}
		})
	}
}

func TestBaseEnricher_CanEnrich_AuthTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		allowedAuths []string
		authType     string
		expandParams []string
		expected     bool
	}{
		{
			name:         "wildcard allows any auth",
			allowedAuths: []string{"*"},
			authType:     "private",
			expected:     true,
		},
		{
			name:         "specific auth matches",
			allowedAuths: []string{"private"},
			authType:     "private",
			expected:     true,
		},
		{
			name:         "specific auth does not match",
			allowedAuths: []string{"private"},
			authType:     "public",
			expected:     false,
		},
		{
			name:         "multiple auths - one matches",
			allowedAuths: []string{"private", "admin"},
			authType:     "admin",
			expected:     true,
		},
		{
			name:         "empty auth type with wildcard",
			allowedAuths: []string{"*"},
			authType:     "",
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enricher := datapipe.NewBaseEnricher[testEntity]("e",
				datapipe.EnricherWithAllowedAuths[testEntity](tt.allowedAuths...),
			)
			if got := enricher.CanEnrich(tt.authType, tt.expandParams); got != tt.expected {
				t.Errorf("CanEnrich() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBaseEnricher_CanEnrich_ExpandParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		expandKey    string
		expandParams []string
		expected     bool
	}{
		{
			name:         "no expand key - always enriches",
			expandKey:    "",
			expandParams: nil,
			expected:     true,
		},
		{
			name:         "expand key matches param",
			expandKey:    "user",
			expandParams: []string{"user", "account"},
			expected:     true,
		},
		{
			name:         "expand key does not match",
			expandKey:    "transaction",
			expandParams: []string{"user", "account"},
			expected:     false,
		},
		{
			name:         "expand key with empty params",
			expandKey:    "user",
			expandParams: nil,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := []datapipe.EnricherOption[testEntity]{}
			if tt.expandKey != "" {
				opts = append(opts, datapipe.EnricherWithExpandKey[testEntity](tt.expandKey))
			}
			enricher := datapipe.NewBaseEnricher[testEntity]("e", opts...)
			if got := enricher.CanEnrich("private", tt.expandParams); got != tt.expected {
				t.Errorf("CanEnrich() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBaseEnricher_ExpandKey(t *testing.T) {
	t.Parallel()

	enricher := datapipe.NewBaseEnricher[testEntity]("e",
		datapipe.EnricherWithExpandKey[testEntity]("fund_account"),
	)

	if enricher.ExpandKey() != "fund_account" {
		t.Errorf("ExpandKey() = %v, want fund_account", enricher.ExpandKey())
	}
}

func TestBaseEnricher_AllowedAuths(t *testing.T) {
	t.Parallel()

	enricher := datapipe.NewBaseEnricher[testEntity]("e",
		datapipe.EnricherWithAllowedAuths[testEntity]("private", "admin"),
	)

	auths := enricher.AllowedAuths()
	if len(auths) != 2 {
		t.Fatalf("AllowedAuths() length = %v, want 2", len(auths))
	}
	if auths[0] != "private" || auths[1] != "admin" {
		t.Errorf("AllowedAuths() = %v, want [private, admin]", auths)
	}
}
