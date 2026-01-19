package datapipe_test

import (
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestEntity_GetID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		entity   datapipe.Entity
		expected string
	}{
		{
			name:     "returns correct ID",
			entity:   testEntity{ID: "entity_123", Name: "Test"},
			expected: "entity_123",
		},
		{
			name:     "returns empty ID",
			entity:   testEntity{ID: "", Name: "Empty"},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.entity.GetID(); got != tt.expected {
				t.Errorf("GetID() = %v, want %v", got, tt.expected)
			}
		})
	}
}
