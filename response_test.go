package datapipe_test

import (
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestCollectionResponse_GetItems(t *testing.T) {
	t.Parallel()

	items := []testEntity{
		{ID: "1", Name: "First"},
		{ID: "2", Name: "Second"},
	}

	resp := datapipe.NewCollectionResponse(items, false)

	if len(resp.GetItems()) != 2 {
		t.Errorf("GetItems() length = %v, want 2", len(resp.GetItems()))
	}
}

func TestCollectionResponse_GetCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []testEntity
		expected int
	}{
		{
			name:     "empty collection",
			items:    []testEntity{},
			expected: 0,
		},
		{
			name: "collection with items",
			items: []testEntity{
				{ID: "1"},
				{ID: "2"},
				{ID: "3"},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := datapipe.NewCollectionResponse(tt.items, false)
			if got := resp.GetCount(); got != tt.expected {
				t.Errorf("GetCount() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestCollectionResponse_HasMore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hasMore  bool
		expected bool
	}{
		{
			name:     "has more results",
			hasMore:  true,
			expected: true,
		},
		{
			name:     "no more results",
			hasMore:  false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := datapipe.NewCollectionResponse([]testEntity{}, tt.hasMore)
			if got := resp.HasMore(); got != tt.expected {
				t.Errorf("HasMore() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNewCollectionResponse(t *testing.T) {
	t.Parallel()

	items := []testEntity{{ID: "1"}, {ID: "2"}}
	resp := datapipe.NewCollectionResponse(items, true)

	if resp.Entity != "collection" {
		t.Errorf("Entity = %v, want collection", resp.Entity)
	}
	if resp.Count != 2 {
		t.Errorf("Count = %v, want 2", resp.Count)
	}
	if !resp.More {
		t.Error("More should be true")
	}
}
