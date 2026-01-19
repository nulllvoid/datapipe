package datapipe_test

import (
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestRequest_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		request        *testRequest
		expectedLimit  int
		expectedOffset int
	}{
		{
			name:           "default pagination",
			request:        &testRequest{Limit: 10, Offset: 0},
			expectedLimit:  10,
			expectedOffset: 0,
		},
		{
			name:           "with offset",
			request:        &testRequest{Limit: 20, Offset: 100},
			expectedLimit:  20,
			expectedOffset: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.request.GetLimit(); got != tt.expectedLimit {
				t.Errorf("GetLimit() = %v, want %v", got, tt.expectedLimit)
			}
			if got := tt.request.GetOffset(); got != tt.expectedOffset {
				t.Errorf("GetOffset() = %v, want %v", got, tt.expectedOffset)
			}
		})
	}
}

func TestRequest_Clone(t *testing.T) {
	t.Parallel()

	original := &testRequest{Limit: 10, Offset: 5, Query: "test"}
	cloned := original.Clone().(*testRequest)

	if cloned.Limit != original.Limit {
		t.Error("Clone did not preserve Limit")
	}

	cloned.Limit = 100
	if original.Limit == cloned.Limit {
		t.Error("Clone is not independent of original")
	}
}

func TestSearchableRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		request     *testRequest
		hasSearch   bool
		searchQuery string
	}{
		{
			name:        "with search query",
			request:     &testRequest{Query: "test search"},
			hasSearch:   true,
			searchQuery: "test search",
		},
		{
			name:        "without search query",
			request:     &testRequest{Query: ""},
			hasSearch:   false,
			searchQuery: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.request.HasSearchQuery(); got != tt.hasSearch {
				t.Errorf("HasSearchQuery() = %v, want %v", got, tt.hasSearch)
			}
			if got := tt.request.GetSearchQuery(); got != tt.searchQuery {
				t.Errorf("GetSearchQuery() = %v, want %v", got, tt.searchQuery)
			}
		})
	}
}

func TestSortOrder_Constants(t *testing.T) {
	t.Parallel()

	if datapipe.SortAsc != "asc" {
		t.Errorf("SortAsc = %v, want asc", datapipe.SortAsc)
	}
	if datapipe.SortDesc != "desc" {
		t.Errorf("SortDesc = %v, want desc", datapipe.SortDesc)
	}
}
