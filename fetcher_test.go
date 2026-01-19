package datapipe_test

import (
	"context"
	"testing"

	"github.com/nulllvoid/datapipe"
)

type mockFetcher[T datapipe.Entity, Req datapipe.Request] struct {
	datapipe.BaseFetcher[T, Req]
	results     []T
	hasMore     bool
	err         error
	canHandle   func(Req) bool
	fetchCalled bool
}

func newMockFetcher[T datapipe.Entity, Req datapipe.Request](name string, priority int, results []T, hasMore bool, err error) *mockFetcher[T, Req] {
	return &mockFetcher[T, Req]{
		BaseFetcher: datapipe.NewBaseFetcher[T, Req](name, priority),
		results:     results,
		hasMore:     hasMore,
		err:         err,
	}
}

func (f *mockFetcher[T, Req]) Fetch(ctx context.Context, req Req) ([]T, bool, error) {
	f.fetchCalled = true
	if f.err != nil {
		return nil, false, f.err
	}
	return f.results, f.hasMore, nil
}

func (f *mockFetcher[T, Req]) CanHandle(req Req) bool {
	if f.canHandle != nil {
		return f.canHandle(req)
	}
	return f.BaseFetcher.CanHandle(req)
}

func TestBaseFetcher_Name(t *testing.T) {
	t.Parallel()

	fetcher := datapipe.NewBaseFetcher[testEntity, *testRequest]("test_fetcher", 1)

	if fetcher.Name() != "test_fetcher" {
		t.Errorf("Name() = %v, want test_fetcher", fetcher.Name())
	}
}

func TestBaseFetcher_Priority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		priority int
	}{
		{"priority 1", 1},
		{"priority 10", 10},
		{"priority 0", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fetcher := datapipe.NewBaseFetcher[testEntity, *testRequest]("fetcher", tt.priority)
			if fetcher.Priority() != tt.priority {
				t.Errorf("Priority() = %v, want %v", fetcher.Priority(), tt.priority)
			}
		})
	}
}

func TestBaseFetcher_CanHandle(t *testing.T) {
	t.Parallel()

	fetcher := datapipe.NewBaseFetcher[testEntity, *testRequest]("fetcher", 1)
	req := &testRequest{Limit: 10}

	if !fetcher.CanHandle(req) {
		t.Error("BaseFetcher.CanHandle should return true by default")
	}
}

func TestMockFetcher_CustomCanHandle(t *testing.T) {
	t.Parallel()

	fetcher := newMockFetcher[testEntity, *testRequest]("fetcher", 1, nil, false, nil)
	fetcher.canHandle = func(req *testRequest) bool {
		return req.HasSearchQuery()
	}

	t.Run("returns false for non-search request", func(t *testing.T) {
		t.Parallel()
		req := &testRequest{Limit: 10}
		if fetcher.CanHandle(req) {
			t.Error("Should return false for non-search request")
		}
	})

	t.Run("returns true for search request", func(t *testing.T) {
		t.Parallel()
		req := &testRequest{Limit: 10, Query: "search term"}
		if !fetcher.CanHandle(req) {
			t.Error("Should return true for search request")
		}
	})
}
