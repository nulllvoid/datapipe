package datapipe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nulllvoid/datapipe"
)

func TestFetcherChain_NewFetcherChain(t *testing.T) {
	t.Parallel()

	chain := datapipe.NewFetcherChain[testEntity, *testRequest]()

	if chain.FetcherCount() != 0 {
		t.Errorf("FetcherCount() = %v, want 0", chain.FetcherCount())
	}
}

func TestFetcherChain_WithFetchers(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 2, nil, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 1, nil, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
	)

	if chain.FetcherCount() != 2 {
		t.Errorf("FetcherCount() = %v, want 2", chain.FetcherCount())
	}
}

func TestFetcherChain_Sequential_SingleFetcher(t *testing.T) {
	t.Parallel()

	results := []testEntity{{ID: "1"}, {ID: "2"}}
	f := newMockFetcher[testEntity, *testRequest]("primary", 1, results, true, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
	)

	got, hasMore, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("Results length = %v, want 2", len(got))
	}
	if !hasMore {
		t.Error("HasMore should be true")
	}
	if source != "primary" {
		t.Errorf("Source = %v, want primary", source)
	}
}

func TestFetcherChain_Sequential_FallbackOnError(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("primary", 1, nil, false, errors.New("primary failed"))
	f2 := newMockFetcher[testEntity, *testRequest]("secondary", 2, []testEntity{{ID: "1"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
	if source != "secondary" {
		t.Errorf("Source = %v, want secondary", source)
	}
}

func TestFetcherChain_Sequential_FallbackOnEmptyResults(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("primary", 1, []testEntity{}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("secondary", 2, []testEntity{{ID: "1"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
	if source != "secondary" {
		t.Errorf("Source = %v, want secondary", source)
	}
}

func TestFetcherChain_Sequential_AllFail(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, nil, false, errors.New("f1 failed"))
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, nil, false, errors.New("f2 failed"))

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
	)

	_, _, _, err := chain.Fetch(context.Background(), &testRequest{})

	if err == nil {
		t.Fatal("Fetch() should return error when all fetchers fail")
	}
}

func TestFetcherChain_Sequential_MinResults(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, []testEntity{{ID: "1"}}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "2"}, {ID: "3"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
		datapipe.ChainWithMinResults[testEntity, *testRequest](3),
	)

	got, _, _, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("Results length = %v, want 3", len(got))
	}
}

func TestFetcherChain_Sequential_CanHandleFiltering(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, []testEntity{{ID: "1"}}, false, nil)
	f1.canHandle = func(req *testRequest) bool { return false }

	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "2"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !f2.fetchCalled {
		t.Error("f2 should have been called")
	}
	if f1.fetchCalled {
		t.Error("f1 should not have been called (CanHandle returned false)")
	}
	if source != "f2" {
		t.Errorf("Source = %v, want f2", source)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
}

func TestFetcherChain_FirstSuccess(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, []testEntity{{ID: "1"}}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "2"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackFirstSuccess),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
	if source != "f1" {
		t.Errorf("Source = %v, want f1", source)
	}
	if f2.fetchCalled {
		t.Error("f2 should not have been called in FirstSuccess mode")
	}
}

func TestFetcherChain_FirstSuccess_SkipsEmpty(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, []testEntity{}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "1"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackFirstSuccess),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if source != "f2" {
		t.Errorf("Source = %v, want f2", source)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
}

func TestFetcherChain_Parallel(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, []testEntity{{ID: "1"}}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "2"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackParallel),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if source != "f1" {
		t.Errorf("Source = %v, want f1 (highest priority)", source)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
}

func TestFetcherChain_Parallel_UsesHighestPriority(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("low_priority", 10, []testEntity{{ID: "1"}}, false, nil)
	f2 := newMockFetcher[testEntity, *testRequest]("high_priority", 1, []testEntity{{ID: "2"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackParallel),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if source != "high_priority" {
		t.Errorf("Source = %v, want high_priority", source)
	}
	if got[0].ID != "2" {
		t.Errorf("Result ID = %v, want 2", got[0].ID)
	}
}

func TestFetcherChain_Parallel_FallsBackOnError(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, nil, false, errors.New("f1 failed"))
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "1"}}, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackParallel),
	)

	got, _, source, err := chain.Fetch(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if source != "f2" {
		t.Errorf("Source = %v, want f2", source)
	}
	if len(got) != 1 {
		t.Errorf("Results length = %v, want 1", len(got))
	}
}

func TestFetcherChain_Parallel_ContextCancellation(t *testing.T) {
	t.Parallel()

	slowFetcher := &slowMockFetcher[testEntity, *testRequest]{
		BaseFetcher: datapipe.NewBaseFetcher[testEntity, *testRequest]("slow", 1),
		delay:       100 * time.Millisecond,
	}

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](slowFetcher),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackParallel),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, _, _, err := chain.Fetch(ctx, &testRequest{})

	if err == nil {
		t.Fatal("Fetch() should return error on context cancellation")
	}
}

func TestFetcherChain_NoFetchersAvailable(t *testing.T) {
	t.Parallel()

	chain := datapipe.NewFetcherChain[testEntity, *testRequest]()

	_, _, _, err := chain.Fetch(context.Background(), &testRequest{})

	if !errors.Is(err, datapipe.ErrNoFetcherAvailable) {
		t.Errorf("Fetch() error = %v, want ErrNoFetcherAvailable", err)
	}
}

func TestFetcherChain_PrioritySorting(t *testing.T) {
	t.Parallel()

	callOrder := make([]string, 0)

	f1 := &orderTrackingFetcher[testEntity, *testRequest]{
		BaseFetcher: datapipe.NewBaseFetcher[testEntity, *testRequest]("third", 3),
		callOrder:   &callOrder,
	}
	f2 := &orderTrackingFetcher[testEntity, *testRequest]{
		BaseFetcher: datapipe.NewBaseFetcher[testEntity, *testRequest]("first", 1),
		callOrder:   &callOrder,
	}
	f3 := &orderTrackingFetcher[testEntity, *testRequest]{
		BaseFetcher: datapipe.NewBaseFetcher[testEntity, *testRequest]("second", 2),
		callOrder:   &callOrder,
	}

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](f1),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f2),
		datapipe.ChainWithFetcher[testEntity, *testRequest](f3),
		datapipe.ChainWithFallbackMode[testEntity, *testRequest](datapipe.FallbackSequential),
	)

	chain.Fetch(context.Background(), &testRequest{})

	expected := []string{"first", "second", "third"}
	for i, name := range expected {
		if i >= len(callOrder) || callOrder[i] != name {
			t.Errorf("Call order[%d] = %v, want %v", i, callOrder[i], name)
		}
	}
}

type slowMockFetcher[T datapipe.Entity, Req datapipe.Request] struct {
	datapipe.BaseFetcher[T, Req]
	delay time.Duration
}

func (f *slowMockFetcher[T, Req]) Fetch(ctx context.Context, req Req) ([]T, bool, error) {
	select {
	case <-time.After(f.delay):
		return nil, false, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

type orderTrackingFetcher[T datapipe.Entity, Req datapipe.Request] struct {
	datapipe.BaseFetcher[T, Req]
	callOrder *[]string
}

func (f *orderTrackingFetcher[T, Req]) Fetch(ctx context.Context, req Req) ([]T, bool, error) {
	*f.callOrder = append(*f.callOrder, f.Name())
	return nil, false, nil
}

