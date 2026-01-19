package datapipe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestFetchStage_Name(t *testing.T) {
	t.Parallel()

	chain := datapipe.NewFetcherChain[testEntity, *testRequest]()
	stage := datapipe.NewFetchStage[testEntity, *testRequest](chain)

	if stage.Name() != "fetch" {
		t.Errorf("Name() = %v, want fetch", stage.Name())
	}
}

func TestFetchStage_Required(t *testing.T) {
	t.Parallel()

	chain := datapipe.NewFetcherChain[testEntity, *testRequest]()
	stage := datapipe.NewFetchStage[testEntity, *testRequest](chain)

	if !stage.Required() {
		t.Error("FetchStage should be required")
	}
}

func TestFetchStage_Execute_Success(t *testing.T) {
	t.Parallel()

	results := []testEntity{{ID: "1"}, {ID: "2"}}
	fetcher := newMockFetcher[testEntity, *testRequest]("primary", 1, results, true, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](fetcher),
	)

	stage := datapipe.NewFetchStage[testEntity, *testRequest](chain)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{Limit: 10})

	err := stage.Execute(context.Background(), state)

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if state.ResultCount() != 2 {
		t.Errorf("ResultCount() = %v, want 2", state.ResultCount())
	}
	if !state.HasMore() {
		t.Error("HasMore() should be true")
	}
	if state.Source() != "primary" {
		t.Errorf("Source() = %v, want primary", state.Source())
	}
}

func TestFetchStage_Execute_Error(t *testing.T) {
	t.Parallel()

	fetchErr := errors.New("fetch failed")
	fetcher := newMockFetcher[testEntity, *testRequest]("primary", 1, nil, false, fetchErr)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](fetcher),
	)

	stage := datapipe.NewFetchStage[testEntity, *testRequest](chain)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	err := stage.Execute(context.Background(), state)

	if err == nil {
		t.Fatal("Execute() should return error")
	}
	if state.ResultCount() != 0 {
		t.Error("State should not have results on error")
	}
}

func TestFetchStage_Execute_NoFetchers(t *testing.T) {
	t.Parallel()

	chain := datapipe.NewFetcherChain[testEntity, *testRequest]()
	stage := datapipe.NewFetchStage[testEntity, *testRequest](chain)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	err := stage.Execute(context.Background(), state)

	if !errors.Is(err, datapipe.ErrNoFetcherAvailable) {
		t.Errorf("Execute() error = %v, want ErrNoFetcherAvailable", err)
	}
}

func TestFetchStage_WithPipeline(t *testing.T) {
	t.Parallel()

	results := []testEntity{{ID: "1"}, {ID: "2"}}
	fetcher := newMockFetcher[testEntity, *testRequest]("primary", 1, results, false, nil)

	chain := datapipe.NewFetcherChain[testEntity, *testRequest](
		datapipe.ChainWithFetcher[testEntity, *testRequest](fetcher),
	)

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithFetcherChain[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](chain),
		datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
		),
	)

	resp, err := pipeline.Execute(context.Background(), &testRequest{Limit: 10})

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if resp.GetCount() != 2 {
		t.Errorf("Count = %v, want 2", resp.GetCount())
	}
}

func TestPipeline_WithFetchers(t *testing.T) {
	t.Parallel()

	results := []testEntity{{ID: "1"}}
	fetcher := newMockFetcher[testEntity, *testRequest]("primary", 1, results, false, nil)

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithFetchers[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](fetcher),
		datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
		),
	)

	resp, err := pipeline.Execute(context.Background(), &testRequest{Limit: 10})

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Count = %v, want 1", resp.GetCount())
	}
}

func TestPipeline_WithFetchersAndMode(t *testing.T) {
	t.Parallel()

	f1 := newMockFetcher[testEntity, *testRequest]("f1", 1, nil, false, errors.New("f1 failed"))
	f2 := newMockFetcher[testEntity, *testRequest]("f2", 2, []testEntity{{ID: "1"}}, false, nil)

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithFetchersAndMode[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			datapipe.FallbackSequential,
			f1, f2,
		),
		datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
		),
	)

	resp, err := pipeline.Execute(context.Background(), &testRequest{})

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if resp.GetCount() != 1 {
		t.Errorf("Count = %v, want 1", resp.GetCount())
	}
}
