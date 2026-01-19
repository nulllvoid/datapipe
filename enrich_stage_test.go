package datapipe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestEnrichStage_Name(t *testing.T) {
	t.Parallel()

	composite := datapipe.NewCompositeEnricher[testEntity]()
	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)

	if stage.Name() != "enrich" {
		t.Errorf("Name() = %v, want enrich", stage.Name())
	}
}

func TestEnrichStage_Required(t *testing.T) {
	t.Parallel()

	composite := datapipe.NewCompositeEnricher[testEntity]()
	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)

	if stage.Required() {
		t.Error("EnrichStage should NOT be required")
	}
}

func TestEnrichStage_Execute_Success(t *testing.T) {
	t.Parallel()

	enricher := newMockEnricher[testEntity]("test")
	enricher.enrichFunc = func(ctx context.Context, items []testEntity) error {
		for i := range items {
			items[i].Name = "enriched"
		}
		return nil
	}

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](enricher),
	)

	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	state.SetResults([]testEntity{{ID: "1", Name: "original"}}, false, "test")

	err := stage.Execute(context.Background(), state)

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if enricher.enrichCalls != 1 {
		t.Error("Enricher should have been called")
	}
}

func TestEnrichStage_Execute_EmptyResults(t *testing.T) {
	t.Parallel()

	enricher := newMockEnricher[testEntity]("test")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](enricher),
	)

	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	err := stage.Execute(context.Background(), state)

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if enricher.enrichCalls != 0 {
		t.Error("Enricher should NOT have been called for empty results")
	}
}

func TestEnrichStage_Execute_CollectsErrors(t *testing.T) {
	t.Parallel()

	enricher := newMockEnricher[testEntity]("failing")
	enricher.enrichFunc = func(ctx context.Context, items []testEntity) error {
		return errors.New("enrichment failed")
	}

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](enricher),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	state.SetResults([]testEntity{{ID: "1"}}, false, "test")

	err := stage.Execute(context.Background(), state)

	if err != nil {
		t.Fatal("Execute() should not return error for optional enrichment failures")
	}
	if !state.HasErrors() {
		t.Error("State should have collected the enrichment error")
	}
	if len(state.Errors()) != 1 {
		t.Errorf("Expected 1 error in state, got %d", len(state.Errors()))
	}
}

type authenticatedRequest struct {
	testRequest
	AuthType string
}

func (r *authenticatedRequest) GetAuthType() string { return r.AuthType }

func TestEnrichStage_ExtractsAuthType(t *testing.T) {
	t.Parallel()

	privateEnricher := newMockEnricher[testEntity]("private",
		datapipe.EnricherWithAllowedAuths[testEntity]("private"),
	)
	publicEnricher := newMockEnricher[testEntity]("public",
		datapipe.EnricherWithAllowedAuths[testEntity]("public"),
	)

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](privateEnricher),
		datapipe.CompositeWithEnricher[testEntity](publicEnricher),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	stage := datapipe.NewEnrichStage[testEntity, *authenticatedRequest](composite)
	state := datapipe.NewState[testEntity, *authenticatedRequest](&authenticatedRequest{
		testRequest: testRequest{Limit: 10},
		AuthType:    "private",
	})
	state.SetResults([]testEntity{{ID: "1"}}, false, "test")

	_ = stage.Execute(context.Background(), state)

	if privateEnricher.enrichCalls != 1 {
		t.Error("Private enricher should have been called")
	}
	if publicEnricher.enrichCalls != 0 {
		t.Error("Public enricher should NOT have been called")
	}
}

type expandableRequest struct {
	testRequest
	ExpandParams []string
}

func (r *expandableRequest) GetExpandParams() []string { return r.ExpandParams }

func TestEnrichStage_ExtractsExpandParams_FromRequest(t *testing.T) {
	t.Parallel()

	userEnricher := newMockEnricher[testEntity]("user",
		datapipe.EnricherWithExpandKey[testEntity]("user"),
	)
	accountEnricher := newMockEnricher[testEntity]("account",
		datapipe.EnricherWithExpandKey[testEntity]("account"),
	)

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](userEnricher),
		datapipe.CompositeWithEnricher[testEntity](accountEnricher),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	stage := datapipe.NewEnrichStage[testEntity, *expandableRequest](composite)
	state := datapipe.NewState[testEntity, *expandableRequest](&expandableRequest{
		testRequest:  testRequest{Limit: 10},
		ExpandParams: []string{"user"},
	})
	state.SetResults([]testEntity{{ID: "1"}}, false, "test")

	_ = stage.Execute(context.Background(), state)

	if userEnricher.enrichCalls != 1 {
		t.Error("User enricher should have been called")
	}
	if accountEnricher.enrichCalls != 0 {
		t.Error("Account enricher should NOT have been called")
	}
}

func TestEnrichStage_ExtractsExpandParams_FromMetadata(t *testing.T) {
	t.Parallel()

	userEnricher := newMockEnricher[testEntity]("user",
		datapipe.EnricherWithExpandKey[testEntity]("user"),
	)

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](userEnricher),
	)

	stage := datapipe.NewEnrichStage[testEntity, *testRequest](composite)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	state.SetResults([]testEntity{{ID: "1"}}, false, "test")
	state.SetMetadata("expand", []string{"user"})

	_ = stage.Execute(context.Background(), state)

	if userEnricher.enrichCalls != 1 {
		t.Error("User enricher should have been called via metadata expand")
	}
}

func TestPipeline_WithEnrichers(t *testing.T) {
	t.Parallel()

	fetcher := newMockFetcher[testEntity, *testRequest]("db", 1, []testEntity{{ID: "1"}}, false, nil)
	enricher := newMockEnricher[testEntity]("test")

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithFetchers[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](fetcher),
		datapipe.WithEnrichers[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](enricher),
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
	if enricher.enrichCalls != 1 {
		t.Error("Enricher should have been called")
	}
}

func TestPipeline_WithParallelEnrichment(t *testing.T) {
	t.Parallel()

	fetcher := newMockFetcher[testEntity, *testRequest]("db", 1, []testEntity{{ID: "1"}}, false, nil)
	e1 := newMockEnricher[testEntity]("e1")
	e2 := newMockEnricher[testEntity]("e2")

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithFetchers[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](fetcher),
		datapipe.WithParallelEnrichment[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](true, 5, e1, e2),
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
	if e1.enrichCalls != 1 || e2.enrichCalls != 1 {
		t.Error("Both enrichers should have been called")
	}
}

