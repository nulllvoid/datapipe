package datapipe_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nulllvoid/datapipe"
)

func TestCompositeEnricher_NewCompositeEnricher(t *testing.T) {
	t.Parallel()

	composite := datapipe.NewCompositeEnricher[testEntity]()

	if composite.EnricherCount() != 0 {
		t.Errorf("EnricherCount() = %v, want 0", composite.EnricherCount())
	}
	if !composite.IsParallel() {
		t.Error("IsParallel() should be true by default")
	}
}

func TestCompositeEnricher_WithEnrichers(t *testing.T) {
	t.Parallel()

	e1 := newMockEnricher[testEntity]("e1")
	e2 := newMockEnricher[testEntity]("e2")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e1),
		datapipe.CompositeWithEnricher[testEntity](e2),
	)

	if composite.EnricherCount() != 2 {
		t.Errorf("EnricherCount() = %v, want 2", composite.EnricherCount())
	}
}

func TestCompositeEnricher_Sequential(t *testing.T) {
	t.Parallel()

	callOrder := make([]string, 0)
	e1 := newMockEnricher[testEntity]("first")
	e1.enrichFunc = func(ctx context.Context, items []testEntity) error {
		callOrder = append(callOrder, "first")
		return nil
	}
	e2 := newMockEnricher[testEntity]("second")
	e2.enrichFunc = func(ctx context.Context, items []testEntity) error {
		callOrder = append(callOrder, "second")
		return nil
	}

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e1),
		datapipe.CompositeWithEnricher[testEntity](e2),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	items := []testEntity{{ID: "1"}}
	errs := composite.Enrich(context.Background(), items, "", nil)

	if len(errs) != 0 {
		t.Errorf("Enrich() errors = %v, want none", errs)
	}
	if len(callOrder) != 2 {
		t.Fatalf("Expected 2 calls, got %d", len(callOrder))
	}
	if callOrder[0] != "first" || callOrder[1] != "second" {
		t.Errorf("Call order = %v, want [first, second]", callOrder)
	}
}

func TestCompositeEnricher_Parallel(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	e1 := newMockEnricher[testEntity]("e1")
	e1.enrichFunc = func(ctx context.Context, items []testEntity) error {
		callCount.Add(1)
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	e2 := newMockEnricher[testEntity]("e2")
	e2.enrichFunc = func(ctx context.Context, items []testEntity) error {
		callCount.Add(1)
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e1),
		datapipe.CompositeWithEnricher[testEntity](e2),
		datapipe.CompositeWithParallel[testEntity](true),
	)

	items := []testEntity{{ID: "1"}}
	start := time.Now()
	errs := composite.Enrich(context.Background(), items, "", nil)
	elapsed := time.Since(start)

	if len(errs) != 0 {
		t.Errorf("Enrich() errors = %v, want none", errs)
	}
	if callCount.Load() != 2 {
		t.Errorf("Call count = %v, want 2", callCount.Load())
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("Parallel execution took %v, expected < 50ms", elapsed)
	}
}

func TestCompositeEnricher_MaxWorkers(t *testing.T) {
	t.Parallel()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	enrichers := make([]datapipe.Enricher[testEntity], 5)
	for i := 0; i < 5; i++ {
		e := newMockEnricher[testEntity]("e")
		e.enrichFunc = func(ctx context.Context, items []testEntity) error {
			current := concurrent.Add(1)
			for {
				max := maxConcurrent.Load()
				if current <= max {
					break
				}
				if maxConcurrent.CompareAndSwap(max, current) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			concurrent.Add(-1)
			return nil
		}
		enrichers[i] = e
	}

	opts := make([]datapipe.CompositeEnricherOption[testEntity], 0)
	opts = append(opts, datapipe.CompositeWithMaxWorkers[testEntity](2))
	for _, e := range enrichers {
		opts = append(opts, datapipe.CompositeWithEnricher[testEntity](e))
	}
	composite := datapipe.NewCompositeEnricher[testEntity](opts...)

	items := []testEntity{{ID: "1"}}
	composite.Enrich(context.Background(), items, "", nil)

	if maxConcurrent.Load() > 2 {
		t.Errorf("Max concurrent = %v, expected <= 2", maxConcurrent.Load())
	}
}

func TestCompositeEnricher_ErrorHandling_Optional(t *testing.T) {
	t.Parallel()

	e1 := newMockEnricher[testEntity]("failing")
	e1.enrichFunc = func(ctx context.Context, items []testEntity) error {
		return errors.New("enrichment failed")
	}
	e2 := newMockEnricher[testEntity]("succeeding")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e1),
		datapipe.CompositeWithEnricher[testEntity](e2),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	items := []testEntity{{ID: "1"}}
	errs := composite.Enrich(context.Background(), items, "", nil)

	if len(errs) != 1 {
		t.Errorf("Expected 1 error, got %d", len(errs))
	}
	if e2.enrichCalls != 1 {
		t.Error("Second enricher should have been called")
	}
}

func TestCompositeEnricher_ErrorHandling_Required_Sequential(t *testing.T) {
	t.Parallel()

	e1 := newMockEnricher[testEntity]("required",
		datapipe.EnricherWithRequired[testEntity](true),
	)
	e1.enrichFunc = func(ctx context.Context, items []testEntity) error {
		return errors.New("critical failure")
	}
	e2 := newMockEnricher[testEntity]("after")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e1),
		datapipe.CompositeWithEnricher[testEntity](e2),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	items := []testEntity{{ID: "1"}}
	errs := composite.Enrich(context.Background(), items, "", nil)

	if len(errs) != 1 {
		t.Errorf("Expected 1 error, got %d", len(errs))
	}
	if e2.enrichCalls != 0 {
		t.Error("Second enricher should NOT have been called after required failure")
	}
}

func TestCompositeEnricher_AuthFiltering(t *testing.T) {
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

	items := []testEntity{{ID: "1"}}

	t.Run("private auth only runs private enricher", func(t *testing.T) {
		privateEnricher.enrichCalls = 0
		publicEnricher.enrichCalls = 0

		composite.Enrich(context.Background(), items, "private", nil)

		if privateEnricher.enrichCalls != 1 {
			t.Error("Private enricher should have been called")
		}
		if publicEnricher.enrichCalls != 0 {
			t.Error("Public enricher should NOT have been called")
		}
	})

	t.Run("public auth only runs public enricher", func(t *testing.T) {
		privateEnricher.enrichCalls = 0
		publicEnricher.enrichCalls = 0

		composite.Enrich(context.Background(), items, "public", nil)

		if privateEnricher.enrichCalls != 0 {
			t.Error("Private enricher should NOT have been called")
		}
		if publicEnricher.enrichCalls != 1 {
			t.Error("Public enricher should have been called")
		}
	})
}

func TestCompositeEnricher_ExpandFiltering(t *testing.T) {
	t.Parallel()

	userEnricher := newMockEnricher[testEntity]("user",
		datapipe.EnricherWithExpandKey[testEntity]("user"),
	)
	accountEnricher := newMockEnricher[testEntity]("account",
		datapipe.EnricherWithExpandKey[testEntity]("account"),
	)
	alwaysEnricher := newMockEnricher[testEntity]("always")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](userEnricher),
		datapipe.CompositeWithEnricher[testEntity](accountEnricher),
		datapipe.CompositeWithEnricher[testEntity](alwaysEnricher),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	items := []testEntity{{ID: "1"}}

	composite.Enrich(context.Background(), items, "", []string{"user"})

	if userEnricher.enrichCalls != 1 {
		t.Error("User enricher should have been called")
	}
	if accountEnricher.enrichCalls != 0 {
		t.Error("Account enricher should NOT have been called")
	}
	if alwaysEnricher.enrichCalls != 1 {
		t.Error("Always enricher should have been called")
	}
}

func TestCompositeEnricher_EmptyItems(t *testing.T) {
	t.Parallel()

	e := newMockEnricher[testEntity]("e")

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e),
	)

	errs := composite.Enrich(context.Background(), []testEntity{}, "", nil)

	if len(errs) != 0 {
		t.Errorf("Expected no errors, got %v", errs)
	}
	if e.enrichCalls != 0 {
		t.Error("Enricher should NOT have been called for empty items")
	}
}

func TestCompositeEnricher_ContextCancellation(t *testing.T) {
	t.Parallel()

	e := newMockEnricher[testEntity]("slow")
	e.enrichFunc = func(ctx context.Context, items []testEntity) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	composite := datapipe.NewCompositeEnricher[testEntity](
		datapipe.CompositeWithEnricher[testEntity](e),
		datapipe.CompositeWithParallel[testEntity](false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	items := []testEntity{{ID: "1"}}
	errs := composite.Enrich(ctx, items, "", nil)

	if len(errs) == 0 {
		t.Error("Expected context cancellation error")
	}
}

