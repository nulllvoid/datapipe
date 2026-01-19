package datapipe

import (
	"context"
	"sync"
)

type CompositeEnricher[T Entity] struct {
	enrichers  []Enricher[T]
	parallel   bool
	maxWorkers int
}

type CompositeEnricherOption[T Entity] func(*CompositeEnricher[T])

func NewCompositeEnricher[T Entity](opts ...CompositeEnricherOption[T]) *CompositeEnricher[T] {
	c := &CompositeEnricher[T]{
		enrichers:  make([]Enricher[T], 0),
		parallel:   true,
		maxWorkers: 10,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func CompositeWithEnricher[T Entity](e Enricher[T]) CompositeEnricherOption[T] {
	return func(c *CompositeEnricher[T]) {
		c.enrichers = append(c.enrichers, e)
	}
}

func CompositeWithParallel[T Entity](parallel bool) CompositeEnricherOption[T] {
	return func(c *CompositeEnricher[T]) {
		c.parallel = parallel
	}
}

func CompositeWithMaxWorkers[T Entity](max int) CompositeEnricherOption[T] {
	return func(c *CompositeEnricher[T]) {
		c.maxWorkers = max
	}
}

func (c *CompositeEnricher[T]) Enrich(ctx context.Context, items []T, authType string, expandParams []string) []error {
	eligible := make([]Enricher[T], 0)
	for _, e := range c.enrichers {
		if e.CanEnrich(authType, expandParams) {
			eligible = append(eligible, e)
		}
	}

	if len(eligible) == 0 || len(items) == 0 {
		return nil
	}

	if c.parallel {
		return c.enrichParallel(ctx, items, eligible)
	}

	return c.enrichSequential(ctx, items, eligible)
}

func (c *CompositeEnricher[T]) EnricherCount() int {
	return len(c.enrichers)
}

func (c *CompositeEnricher[T]) IsParallel() bool {
	return c.parallel
}

func (c *CompositeEnricher[T]) enrichSequential(ctx context.Context, items []T, enrichers []Enricher[T]) []error {
	var errs []error

	for _, enricher := range enrichers {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			return errs
		}

		if err := enricher.Enrich(ctx, items); err != nil {
			if enricher.Required() {
				return []error{NewEnrichmentError(enricher.Name(), "required enrichment failed", err)}
			}
			errs = append(errs, NewEnrichmentError(enricher.Name(), "enrichment failed", err))
		}
	}

	return errs
}

func (c *CompositeEnricher[T]) enrichParallel(ctx context.Context, items []T, enrichers []Enricher[T]) []error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		sem  = make(chan struct{}, c.maxWorkers)
	)

	for _, enricher := range enrichers {
		wg.Add(1)
		go func(e Enricher[T]) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				errs = append(errs, ctx.Err())
				mu.Unlock()
				return
			}

			if ctx.Err() != nil {
				return
			}

			if err := e.Enrich(ctx, items); err != nil {
				mu.Lock()
				errs = append(errs, NewEnrichmentError(e.Name(), "enrichment failed", err))
				mu.Unlock()
			}
		}(enricher)
	}

	wg.Wait()
	return errs
}

