package datapipe

import (
	"context"
	"sort"
)

type FallbackMode int

const (
	FallbackSequential FallbackMode = iota
	FallbackFirstSuccess
	FallbackParallel
)

type AggregateMode int

const (
	AggregateFirst AggregateMode = iota
	AggregateMerge
	AggregateDedupe
)

type FetcherChain[T Entity, Req Request] struct {
	fetchers      []Fetcher[T, Req]
	fallbackMode  FallbackMode
	aggregateMode AggregateMode
	minResults    int
}

type FetcherChainOption[T Entity, Req Request] func(*FetcherChain[T, Req])

func NewFetcherChain[T Entity, Req Request](opts ...FetcherChainOption[T, Req]) *FetcherChain[T, Req] {
	chain := &FetcherChain[T, Req]{
		fetchers:      make([]Fetcher[T, Req], 0),
		fallbackMode:  FallbackSequential,
		aggregateMode: AggregateFirst,
		minResults:    0,
	}

	for _, opt := range opts {
		opt(chain)
	}

	sort.Slice(chain.fetchers, func(i, j int) bool {
		return chain.fetchers[i].Priority() < chain.fetchers[j].Priority()
	})

	return chain
}

func ChainWithFetcher[T Entity, Req Request](f Fetcher[T, Req]) FetcherChainOption[T, Req] {
	return func(c *FetcherChain[T, Req]) {
		c.fetchers = append(c.fetchers, f)
	}
}

func ChainWithFallbackMode[T Entity, Req Request](mode FallbackMode) FetcherChainOption[T, Req] {
	return func(c *FetcherChain[T, Req]) {
		c.fallbackMode = mode
	}
}

func ChainWithAggregateMode[T Entity, Req Request](mode AggregateMode) FetcherChainOption[T, Req] {
	return func(c *FetcherChain[T, Req]) {
		c.aggregateMode = mode
	}
}

func ChainWithMinResults[T Entity, Req Request](min int) FetcherChainOption[T, Req] {
	return func(c *FetcherChain[T, Req]) {
		c.minResults = min
	}
}

func (c *FetcherChain[T, Req]) Fetch(ctx context.Context, req Req) ([]T, bool, string, error) {
	switch c.fallbackMode {
	case FallbackParallel:
		return c.fetchParallel(ctx, req)
	case FallbackFirstSuccess:
		return c.fetchFirstSuccess(ctx, req)
	default:
		return c.fetchSequential(ctx, req)
	}
}

func (c *FetcherChain[T, Req]) FetcherCount() int {
	return len(c.fetchers)
}

func (c *FetcherChain[T, Req]) fetchSequential(ctx context.Context, req Req) ([]T, bool, string, error) {
	var allResults []T
	var hasMore bool
	var source string
	var lastErr error

	for _, fetcher := range c.fetchers {
		if ctx.Err() != nil {
			return nil, false, "", ctx.Err()
		}

		if !fetcher.CanHandle(req) {
			continue
		}

		results, more, err := fetcher.Fetch(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}

		if len(results) > 0 {
			allResults = append(allResults, results...)
			hasMore = more
			source = fetcher.Name()

			if c.minResults == 0 || len(allResults) >= c.minResults {
				return allResults, hasMore, source, nil
			}
		}
	}

	if len(allResults) > 0 {
		return allResults, hasMore, source, nil
	}

	if lastErr != nil {
		return nil, false, "", lastErr
	}

	return nil, false, "", ErrNoFetcherAvailable
}

func (c *FetcherChain[T, Req]) fetchFirstSuccess(ctx context.Context, req Req) ([]T, bool, string, error) {
	var lastErr error

	for _, fetcher := range c.fetchers {
		if ctx.Err() != nil {
			return nil, false, "", ctx.Err()
		}

		if !fetcher.CanHandle(req) {
			continue
		}

		results, hasMore, err := fetcher.Fetch(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}

		if len(results) > 0 {
			return results, hasMore, fetcher.Name(), nil
		}
	}

	if lastErr != nil {
		return nil, false, "", lastErr
	}

	return []T{}, false, "", nil
}

func (c *FetcherChain[T, Req]) fetchParallel(ctx context.Context, req Req) ([]T, bool, string, error) {
	type fetchResult struct {
		results  []T
		hasMore  bool
		source   string
		priority int
		err      error
	}

	eligible := make([]Fetcher[T, Req], 0)
	for _, f := range c.fetchers {
		if f.CanHandle(req) {
			eligible = append(eligible, f)
		}
	}

	if len(eligible) == 0 {
		return nil, false, "", ErrNoFetcherAvailable
	}

	resultCh := make(chan fetchResult, len(eligible))

	for _, fetcher := range eligible {
		go func(f Fetcher[T, Req]) {
			results, hasMore, err := f.Fetch(ctx, req)
			resultCh <- fetchResult{
				results:  results,
				hasMore:  hasMore,
				source:   f.Name(),
				priority: f.Priority(),
				err:      err,
			}
		}(fetcher)
	}

	var allResults []fetchResult
	for i := 0; i < len(eligible); i++ {
		select {
		case r := <-resultCh:
			allResults = append(allResults, r)
		case <-ctx.Done():
			return nil, false, "", ctx.Err()
		}
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].priority < allResults[j].priority
	})

	for _, r := range allResults {
		if r.err == nil && len(r.results) > 0 {
			return r.results, r.hasMore, r.source, nil
		}
	}

	for _, r := range allResults {
		if r.err != nil {
			return nil, false, "", r.err
		}
	}

	return []T{}, false, "", nil
}

