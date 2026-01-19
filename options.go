package datapipe

import (
	"context"
	"time"
)

type Option[T Entity, Req Request, Resp Response[T]] func(*Pipeline[T, Req, Resp])

func WithResponseBuilder[T Entity, Req Request, Resp Response[T]](builder ResponseBuilder[T, Req, Resp]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.responseBuilder = builder
	}
}

func WithTimeout[T Entity, Req Request, Resp Response[T]](d time.Duration) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.config.Timeout = d
	}
}

func WithFailFast[T Entity, Req Request, Resp Response[T]](enabled bool) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.config.FailFast = enabled
	}
}

func WithMetrics[T Entity, Req Request, Resp Response[T]](m Metrics) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.metrics = m
	}
}

func WithValidation[T Entity, Req Request, Resp Response[T]](fn func(Req) error) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		stage := NewStage[T, Req]("validation", true, func(ctx context.Context, state *State[T, Req]) error {
			return fn(state.Request())
		})
		p.stages = append([]Stage[T, Req]{stage}, p.stages...)
	}
}

func WithStage[T Entity, Req Request, Resp Response[T]](stage Stage[T, Req]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.AddStage(stage)
	}
}

func WithMiddleware[T Entity, Req Request, Resp Response[T]](m Middleware[T, Req]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.Use(m)
	}
}

func WithConfig[T Entity, Req Request, Resp Response[T]](cfg Config) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.config = cfg
	}
}

func WithFetcherChain[T Entity, Req Request, Resp Response[T]](chain *FetcherChain[T, Req]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.AddStage(NewFetchStage[T, Req](chain))
	}
}

func WithFetchers[T Entity, Req Request, Resp Response[T]](fetchers ...Fetcher[T, Req]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		opts := make([]FetcherChainOption[T, Req], 0, len(fetchers))
		for _, f := range fetchers {
			opts = append(opts, ChainWithFetcher[T, Req](f))
		}
		chain := NewFetcherChain(opts...)
		p.AddStage(NewFetchStage[T, Req](chain))
	}
}

func WithFetchersAndMode[T Entity, Req Request, Resp Response[T]](mode FallbackMode, fetchers ...Fetcher[T, Req]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		opts := make([]FetcherChainOption[T, Req], 0, len(fetchers)+1)
		opts = append(opts, ChainWithFallbackMode[T, Req](mode))
		for _, f := range fetchers {
			opts = append(opts, ChainWithFetcher[T, Req](f))
		}
		chain := NewFetcherChain(opts...)
		p.AddStage(NewFetchStage[T, Req](chain))
	}
}

func WithEnricherComposite[T Entity, Req Request, Resp Response[T]](enricher *CompositeEnricher[T]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		p.AddStage(NewEnrichStage[T, Req](enricher))
	}
}

func WithEnrichers[T Entity, Req Request, Resp Response[T]](enrichers ...Enricher[T]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		opts := make([]CompositeEnricherOption[T], 0, len(enrichers))
		for _, e := range enrichers {
			opts = append(opts, CompositeWithEnricher[T](e))
		}
		composite := NewCompositeEnricher(opts...)
		p.AddStage(NewEnrichStage[T, Req](composite))
	}
}

func WithParallelEnrichment[T Entity, Req Request, Resp Response[T]](parallel bool, maxWorkers int, enrichers ...Enricher[T]) Option[T, Req, Resp] {
	return func(p *Pipeline[T, Req, Resp]) {
		opts := make([]CompositeEnricherOption[T], 0, len(enrichers)+2)
		opts = append(opts, CompositeWithParallel[T](parallel))
		opts = append(opts, CompositeWithMaxWorkers[T](maxWorkers))
		for _, e := range enrichers {
			opts = append(opts, CompositeWithEnricher[T](e))
		}
		composite := NewCompositeEnricher(opts...)
		p.AddStage(NewEnrichStage[T, Req](composite))
	}
}
