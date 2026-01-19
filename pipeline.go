package datapipe

import (
	"context"
	"time"
)

type ResponseBuilder[T Entity, Req Request, Resp Response[T]] interface {
	Build(ctx context.Context, state *State[T, Req]) (Resp, error)
}

type DefaultResponseBuilder[T Entity, Req Request] struct{}

func (b *DefaultResponseBuilder[T, Req]) Build(ctx context.Context, state *State[T, Req]) (*CollectionResponse[T], error) {
	return NewCollectionResponse(state.Results(), state.HasMore()), nil
}

type Pipeline[T Entity, Req Request, Resp Response[T]] struct {
	name            string
	stages          []Stage[T, Req]
	responseBuilder ResponseBuilder[T, Req, Resp]
	middleware      []Middleware[T, Req]
	config          Config
	metrics         Metrics
}

func New[T Entity, Req Request, Resp Response[T]](name string, opts ...Option[T, Req, Resp]) *Pipeline[T, Req, Resp] {
	p := &Pipeline[T, Req, Resp]{
		name:   name,
		stages: make([]Stage[T, Req], 0),
		config: DefaultConfig(),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pipeline[T, Req, Resp]) Execute(ctx context.Context, req Req) (Resp, error) {
	var zero Resp

	if p.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.Timeout)
		defer cancel()
	}

	state := NewState[T, Req](req)

	for _, stage := range p.stages {
		if ctx.Err() != nil {
			return zero, NewPipelineError(p.name, stage.Name(), "execute", ctx.Err())
		}

		stageStart := time.Now()
		err := p.executeStage(ctx, stage, state)
		stageDuration := time.Since(stageStart)

		if p.metrics != nil {
			p.metrics.RecordStageDuration(p.name, stage.Name(), stageDuration)
		}

		if err != nil {
			if p.metrics != nil {
				p.metrics.RecordError(p.name, stage.Name(), "execution_error")
			}
			if stage.Required() || p.config.FailFast {
				return zero, NewPipelineError(p.name, stage.Name(), "execute", err)
			}
			state.AddError(err)
		}
	}

	if p.metrics != nil {
		p.metrics.RecordResultCount(p.name, state.ResultCount())
		if state.Source() != "" {
			p.metrics.RecordFetchSource(p.name, state.Source())
		}
	}

	if p.responseBuilder != nil {
		return p.responseBuilder.Build(ctx, state)
	}

	return zero, nil
}

func (p *Pipeline[T, Req, Resp]) executeStage(ctx context.Context, stage Stage[T, Req], state *State[T, Req]) error {
	execute := stage.Execute

	for i := len(p.middleware) - 1; i >= 0; i-- {
		execute = p.middleware[i](stage.Name(), execute)
	}

	return execute(ctx, state)
}

func (p *Pipeline[T, Req, Resp]) AddStage(stage Stage[T, Req]) {
	p.stages = append(p.stages, stage)
}

func (p *Pipeline[T, Req, Resp]) Use(m Middleware[T, Req]) {
	p.middleware = append(p.middleware, m)
}

func (p *Pipeline[T, Req, Resp]) Name() string {
	return p.name
}

func (p *Pipeline[T, Req, Resp]) StageCount() int {
	return len(p.stages)
}
