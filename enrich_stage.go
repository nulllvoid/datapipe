package datapipe

import "context"

type ExpandableRequest interface {
	Request
	GetExpandParams() []string
}

type EnrichStage[T Entity, Req Request] struct {
	enricher *CompositeEnricher[T]
}

func NewEnrichStage[T Entity, Req Request](enricher *CompositeEnricher[T]) *EnrichStage[T, Req] {
	return &EnrichStage[T, Req]{enricher: enricher}
}

func (s *EnrichStage[T, Req]) Name() string   { return "enrich" }
func (s *EnrichStage[T, Req]) Required() bool { return false }

func (s *EnrichStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
	results := state.Results()
	if len(results) == 0 {
		return nil
	}

	authType := s.extractAuthType(state)
	expandParams := s.extractExpandParams(state)

	errs := s.enricher.Enrich(ctx, results, authType, expandParams)
	for _, err := range errs {
		state.AddError(err)
	}

	return nil
}

func (s *EnrichStage[T, Req]) extractAuthType(state *State[T, Req]) string {
	if ar, ok := any(state.Request()).(AuthenticatedRequest); ok {
		return ar.GetAuthType()
	}
	return ""
}

func (s *EnrichStage[T, Req]) extractExpandParams(state *State[T, Req]) []string {
	if er, ok := any(state.Request()).(ExpandableRequest); ok {
		return er.GetExpandParams()
	}

	if expand, ok := state.GetMetadata("expand"); ok {
		if params, ok := expand.([]string); ok {
			return params
		}
	}

	return nil
}
