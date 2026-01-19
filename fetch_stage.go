package datapipe

import "context"

type FetchStage[T Entity, Req Request] struct {
	chain *FetcherChain[T, Req]
}

func NewFetchStage[T Entity, Req Request](chain *FetcherChain[T, Req]) *FetchStage[T, Req] {
	return &FetchStage[T, Req]{chain: chain}
}

func (s *FetchStage[T, Req]) Name() string   { return "fetch" }
func (s *FetchStage[T, Req]) Required() bool { return true }

func (s *FetchStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
	results, hasMore, source, err := s.chain.Fetch(ctx, state.Request())
	if err != nil {
		return err
	}

	state.SetResults(results, hasMore, source)
	return nil
}
