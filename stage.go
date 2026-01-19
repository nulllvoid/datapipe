package datapipe

import "context"

type Stage[T Entity, Req Request] interface {
	Name() string
	Execute(ctx context.Context, state *State[T, Req]) error
	Required() bool
}

type FunctionalStage[T Entity, Req Request] struct {
	name     string
	fn       StageFunc[T, Req]
	required bool
}

func NewStage[T Entity, Req Request](name string, required bool, fn StageFunc[T, Req]) *FunctionalStage[T, Req] {
	return &FunctionalStage[T, Req]{
		name:     name,
		fn:       fn,
		required: required,
	}
}

func (s *FunctionalStage[T, Req]) Name() string   { return s.name }
func (s *FunctionalStage[T, Req]) Required() bool { return s.required }
func (s *FunctionalStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
	return s.fn(ctx, state)
}

