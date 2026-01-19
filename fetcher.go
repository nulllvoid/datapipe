package datapipe

import "context"

type Fetcher[T Entity, Req Request] interface {
	Name() string
	Priority() int
	CanHandle(req Req) bool
	Fetch(ctx context.Context, req Req) (results []T, hasMore bool, err error)
}

type BaseFetcher[T Entity, Req Request] struct {
	name     string
	priority int
}

func NewBaseFetcher[T Entity, Req Request](name string, priority int) BaseFetcher[T, Req] {
	return BaseFetcher[T, Req]{name: name, priority: priority}
}

func (f BaseFetcher[T, Req]) Name() string      { return f.name }
func (f BaseFetcher[T, Req]) Priority() int     { return f.priority }
func (f BaseFetcher[T, Req]) CanHandle(Req) bool { return true }

