package datapipe

import "sync"

type State[T Entity, Req Request] struct {
	mu       sync.RWMutex
	request  Req
	results  []T
	hasMore  bool
	source   string
	metadata map[string]any
	errors   []error
}

func NewState[T Entity, Req Request](req Req) *State[T, Req] {
	return &State[T, Req]{
		request:  req,
		results:  make([]T, 0),
		metadata: make(map[string]any),
		errors:   make([]error, 0),
	}
}

func (s *State[T, Req]) Request() Req {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.request
}

func (s *State[T, Req]) SetRequest(req Req) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.request = req
}

func (s *State[T, Req]) Results() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.results
}

func (s *State[T, Req]) SetResults(results []T, hasMore bool, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = results
	s.hasMore = hasMore
	s.source = source
}

func (s *State[T, Req]) AppendResults(results []T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, results...)
}

func (s *State[T, Req]) HasMore() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasMore
}

func (s *State[T, Req]) Source() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.source
}

func (s *State[T, Req]) SetMetadata(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata[key] = value
}

func (s *State[T, Req]) GetMetadata(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.metadata[key]
	return v, ok
}

func (s *State[T, Req]) AddError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, err)
}

func (s *State[T, Req]) Errors() []error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errors
}

func (s *State[T, Req]) HasErrors() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.errors) > 0
}

func (s *State[T, Req]) ResultCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.results)
}
