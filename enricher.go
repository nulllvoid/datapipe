package datapipe

import "context"

type Enricher[T Entity] interface {
	Name() string
	CanEnrich(authType string, expandParams []string) bool
	Enrich(ctx context.Context, items []T) error
	Required() bool
}

type BaseEnricher[T Entity] struct {
	name         string
	allowedAuths []string
	expandKey    string
	required     bool
}

type EnricherOption[T Entity] func(*BaseEnricher[T])

func NewBaseEnricher[T Entity](name string, opts ...EnricherOption[T]) BaseEnricher[T] {
	e := BaseEnricher[T]{
		name:         name,
		allowedAuths: []string{"*"},
		required:     false,
	}

	for _, opt := range opts {
		opt(&e)
	}

	return e
}

func EnricherWithAllowedAuths[T Entity](auths ...string) EnricherOption[T] {
	return func(e *BaseEnricher[T]) {
		e.allowedAuths = auths
	}
}

func EnricherWithExpandKey[T Entity](key string) EnricherOption[T] {
	return func(e *BaseEnricher[T]) {
		e.expandKey = key
	}
}

func EnricherWithRequired[T Entity](required bool) EnricherOption[T] {
	return func(e *BaseEnricher[T]) {
		e.required = required
	}
}

func (e BaseEnricher[T]) Name() string   { return e.name }
func (e BaseEnricher[T]) Required() bool { return e.required }

func (e BaseEnricher[T]) CanEnrich(authType string, expandParams []string) bool {
	if !e.isAuthAllowed(authType) {
		return false
	}

	if e.expandKey == "" {
		return true
	}

	for _, p := range expandParams {
		if p == e.expandKey {
			return true
		}
	}

	return false
}

func (e BaseEnricher[T]) isAuthAllowed(authType string) bool {
	for _, a := range e.allowedAuths {
		if a == "*" || a == authType {
			return true
		}
	}
	return false
}

func (e BaseEnricher[T]) ExpandKey() string {
	return e.expandKey
}

func (e BaseEnricher[T]) AllowedAuths() []string {
	return e.allowedAuths
}

