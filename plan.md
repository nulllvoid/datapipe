# DataPipe: Implementation Plan


## Building a Generic Multi-Source Data Pipeline Package


---


## Overview


This document outlines the step-by-step plan to build **DataPipe** - a generic, open-source Go package for multi-source data fetching with search and enrichment support.


### Package Identity


| Attribute | Value |
|-----------|-------|
| **Name** | `datapipe` |
| **Import Path** | `github.com/razorpay/datapipe` |
| **Go Version** | 1.21+ (generics required) |
| **Dependencies** | stdlib only (+ optional OTel integration) |


---


## Phase 1: Core Foundation


### 1.1 Entity Interface


**File:** `entity.go`


```go
package datapipe


type Entity interface {
   GetID() string
}
```


**Design Rationale:**
- Minimal interface for maximum flexibility
- Any struct with an ID can be fetched
- No reflection, compile-time type safety via generics


---


### 1.2 Request Interface


**File:** `request.go`


```go
package datapipe


type Request interface {
   GetLimit() int
   GetOffset() int
   Clone() Request
}


type AuthenticatedRequest interface {
   Request
   GetAuthType() string
}


type SearchableRequest interface {
   Request
   GetSearchQuery() string
   HasSearchQuery() bool
}


type FilterableRequest interface {
   Request
   GetFilters() map[string]any
}


type SortableRequest interface {
   Request
   GetSortField() string
   GetSortOrder() SortOrder
}


type SortOrder string


const (
   SortAsc  SortOrder = "asc"
   SortDesc SortOrder = "desc"
)
```


**Design Rationale:**
- Base `Request` is minimal - just pagination
- Composed interfaces for optional capabilities
- Fetchers/enrichers can type-assert for capabilities they need
- `Clone()` enables safe request modification in pipeline


---


### 1.3 Response Interface


**File:** `response.go`


```go
package datapipe


type Response[T Entity] interface {
   GetItems() []T
   GetCount() int
   HasMore() bool
}


type CollectionResponse[T Entity] struct {
   Entity  string `json:"entity"`
   Count   int    `json:"count"`
   HasMore bool   `json:"has_more"`
   Items   []T    `json:"items"`
}


func (r *CollectionResponse[T]) GetItems() []T { return r.Items }
func (r *CollectionResponse[T]) GetCount() int { return r.Count }
func (r *CollectionResponse[T]) HasMore() bool { return r.HasMore }


func NewCollectionResponse[T Entity](items []T, hasMore bool) *CollectionResponse[T] {
   return &CollectionResponse[T]{
       Entity:  "collection",
       Count:   len(items),
       HasMore: hasMore,
       Items:   items,
   }
}
```


**Design Rationale:**
- Generic response interface
- Built-in `CollectionResponse` for common use case
- JSON tags for direct API serialization


---


### 1.4 State Management


**File:** `state.go`


```go
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
```


**Design Rationale:**
- Thread-safe for parallel enrichment
- Carries all data through pipeline
- Metadata for inter-stage communication
- Error collection for non-fatal failures


---


### 1.5 Stage Interface


**File:** `stage.go`


```go
package datapipe


import "context"


type Stage[T Entity, Req Request] interface {
   Name() string
   Execute(ctx context.Context, state *State[T, Req]) error
   Required() bool
}


type StageFunc[T Entity, Req Request] func(ctx context.Context, state *State[T, Req]) error


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


func (s *FunctionalStage[T, Req]) Name() string { return s.name }
func (s *FunctionalStage[T, Req]) Required() bool { return s.required }
func (s *FunctionalStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
   return s.fn(ctx, state)
}
```


**Design Rationale:**
- Simple interface with three methods
- `Required()` determines if error stops pipeline
- `FunctionalStage` for inline stage creation


---


### 1.6 Error Types


**File:** `errors.go`


```go
package datapipe


import (
   "errors"
   "fmt"
)


var (
   ErrNoFetcherAvailable = errors.New("no fetcher available for request")
   ErrValidationFailed   = errors.New("validation failed")
   ErrFetchFailed        = errors.New("fetch operation failed")
   ErrEnrichmentFailed   = errors.New("enrichment failed")
   ErrTimeout            = errors.New("operation timed out")
   ErrCanceled           = errors.New("operation canceled")
)


type PipelineError struct {
   Pipeline string
   Stage    string
   Op       string
   Err      error
}


func (e *PipelineError) Error() string {
   if e.Stage != "" {
       return fmt.Sprintf("pipeline %s: stage %s: %s: %v", e.Pipeline, e.Stage, e.Op, e.Err)
   }
   return fmt.Sprintf("pipeline %s: %s: %v", e.Pipeline, e.Op, e.Err)
}


func (e *PipelineError) Unwrap() error {
   return e.Err
}


func NewPipelineError(pipeline, stage, op string, err error) *PipelineError {
   return &PipelineError{Pipeline: pipeline, Stage: stage, Op: op, Err: err}
}


type ValidationError struct {
   Field   string
   Message string
}


func (e *ValidationError) Error() string {
   return fmt.Sprintf("validation error: field '%s': %s", e.Field, e.Message)
}


func NewValidationError(field, message string) *ValidationError {
   return &ValidationError{Field: field, Message: message}
}


type FetchError struct {
   Source  string
   Message string
   Err     error
}


func (e *FetchError) Error() string {
   if e.Err != nil {
       return fmt.Sprintf("fetch error from %s: %s: %v", e.Source, e.Message, e.Err)
   }
   return fmt.Sprintf("fetch error from %s: %s", e.Source, e.Message)
}


func (e *FetchError) Unwrap() error {
   return e.Err
}


func NewFetchError(source, message string, err error) *FetchError {
   return &FetchError{Source: source, Message: message, Err: err}
}


type EnrichmentError struct {
   Enricher string
   Message  string
   Err      error
}


func (e *EnrichmentError) Error() string {
   if e.Err != nil {
       return fmt.Sprintf("enrichment error from %s: %s: %v", e.Enricher, e.Message, e.Err)
   }
   return fmt.Sprintf("enrichment error from %s: %s", e.Enricher, e.Message)
}


func (e *EnrichmentError) Unwrap() error {
   return e.Err
}


func NewEnrichmentError(enricher, message string, err error) *EnrichmentError {
   return &EnrichmentError{Enricher: enricher, Message: message, Err: err}
}
```


---


### 1.7 Pipeline Core


**File:** `pipeline.go`


```go
package datapipe


import (
   "context"
   "time"
)


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
           if stage.Required() || p.config.FailFast {
               return zero, NewPipelineError(p.name, stage.Name(), "execute", err)
           }
           state.AddError(err)
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
```


---


### 1.8 Configuration


**File:** `config.go`


```go
package datapipe


import "time"


type Config struct {
   Timeout     time.Duration
   FailFast    bool
   MaxRetries  int
   RetryDelay  time.Duration
}


func DefaultConfig() Config {
   return Config{
       Timeout:     30 * time.Second,
       FailFast:    false,
       MaxRetries:  0,
       RetryDelay:  100 * time.Millisecond,
   }
}
```


---


## Phase 2: Fetcher System


### 2.1 Fetcher Interface


**File:** `fetcher.go`


```go
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


func (f BaseFetcher[T, Req]) Name() string     { return f.name }
func (f BaseFetcher[T, Req]) Priority() int    { return f.priority }
func (f BaseFetcher[T, Req]) CanHandle(Req) bool { return true }
```


---


### 2.2 Fetcher Chain


**File:** `fetcher_chain.go`


```go
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


func WithFetcher[T Entity, Req Request](f Fetcher[T, Req]) FetcherChainOption[T, Req] {
   return func(c *FetcherChain[T, Req]) {
       c.fetchers = append(c.fetchers, f)
   }
}


func WithFallbackMode[T Entity, Req Request](mode FallbackMode) FetcherChainOption[T, Req] {
   return func(c *FetcherChain[T, Req]) {
       c.fallbackMode = mode
   }
}


func WithAggregateMode[T Entity, Req Request](mode AggregateMode) FetcherChainOption[T, Req] {
   return func(c *FetcherChain[T, Req]) {
       c.aggregateMode = mode
   }
}


func WithMinResults[T Entity, Req Request](min int) FetcherChainOption[T, Req] {
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


func (c *FetcherChain[T, Req]) fetchSequential(ctx context.Context, req Req) ([]T, bool, string, error) {
   var allResults []T
   var hasMore bool
   var source string
   var lastErr error
  
   for _, fetcher := range c.fetchers {
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
       allResults = append(allResults, <-resultCh)
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
```


---


### 2.3 Fetch Stage


**File:** `fetch_stage.go`


```go
package datapipe


import "context"


type FetchStage[T Entity, Req Request] struct {
   chain *FetcherChain[T, Req]
}


func NewFetchStage[T Entity, Req Request](chain *FetcherChain[T, Req]) *FetchStage[T, Req] {
   return &FetchStage[T, Req]{chain: chain}
}


func (s *FetchStage[T, Req]) Name() string { return "fetch" }
func (s *FetchStage[T, Req]) Required() bool { return true }


func (s *FetchStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
   results, hasMore, source, err := s.chain.Fetch(ctx, state.Request())
   if err != nil {
       return err
   }
  
   state.SetResults(results, hasMore, source)
   return nil
}
```


---


## Phase 3: Enrichment System


### 3.1 Enricher Interface


**File:** `enricher.go`


```go
package datapipe


import "context"


type Enricher[T Entity] interface {
   Name() string
   CanEnrich(authType string, expandParams []string) bool
   Enrich(ctx context.Context, items []T) error
   Required() bool
}


type BaseEnricher[T Entity] struct {
   name          string
   allowedAuths  []string
   expandKey     string
   required      bool
}


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


type EnricherOption[T Entity] func(*BaseEnricher[T])


func WithAllowedAuthTypes[T Entity](auths ...string) EnricherOption[T] {
   return func(e *BaseEnricher[T]) {
       e.allowedAuths = auths
   }
}


func WithExpandKey[T Entity](key string) EnricherOption[T] {
   return func(e *BaseEnricher[T]) {
       e.expandKey = key
   }
}


func WithEnricherRequired[T Entity](required bool) EnricherOption[T] {
   return func(e *BaseEnricher[T]) {
       e.required = required
   }
}


func (e BaseEnricher[T]) Name() string { return e.name }
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
```


---


### 3.2 Composite Enricher


**File:** `composite_enricher.go`


```go
package datapipe


import (
   "context"
   "sync"
)


type CompositeEnricher[T Entity] struct {
   enrichers   []Enricher[T]
   parallel    bool
   maxWorkers  int
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


func WithEnricherAdded[T Entity](e Enricher[T]) CompositeEnricherOption[T] {
   return func(c *CompositeEnricher[T]) {
       c.enrichers = append(c.enrichers, e)
   }
}


func WithParallelExecution[T Entity](parallel bool) CompositeEnricherOption[T] {
   return func(c *CompositeEnricher[T]) {
       c.parallel = parallel
   }
}


func WithMaxWorkers[T Entity](max int) CompositeEnricherOption[T] {
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


func (c *CompositeEnricher[T]) enrichSequential(ctx context.Context, items []T, enrichers []Enricher[T]) []error {
   var errs []error
  
   for _, enricher := range enrichers {
       if err := enricher.Enrich(ctx, items); err != nil {
           if enricher.Required() {
               return []error{err}
           }
           errs = append(errs, err)
       }
   }
  
   return errs
}


func (c *CompositeEnricher[T]) enrichParallel(ctx context.Context, items []T, enrichers []Enricher[T]) []error {
   var (
       wg      sync.WaitGroup
       mu      sync.Mutex
       errs    []error
       sem     = make(chan struct{}, c.maxWorkers)
   )
  
   for _, enricher := range enrichers {
       wg.Add(1)
       go func(e Enricher[T]) {
           defer wg.Done()
          
           sem <- struct{}{}
           defer func() { <-sem }()
          
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
```


---


### 3.3 Enrich Stage


**File:** `enrich_stage.go`


```go
package datapipe


import "context"


type EnrichStage[T Entity, Req Request] struct {
   enricher *CompositeEnricher[T]
}


func NewEnrichStage[T Entity, Req Request](enricher *CompositeEnricher[T]) *EnrichStage[T, Req] {
   return &EnrichStage[T, Req]{enricher: enricher}
}


func (s *EnrichStage[T, Req]) Name() string { return "enrich" }
func (s *EnrichStage[T, Req]) Required() bool { return false }


func (s *EnrichStage[T, Req]) Execute(ctx context.Context, state *State[T, Req]) error {
   results := state.Results()
   if len(results) == 0 {
       return nil
   }
  
   authType := ""
   if ar, ok := any(state.Request()).(AuthenticatedRequest); ok {
       authType = ar.GetAuthType()
   }
  
   var expandParams []string
   if er, ok := state.GetMetadata("expand"); ok {
       if params, ok := er.([]string); ok {
           expandParams = params
       }
   }
  
   errs := s.enricher.Enrich(ctx, results, authType, expandParams)
   for _, err := range errs {
       state.AddError(err)
   }
  
   return nil
}
```


---


## Phase 4: Response Building


### 4.1 Response Builder Interface


**File:** `response_builder.go`


```go
package datapipe


import "context"


type ResponseBuilder[T Entity, Req Request, Resp Response[T]] interface {
   Build(ctx context.Context, state *State[T, Req]) (Resp, error)
}


type DefaultResponseBuilder[T Entity, Req Request] struct{}


func (b *DefaultResponseBuilder[T, Req]) Build(ctx context.Context, state *State[T, Req]) (*CollectionResponse[T], error) {
   return NewCollectionResponse(state.Results(), state.HasMore()), nil
}
```


---


## Phase 5: Options & Middleware


### 5.1 Functional Options


**File:** `options.go`


```go
package datapipe


import "time"


type Option[T Entity, Req Request, Resp Response[T]] func(*Pipeline[T, Req, Resp])


func WithFetcherChain[T Entity, Req Request, Resp Response[T]](chain *FetcherChain[T, Req]) Option[T, Req, Resp] {
   return func(p *Pipeline[T, Req, Resp]) {
       p.AddStage(NewFetchStage[T, Req](chain))
   }
}


func WithFetchers[T Entity, Req Request, Resp Response[T]](fetchers ...Fetcher[T, Req]) Option[T, Req, Resp] {
   return func(p *Pipeline[T, Req, Resp]) {
       opts := make([]FetcherChainOption[T, Req], 0, len(fetchers))
       for _, f := range fetchers {
           opts = append(opts, WithFetcher[T, Req](f))
       }
       chain := NewFetcherChain(opts...)
       p.AddStage(NewFetchStage[T, Req](chain))
   }
}


func WithEnricher[T Entity, Req Request, Resp Response[T]](enricher *CompositeEnricher[T]) Option[T, Req, Resp] {
   return func(p *Pipeline[T, Req, Resp]) {
       p.AddStage(NewEnrichStage[T, Req](enricher))
   }
}


func WithEnrichers[T Entity, Req Request, Resp Response[T]](enrichers ...Enricher[T]) Option[T, Req, Resp] {
   return func(p *Pipeline[T, Req, Resp]) {
       opts := make([]CompositeEnricherOption[T], 0, len(enrichers))
       for _, e := range enrichers {
           opts = append(opts, WithEnricherAdded[T](e))
       }
       composite := NewCompositeEnricher(opts...)
       p.AddStage(NewEnrichStage[T, Req](composite))
   }
}


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
```


---


### 5.2 Middleware


**File:** `middleware.go`


```go
package datapipe


import (
   "context"
   "time"
)


type Middleware[T Entity, Req Request] func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req]


func LoggingMiddleware[T Entity, Req Request](logger Logger) Middleware[T, Req] {
   return func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req] {
       return func(ctx context.Context, state *State[T, Req]) error {
           start := time.Now()
           err := next(ctx, state)
           logger.Info("stage completed",
               "stage", stageName,
               "duration", time.Since(start),
               "results", state.ResultCount(),
               "error", err,
           )
           return err
       }
   }
}


func RecoveryMiddleware[T Entity, Req Request]() Middleware[T, Req] {
   return func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req] {
       return func(ctx context.Context, state *State[T, Req]) (err error) {
           defer func() {
               if r := recover(); r != nil {
                   err = NewPipelineError("", stageName, "panic", nil)
               }
           }()
           return next(ctx, state)
       }
   }
}


type Logger interface {
   Info(msg string, keyvals ...interface{})
   Error(msg string, keyvals ...interface{})
}
```


---


### 5.3 Metrics


**File:** `metrics.go`


```go
package datapipe


import "time"


type Metrics interface {
   RecordStageDuration(pipeline, stage string, duration time.Duration)
   RecordFetchSource(pipeline, source string)
   RecordResultCount(pipeline string, count int)
   RecordError(pipeline, stage, errorType string)
}


type NoopMetrics struct{}


func (NoopMetrics) RecordStageDuration(string, string, time.Duration) {}
func (NoopMetrics) RecordFetchSource(string, string)                  {}
func (NoopMetrics) RecordResultCount(string, int)                     {}
func (NoopMetrics) RecordError(string, string, string)                {}
```


---


## Phase 6: Built-in Fetchers


### 6.1 Elasticsearch Fetcher


**File:** `fetchers/elasticsearch.go`


```go
package fetchers


import (
   "context"
   "encoding/json"
  
   "github.com/razorpay/datapipe"
)


type ESClient interface {
   Search(ctx context.Context, index string, query map[string]interface{}) ([]byte, error)
}


type ESQueryBuilder[T datapipe.Entity, Req datapipe.Request] interface {
   BuildQuery(req Req) (map[string]interface{}, error)
   ParseResponse(data []byte) ([]T, bool, error)
}


type ESFetcher[T datapipe.Entity, Req datapipe.Request] struct {
   datapipe.BaseFetcher[T, Req]
   client       ESClient
   index        string
   queryBuilder ESQueryBuilder[T, Req]
}


func NewESFetcher[T datapipe.Entity, Req datapipe.Request](
   client ESClient,
   index string,
   queryBuilder ESQueryBuilder[T, Req],
   priority int,
) *ESFetcher[T, Req] {
   return &ESFetcher[T, Req]{
       BaseFetcher:  datapipe.NewBaseFetcher[T, Req]("elasticsearch", priority),
       client:       client,
       index:        index,
       queryBuilder: queryBuilder,
   }
}


func (f *ESFetcher[T, Req]) CanHandle(req Req) bool {
   if sr, ok := any(req).(datapipe.SearchableRequest); ok {
       return sr.HasSearchQuery()
   }
   return false
}


func (f *ESFetcher[T, Req]) Fetch(ctx context.Context, req Req) ([]T, bool, error) {
   query, err := f.queryBuilder.BuildQuery(req)
   if err != nil {
       return nil, false, datapipe.NewFetchError("elasticsearch", "failed to build query", err)
   }
  
   data, err := f.client.Search(ctx, f.index, query)
   if err != nil {
       return nil, false, datapipe.NewFetchError("elasticsearch", "search failed", err)
   }
  
   return f.queryBuilder.ParseResponse(data)
}
```


---


## Summary: File List


| Phase | File | Description |
|-------|------|-------------|
| 1 | `entity.go` | Entity interface |
| 1 | `request.go` | Request interfaces |
| 1 | `response.go` | Response interface + default |
| 1 | `state.go` | Pipeline state management |
| 1 | `stage.go` | Stage interface |
| 1 | `errors.go` | Error types |
| 1 | `pipeline.go` | Core pipeline |
| 1 | `config.go` | Configuration |
| 2 | `fetcher.go` | Fetcher interface |
| 2 | `fetcher_chain.go` | Chain of responsibility |
| 2 | `fetch_stage.go` | Fetch stage |
| 3 | `enricher.go` | Enricher interface |
| 3 | `composite_enricher.go` | Composite pattern |
| 3 | `enrich_stage.go` | Enrich stage |
| 4 | `response_builder.go` | Response builder |
| 5 | `options.go` | Functional options |
| 5 | `middleware.go` | Middleware system |
| 5 | `metrics.go` | Metrics interface |
| 6 | `fetchers/elasticsearch.go` | ES fetcher |
| 6 | `fetchers/db.go` | DB fetcher |


---


## Testing Checklist


- [ ] Unit tests for each file (80%+ coverage)
- [ ] Integration tests with testcontainers
- [ ] Benchmark tests for parallel operations
- [ ] Race condition tests (`go test -race`)
- [ ] Example tests in `examples/` directory


---


## Open Source Checklist


- [ ] MIT License
- [ ] README.md with quick start
- [ ] CONTRIBUTING.md
- [ ] CODE_OF_CONDUCT.md
- [ ] GitHub Actions CI
- [ ] GoDoc comments on all public APIs
- [ ] Examples directory
- [ ] Changelog
- [ ] Semantic versioning setup