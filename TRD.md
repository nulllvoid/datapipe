# Technical Reference Document: DataPipe

## Generic Multi-Source Data Fetching Pipeline for Go

**Version:** 1.0.0  
**Status:** Draft  
**Author:** Payouts Engineering  
**Last Updated:** January 2026

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Goals & Non-Goals](#goals--non-goals)
4. [Architecture Overview](#architecture-overview)
5. [Core Concepts](#core-concepts)
6. [Design Patterns](#design-patterns)
7. [API Reference](#api-reference)
8. [Usage Examples](#usage-examples)
9. [Implementation Plan](#implementation-plan)
10. [Testing Strategy](#testing-strategy)
11. [Performance Considerations](#performance-considerations)
12. [Migration Guide](#migration-guide)

---

## 1. Executive Summary

**DataPipe** is a generic, extensible Go package for building multi-source data fetching pipelines with built-in support for:

- **Multi-source fetching**: Primary DB, replica DB, TiDB, Elasticsearch, Redis cache
- **Intelligent fallback**: Chain-of-responsibility pattern for source selection
- **Parallel enrichment**: Concurrent data augmentation from multiple services
- **Search abstraction**: Unified interface for full-text and structured queries
- **Observability**: Built-in metrics, tracing, and logging hooks

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **Reusability** | One pipeline serves payouts, reversals, transactions, any entity |
| **Testability** | Interface-driven design enables easy mocking |
| **Extensibility** | Add new sources/enrichers without modifying core |
| **Performance** | Parallel execution, connection pooling, caching support |
| **Observability** | OpenTelemetry-ready instrumentation |

---

## 2. Problem Statement

### Current Challenges

Modern microservices often need to:

1. **Fetch from multiple data sources** with different performance characteristics
2. **Support complex search queries** (fuzzy, filters, date ranges)
3. **Enrich results** with data from related services
4. **Handle fallbacks** when primary sources fail or lack data
5. **Maintain consistency** across read replicas

### Existing Solutions & Limitations

| Approach | Limitation |
|----------|------------|
| Hardcoded if-else chains | Unmaintainable, no separation of concerns |
| Strategy pattern explosion | Too many strategies, cognitive overhead |
| Repository pattern alone | Doesn't address multi-source orchestration |
| GraphQL DataLoader | Go ecosystem lacks mature implementations |

### Target Use Cases

```
┌─────────────────────────────────────────────────────────────────┐
│                     API Request                                  │
│   GET /v1/payouts?status=processed&from=...&expand[]=user       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DataPipe Pipeline                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ Validate │──│  Fetch   │──│  Enrich  │──│ Build Response   │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ │
│                     │              │                             │
│            ┌────────┴────────┐     │                             │
│            ▼        ▼        ▼     ▼                             │
│         [AppDB] [TiDB]   [ES]  [Services]                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Goals & Non-Goals

### Goals

| ID | Goal | Priority |
|----|------|----------|
| G1 | Generic pipeline supporting any entity type | P0 |
| G2 | Multi-source fetching with intelligent fallback | P0 |
| G3 | Elasticsearch integration for search queries | P0 |
| G4 | Parallel enrichment from multiple services | P0 |
| G5 | Zero external dependencies (stdlib + otel only) | P1 |
| G6 | OpenTelemetry instrumentation out-of-box | P1 |
| G7 | Functional options for clean initialization | P1 |
| G8 | Middleware support for cross-cutting concerns | P2 |

### Non-Goals

| ID | Non-Goal | Reason |
|----|----------|--------|
| NG1 | ORM or query builder | Use existing solutions (GORM, sqlx) |
| NG2 | Caching layer | Application-specific, provide hooks only |
| NG3 | Circuit breaker implementation | Use resilience4go or similar |
| NG4 | Database connection management | External concern |

---

## 4. Architecture Overview

### High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              DataPipe Package                               │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                         Pipeline[T, Req, Resp]                       │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐  │  │
│  │  │Validate │→ │  Fetch  │→ │Transform│→ │ Enrich  │→ │  Build    │  │  │
│  │  │ Stage   │  │  Stage  │  │  Stage  │  │  Stage  │  │ Response  │  │  │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └───────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│         ┌──────────────────────────┼──────────────────────────┐           │
│         ▼                          ▼                          ▼           │
│  ┌─────────────┐          ┌──────────────┐          ┌──────────────┐      │
│  │   Fetcher   │          │   Enricher   │          │  Response    │      │
│  │   Chain     │          │  Composite   │          │   Builder    │      │
│  └─────────────┘          └──────────────┘          └──────────────┘      │
│         │                          │                                       │
│    ┌────┼────┬────┐          ┌─────┼─────┐                                │
│    ▼    ▼    ▼    ▼          ▼     ▼     ▼                                │
│  [DB] [TiDB][ES][Cache]   [Svc1][Svc2][Svc3]                              │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│  Utilities: State, Context, Metrics, Errors, Middleware                    │
└────────────────────────────────────────────────────────────────────────────┘
```

### Package Structure

```
datapipe/
├── pipeline.go          # Core Pipeline struct and Execute method
├── stage.go             # Stage interface and base implementations
├── fetcher.go           # Fetcher interface and FetcherChain
├── enricher.go          # Enricher interface and CompositeEnricher
├── response.go          # ResponseBuilder interface
├── state.go             # Pipeline execution state
├── query.go             # Query specification pattern
├── options.go           # Functional options
├── middleware.go        # Middleware support
├── metrics.go           # Metrics interfaces
├── errors.go            # Error types
├── context.go           # Context helpers
│
├── fetchers/            # Built-in fetcher implementations
│   ├── db.go            # Generic SQL database fetcher
│   ├── elasticsearch.go # Elasticsearch fetcher
│   └── cache.go         # Cache-first fetcher wrapper
│
├── enrichers/           # Built-in enricher utilities
│   └── batch.go         # Batch enrichment helper
│
└── examples/            # Usage examples
    ├── basic/
    ├── multi_source/
    └── with_search/
```

---

## 5. Core Concepts

### 5.1 Entity

Any data structure that can be fetched and enriched. Must implement:

```go
type Entity interface {
    GetID() string
}
```

**Design Decision:** Minimal interface allows maximum flexibility. Additional interfaces can be composed as needed.

### 5.2 Request

The input specification for a fetch operation:

```go
type Request interface {
    GetLimit() int          // Pagination limit
    GetOffset() int         // Pagination offset
    GetAuthType() string    // Authorization context
    Clone() Request         // Deep copy for modifications
}
```

**Extensible via embedding:**

```go
type SearchableRequest interface {
    Request
    GetQuery() string       // Search query
    IsFullTextSearch() bool // ES vs DB search
}

type FilterableRequest interface {
    Request
    GetFilters() map[string]interface{}
}
```

### 5.3 Response

The output container:

```go
type Response[T Entity] interface {
    GetItems() []T
    GetCount() int
    HasMore() bool
}
```

### 5.4 State

Carries data through pipeline stages:

```go
type State[T Entity, Req Request] struct {
    request   Req
    results   []T
    hasMore   bool
    metadata  map[string]interface{}
    errors    []error
    source    string  // Which fetcher provided the data
}
```

### 5.5 Stage

A discrete step in the pipeline:

```go
type Stage[T Entity, Req Request] interface {
    Name() string
    Execute(ctx context.Context, state *State[T, Req]) error
    IsRequired() bool
}
```

---

## 6. Design Patterns

### 6.1 Pipeline Pattern (Core)

**Intent:** Process data through a sequence of discrete stages.

**Structure:**
```
Request → [Stage1] → [Stage2] → [Stage3] → Response
              ↓          ↓          ↓
           State flows through all stages
```

**Benefits:**
- Clear separation of concerns
- Easy to add/remove/reorder stages
- Each stage can be tested independently

**Implementation:**

```go
type Pipeline[T Entity, Req Request, Resp Response[T]] struct {
    name           string
    stages         []Stage[T, Req]
    responseBuilder ResponseBuilder[T, Req, Resp]
    config         *Config
    metrics        Metrics
}

func (p *Pipeline[T, Req, Resp]) Execute(ctx context.Context, req Req) (Resp, error) {
    state := NewState(req)
    
    for _, stage := range p.stages {
        if err := stage.Execute(ctx, state); err != nil {
            if stage.IsRequired() {
                return zero, err
            }
            state.AddError(err)
        }
    }
    
    return p.responseBuilder.Build(ctx, state)
}
```

### 6.2 Chain of Responsibility (Fetchers)

**Intent:** Pass request along a chain of handlers until one processes it.

**Use Case:** Try AppDB → TiDB → Elasticsearch based on query type and data availability.

```
┌───────────┐     ┌───────────┐     ┌───────────┐
│  AppDB    │────▶│   TiDB    │────▶│    ES     │
│  Fetcher  │     │  Fetcher  │     │  Fetcher  │
└───────────┘     └───────────┘     └───────────┘
     │                  │                  │
     │ CanHandle? ──────┼──────────────────┤
     │ Fetch() ─────────┼──────────────────┤
     │ NeedsMore? ──────┼──────────────────┘
     ▼                  ▼
   Results          Results
```

**Implementation:**

```go
type Fetcher[T Entity, Req Request] interface {
    Name() string
    Priority() int                                    // Lower = tried first
    CanHandle(req Req) bool                          // Can this fetcher handle?
    Fetch(ctx context.Context, req Req) ([]T, bool, error)  // bool = hasMore
}

type FetcherChain[T Entity, Req Request] struct {
    fetchers       []Fetcher[T, Req]  // Sorted by priority
    fallbackMode   FallbackMode       // Sequential, Parallel, FirstSuccess
    aggregateMode  AggregateMode      // First, Merge, Dedupe
}
```

**Fallback Modes:**

| Mode | Behavior |
|------|----------|
| `Sequential` | Try next fetcher if current returns insufficient results |
| `FirstSuccess` | Use first fetcher that returns any results |
| `Parallel` | Query all fetchers simultaneously, merge results |

### 6.3 Composite Pattern (Enrichers)

**Intent:** Treat individual enrichers and groups uniformly.

**Use Case:** Run multiple enrichments (fund_account, user, transaction) in parallel.

```go
type Enricher[T Entity] interface {
    Name() string
    CanEnrich(authType string) bool    // Auth-based filtering
    Enrich(ctx context.Context, items []T) error
    IsRequired() bool                  // Fail pipeline if enrichment fails?
}

type CompositeEnricher[T Entity] struct {
    enrichers    []Enricher[T]
    parallel     bool                  // Execute in parallel?
    maxWorkers   int                   // Concurrency limit
}
```

**Parallel Execution:**

```go
func (c *CompositeEnricher[T]) Enrich(ctx context.Context, items []T) error {
    if !c.parallel {
        return c.enrichSequential(ctx, items)
    }
    
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(c.maxWorkers)
    
    for _, enricher := range c.enrichers {
        e := enricher
        g.Go(func() error {
            return e.Enrich(ctx, items)
        })
    }
    
    return g.Wait()
}
```

### 6.4 Specification Pattern (Query Building)

**Intent:** Encapsulate query logic into reusable, composable specifications.

**Use Case:** Build complex queries for DB and ES from the same request.

```go
type Specification interface {
    IsSatisfiedBy(entity interface{}) bool
    ToSQL() (string, []interface{})
    ToElasticsearch() map[string]interface{}
}

type AndSpecification struct {
    specs []Specification
}

type OrSpecification struct {
    specs []Specification
}

// Usage
spec := And(
    MerchantID("merchant_123"),
    Status("processed"),
    DateRange("created_at", from, to),
)

sqlQuery, args := spec.ToSQL()
esQuery := spec.ToElasticsearch()
```

### 6.5 Functional Options (Configuration)

**Intent:** Clean, extensible initialization without constructor explosion.

```go
// Creating a pipeline
pipeline := datapipe.New[Payout, PayoutRequest, PayoutResponse](
    "payout_fetch",
    datapipe.WithFetchers(appDBFetcher, tidbFetcher, esFetcher),
    datapipe.WithEnrichers(fundAccountEnricher, userEnricher),
    datapipe.WithFallbackMode(datapipe.Sequential),
    datapipe.WithParallelEnrichment(true, 5),
    datapipe.WithMetrics(prometheusMetrics),
    datapipe.WithTimeout(5 * time.Second),
)
```

---

## 7. API Reference

### 7.1 Pipeline

```go
// New creates a new pipeline with the given name and options
func New[T Entity, Req Request, Resp Response[T]](
    name string,
    opts ...Option[T, Req, Resp],
) *Pipeline[T, Req, Resp]

// Execute runs the pipeline with the given request
func (p *Pipeline[T, Req, Resp]) Execute(
    ctx context.Context,
    req Req,
) (Resp, error)

// AddStage appends a stage to the pipeline
func (p *Pipeline[T, Req, Resp]) AddStage(stage Stage[T, Req])

// Use adds middleware to all stages
func (p *Pipeline[T, Req, Resp]) Use(middleware ...Middleware[T, Req])
```

### 7.2 Options

```go
// Fetcher options
func WithFetchers[T, Req, Resp](fetchers ...Fetcher[T, Req]) Option[T, Req, Resp]
func WithFallbackMode[T, Req, Resp](mode FallbackMode) Option[T, Req, Resp]

// Enricher options
func WithEnrichers[T, Req, Resp](enrichers ...Enricher[T]) Option[T, Req, Resp]
func WithParallelEnrichment[T, Req, Resp](parallel bool, workers int) Option[T, Req, Resp]

// Response options
func WithResponseBuilder[T, Req, Resp](builder ResponseBuilder[T, Req, Resp]) Option[T, Req, Resp]

// Observability options
func WithMetrics[T, Req, Resp](m Metrics) Option[T, Req, Resp]
func WithTracer[T, Req, Resp](tracer trace.Tracer) Option[T, Req, Resp]

// Behavior options
func WithTimeout[T, Req, Resp](d time.Duration) Option[T, Req, Resp]
func WithFailFast[T, Req, Resp](enabled bool) Option[T, Req, Resp]
func WithValidation[T, Req, Resp](fn ValidatorFunc[Req]) Option[T, Req, Resp]
```

### 7.3 Built-in Fetchers

```go
// Database fetcher
type DBFetcher[T Entity, Req Request] struct {
    db        *sql.DB
    tableName string
    builder   QueryBuilder[T, Req]
}

// Elasticsearch fetcher
type ESFetcher[T Entity, Req Request] struct {
    client    *elasticsearch.Client
    index     string
    builder   ESQueryBuilder[T, Req]
}

// Cache wrapper
type CachedFetcher[T Entity, Req Request] struct {
    wrapped   Fetcher[T, Req]
    cache     Cache
    ttl       time.Duration
    keyFunc   func(Req) string
}
```

### 7.4 Errors

```go
var (
    ErrValidation     = errors.New("validation error")
    ErrFetchFailed    = errors.New("fetch failed")
    ErrEnrichFailed   = errors.New("enrichment failed")
    ErrTimeout        = errors.New("operation timed out")
    ErrNoFetcher      = errors.New("no fetcher can handle request")
)

type PipelineError struct {
    Stage   string
    Op      string
    Err     error
}
```

---

## 8. Usage Examples

### 8.1 Basic Usage

```go
package main

import (
    "context"
    "github.com/your-org/datapipe"
)

// Define your entity
type User struct {
    ID        string
    Name      string
    Email     string
    CreatedAt time.Time
}

func (u User) GetID() string { return u.ID }

// Define request
type UserFetchRequest struct {
    MerchantID string
    Status     string
    Limit      int
    Offset     int
}

func (r UserFetchRequest) GetLimit() int    { return r.Limit }
func (r UserFetchRequest) GetOffset() int   { return r.Offset }
func (r UserFetchRequest) GetAuthType() string { return "private" }
func (r UserFetchRequest) Clone() datapipe.Request { 
    copy := r
    return &copy 
}

// Define response
type UserResponse struct {
    Items   []User
    Count   int
    HasMore bool
}

func (r UserResponse) GetItems() []User { return r.Items }
func (r UserResponse) GetCount() int    { return r.Count }
func (r UserResponse) HasMore() bool    { return r.HasMore }

// Create and use pipeline
func main() {
    // Create fetcher
    dbFetcher := NewUserDBFetcher(db)
    
    // Create pipeline
    pipeline := datapipe.New[User, *UserFetchRequest, *UserResponse](
        "user_fetch",
        datapipe.WithFetchers[User, *UserFetchRequest, *UserResponse](dbFetcher),
    )
    
    // Execute
    resp, err := pipeline.Execute(context.Background(), &UserFetchRequest{
        MerchantID: "merchant_123",
        Limit:      10,
    })
    
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Found %d users\n", resp.GetCount())
}
```

### 8.2 Multi-Source with Search

```go
// Create fetchers for different sources
appDBFetcher := NewPayoutDBFetcher(appDB)
tiDBFetcher := NewPayoutDBFetcher(tiDB)
esFetcher := NewPayoutESFetcher(esClient)

// Create pipeline with fallback
pipeline := datapipe.New[Payout, *PayoutRequest, *PayoutResponse](
    "payout_fetch",
    datapipe.WithFetchers[Payout, *PayoutRequest, *PayoutResponse](
        appDBFetcher,   // Priority 1
        tiDBFetcher,    // Priority 2  
        esFetcher,      // Priority 3 (for search)
    ),
    datapipe.WithFallbackMode[Payout, *PayoutRequest, *PayoutResponse](
        datapipe.Sequential,
    ),
)

// For regular queries - uses AppDB, falls back to TiDB
resp, _ := pipeline.Execute(ctx, &PayoutRequest{
    MerchantID: "merchant_123",
    Status:     "processed",
})

// For search queries - routes to ES
resp, _ := pipeline.Execute(ctx, &PayoutRequest{
    MerchantID: "merchant_123",
    Query:      "invoice payment",  // Full-text search
})
```

### 8.3 With Enrichment

```go
// Define enrichers
fundAccountEnricher := &FundAccountEnricher{
    service: fundAccountService,
}

userEnricher := &UserEnricher{
    service: userService,
}

transactionEnricher := &TransactionEnricher{
    service: transactionService,
}

// Create pipeline with parallel enrichment
pipeline := datapipe.New[Payout, *PayoutRequest, *PayoutResponse](
    "payout_fetch",
    datapipe.WithFetchers[Payout, *PayoutRequest, *PayoutResponse](dbFetcher),
    datapipe.WithEnrichers[Payout, *PayoutRequest, *PayoutResponse](
        fundAccountEnricher,
        userEnricher,
        transactionEnricher,
    ),
    datapipe.WithParallelEnrichment[Payout, *PayoutRequest, *PayoutResponse](true, 3),
)
```

### 8.4 Custom Middleware

```go
// Logging middleware
loggingMiddleware := func(next datapipe.StageFunc[Payout, *PayoutRequest]) datapipe.StageFunc[Payout, *PayoutRequest] {
    return func(ctx context.Context, state *datapipe.State[Payout, *PayoutRequest]) error {
        start := time.Now()
        err := next(ctx, state)
        log.Printf("Stage completed in %v", time.Since(start))
        return err
    }
}

// Metrics middleware
metricsMiddleware := func(next datapipe.StageFunc[Payout, *PayoutRequest]) datapipe.StageFunc[Payout, *PayoutRequest] {
    return func(ctx context.Context, state *datapipe.State[Payout, *PayoutRequest]) error {
        timer := prometheus.NewTimer(stageLatency)
        defer timer.ObserveDuration()
        return next(ctx, state)
    }
}

pipeline.Use(loggingMiddleware, metricsMiddleware)
```

---

## 9. Implementation Plan

### Phase 1: Core Foundation (Week 1-2)

| Task | Description | Priority |
|------|-------------|----------|
| 1.1 | Define core interfaces (Entity, Request, Response) | P0 |
| 1.2 | Implement Pipeline struct and Execute method | P0 |
| 1.3 | Implement State management | P0 |
| 1.4 | Implement Stage interface and base stages | P0 |
| 1.5 | Create error types | P0 |
| 1.6 | Implement functional options | P1 |
| 1.7 | Write unit tests for core | P0 |

**Deliverables:**
- `pipeline.go`, `stage.go`, `state.go`, `errors.go`, `options.go`
- 80%+ test coverage

### Phase 2: Fetcher System (Week 3-4)

| Task | Description | Priority |
|------|-------------|----------|
| 2.1 | Define Fetcher interface | P0 |
| 2.2 | Implement FetcherChain with fallback modes | P0 |
| 2.3 | Implement FetchStage | P0 |
| 2.4 | Create generic DB fetcher | P1 |
| 2.5 | Create Elasticsearch fetcher | P0 |
| 2.6 | Implement cache wrapper | P2 |
| 2.7 | Write integration tests | P0 |

**Deliverables:**
- `fetcher.go`, `fetchers/db.go`, `fetchers/elasticsearch.go`
- Integration tests with test containers

### Phase 3: Enrichment System (Week 5)

| Task | Description | Priority |
|------|-------------|----------|
| 3.1 | Define Enricher interface | P0 |
| 3.2 | Implement CompositeEnricher with parallel execution | P0 |
| 3.3 | Implement EnrichStage | P0 |
| 3.4 | Create batch enrichment utilities | P1 |
| 3.5 | Write unit and integration tests | P0 |

**Deliverables:**
- `enricher.go`, `enrichers/batch.go`
- Concurrency tests

### Phase 4: Query & Response (Week 6)

| Task | Description | Priority |
|------|-------------|----------|
| 4.1 | Implement Specification pattern for queries | P1 |
| 4.2 | Define ResponseBuilder interface | P0 |
| 4.3 | Implement default response builders | P1 |
| 4.4 | Add auth-type based response filtering | P1 |
| 4.5 | Write tests | P0 |

**Deliverables:**
- `query.go`, `response.go`

### Phase 5: Observability (Week 7)

| Task | Description | Priority |
|------|-------------|----------|
| 5.1 | Define Metrics interface | P1 |
| 5.2 | Implement OpenTelemetry integration | P1 |
| 5.3 | Add tracing to all stages | P1 |
| 5.4 | Implement context helpers | P1 |
| 5.5 | Create Prometheus adapter | P2 |

**Deliverables:**
- `metrics.go`, `context.go`, integration with OTel

### Phase 6: Middleware & Polish (Week 8)

| Task | Description | Priority |
|------|-------------|----------|
| 6.1 | Implement middleware system | P2 |
| 6.2 | Create common middleware (logging, metrics, recovery) | P2 |
| 6.3 | Write comprehensive examples | P1 |
| 6.4 | Write documentation | P0 |
| 6.5 | Performance benchmarks | P1 |
| 6.6 | Code review and refactoring | P0 |

**Deliverables:**
- `middleware.go`, `examples/`, README, CONTRIBUTING.md
- Benchmark results

### Gantt Chart

```
Week     1     2     3     4     5     6     7     8
Phase 1  ████████
Phase 2            ████████
Phase 3                      ████
Phase 4                           ████
Phase 5                                ████
Phase 6                                     ████
```

---

## 10. Testing Strategy

### Unit Tests

```go
func TestPipeline_Execute(t *testing.T) {
    tests := []struct {
        name     string
        stages   []Stage
        request  Request
        wantErr  bool
        validate func(t *testing.T, resp Response)
    }{
        {
            name: "successful execution",
            stages: []Stage{
                NewMockFetchStage(mockResults),
                NewMockEnrichStage(),
            },
            request: &MockRequest{Limit: 10},
            wantErr: false,
            validate: func(t *testing.T, resp Response) {
                assert.Equal(t, 5, resp.GetCount())
            },
        },
        // ... more cases
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            // ... test implementation
        })
    }
}
```

### Integration Tests

- Use testcontainers for MySQL and Elasticsearch
- Test complete pipeline flows
- Verify fallback behavior between sources

### Benchmark Tests

```go
func BenchmarkPipeline_Execute(b *testing.B) {
    pipeline := setupBenchmarkPipeline()
    req := &MockRequest{Limit: 100}
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = pipeline.Execute(context.Background(), req)
    }
}

func BenchmarkFetcherChain_Parallel(b *testing.B) {
    // Benchmark parallel vs sequential fetching
}

func BenchmarkEnricher_Parallel(b *testing.B) {
    // Benchmark parallel vs sequential enrichment
}
```

---

## 11. Performance Considerations

### Memory Optimization

| Technique | Description |
|-----------|-------------|
| Object pooling | Reuse State objects via sync.Pool |
| Slice pre-allocation | Pre-allocate slices based on request limit |
| Zero-copy where possible | Use pointers instead of value copies |

### Concurrency

| Component | Strategy |
|-----------|----------|
| FetcherChain (parallel mode) | errgroup with context cancellation |
| CompositeEnricher | errgroup with worker limit |
| Pipeline | No internal concurrency (caller controls) |

### Caching

- Cache wrapper for fetchers
- Result caching at pipeline level (optional)
- Connection pooling delegated to external clients

### Benchmarks Target

| Operation | Target Latency (p99) |
|-----------|---------------------|
| Simple fetch (DB) | < 10ms |
| Multi-source fetch | < 50ms |
| Fetch + 3 enrichers | < 100ms |
| Search (ES) | < 200ms |

---

## 12. Migration Guide

### From Current fetch_orchestrator to DataPipe

#### Step 1: Map existing components

| Current | DataPipe |
|---------|----------|
| `Orchestrator` | `Pipeline` |
| `ExecutionStrategy` | `FetcherChain` + `Fetcher` |
| `StorageStrategy` | `Fetcher.CanHandle()` |
| `Enricher` | `CompositeEnricher` + `Enricher` |
| `ResponseBuilder` | `ResponseBuilder` |
| `FetchMultiplePayoutsRequest` | Implement `Request` interface |

#### Step 2: Create new fetchers

```go
// Old
type AppDBFetcher struct { ... }
func (f *AppDBFetcher) Fetch(ctx, req, options, connection) ([]Payout, bool, error)

// New
type PayoutDBFetcher struct {
    db *sql.DB
    datapipe.BaseFetcher[*Payout, *PayoutRequest]
}

func (f *PayoutDBFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]*Payout, bool, error) {
    // Implementation
}
```

#### Step 3: Migrate enrichers

```go
// Old
func (e *Enricher) Enrich(ctx, items, authType, expandParams)

// New
type FundAccountEnricher struct {
    datapipe.BaseEnricher[*Payout]
}

func (e *FundAccountEnricher) Enrich(ctx context.Context, items []*Payout) error {
    // Get expand params from context or state
}
```

#### Step 4: Create pipeline factory

```go
func NewPayoutPipeline(deps Dependencies) *datapipe.Pipeline[*Payout, *PayoutRequest, *PayoutResponse] {
    return datapipe.New[*Payout, *PayoutRequest, *PayoutResponse](
        "payout_fetch",
        datapipe.WithFetchers(
            NewPayoutDBFetcher(deps.AppDB),
            NewPayoutTiDBFetcher(deps.TiDB),
            NewPayoutESFetcher(deps.ESClient),
        ),
        datapipe.WithEnrichers(
            NewFundAccountEnricher(deps.FAService),
            NewUserEnricher(deps.UserService),
        ),
        datapipe.WithParallelEnrichment(true, 5),
    )
}
```

#### Step 5: Update service layer

```go
// Old
func (s *Service) FetchMultiple(ctx, req) (*Response, error) {
    return s.orchestrator.FetchAll(ctx, req, ...)
}

// New
func (s *Service) FetchMultiple(ctx context.Context, req *PayoutRequest) (*PayoutResponse, error) {
    return s.pipeline.Execute(ctx, req)
}
```

---

## Appendix A: File Structure After Implementation

```
datapipe/
├── pipeline.go          # ~150 lines
├── stage.go             # ~100 lines
├── fetcher.go           # ~200 lines
├── enricher.go          # ~150 lines
├── response.go          # ~50 lines
├── state.go             # ~100 lines
├── query.go             # ~200 lines
├── options.go           # ~100 lines
├── middleware.go        # ~80 lines
├── metrics.go           # ~60 lines
├── errors.go            # ~50 lines
├── context.go           # ~40 lines
├── doc.go               # Package documentation
│
├── fetchers/
│   ├── db.go            # ~100 lines
│   ├── elasticsearch.go # ~150 lines
│   └── cache.go         # ~80 lines
│
├── enrichers/
│   └── batch.go         # ~100 lines
│
├── internal/
│   └── pool.go          # Object pooling
│
└── examples/
    ├── basic/
    │   └── main.go
    ├── multi_source/
    │   └── main.go
    └── with_search/
        └── main.go

Total: ~1,600 lines of core code (vs current ~3,000+ lines)
```

---

## Appendix B: Comparison with Existing Solution

| Metric | Current | DataPipe |
|--------|---------|----------|
| Files | 25+ | 12 core + 3 fetchers |
| Lines of Code | 3,000+ | ~1,600 |
| Interfaces | 15+ | 6 |
| Strategies | 8 | 0 (replaced by chain) |
| Test Coverage | ~60% | Target 85%+ |
| Cognitive Complexity | High | Low |

---

## Appendix C: Open Source Considerations

### License

MIT License - permissive, allows commercial use

### Documentation

- Comprehensive README with quick start
- GoDoc comments on all public APIs
- Examples for common use cases
- CONTRIBUTING.md with guidelines

### CI/CD

- GitHub Actions for testing
- Codecov for coverage
- golangci-lint for code quality
- GoReleaser for releases

### Versioning

Semantic Versioning (semver)
- v0.x.x - Development, API may change
- v1.x.x - Stable API

---

**Document Version History**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | Jan 2026 | Payouts Engineering | Initial draft |