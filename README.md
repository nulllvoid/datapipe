# DataPipe

[![Go Reference](https://pkg.go.dev/badge/github.com/nulllvoid/datapipe.svg)](https://pkg.go.dev/github.com/nulllvoid/datapipe)
[![Go Report Card](https://goreportcard.com/badge/github.com/nulllvoid/datapipe)](https://goreportcard.com/report/github.com/nulllvoid/datapipe)
[![CI](https://github.com/nulllvoid/datapipe/actions/workflows/ci.yml/badge.svg)](https://github.com/nulllvoid/datapipe/actions/workflows/ci.yml)

A generic, extensible Go package for building multi-source data fetching pipelines with built-in support for fallback, enrichment, and observability.

## Features

- **Multi-Source Fetching**: Chain multiple data sources with intelligent fallback
- **Fallback Modes**: Sequential, FirstSuccess, or Parallel fetching strategies
- **Pipeline Pattern**: Process data through discrete, composable stages
- **Generic Types**: Full Go 1.21+ generics support for type-safe pipelines
- **Middleware Support**: Add cross-cutting concerns like logging and metrics
- **Thread-Safe State**: Safe for parallel enrichment operations
- **Observability Ready**: Built-in hooks for metrics and tracing

## Installation

```bash
go get github.com/nulllvoid/datapipe
```

Requires Go 1.21 or later.

## Quick Start

### 1. Define Your Entity and Request

```go
type User struct {
    ID    string
    Name  string
}

func (u User) GetID() string { return u.ID }

type UserRequest struct {
    Limit  int
    Offset int
}

func (r *UserRequest) GetLimit() int           { return r.Limit }
func (r *UserRequest) GetOffset() int          { return r.Offset }
func (r *UserRequest) Clone() datapipe.Request { copy := *r; return &copy }
```

### 2. Create a Fetcher

```go
type DBFetcher struct {
    datapipe.BaseFetcher[User, *UserRequest]
    db *sql.DB
}

func NewDBFetcher(db *sql.DB, priority int) *DBFetcher {
    return &DBFetcher{
        BaseFetcher: datapipe.NewBaseFetcher[User, *UserRequest]("database", priority),
        db:          db,
    }
}

func (f *DBFetcher) Fetch(ctx context.Context, req *UserRequest) ([]User, bool, error) {
    // Fetch from database
    users := []User{{ID: "1", Name: "Alice"}}
    return users, false, nil
}
```

### 3. Create a Pipeline

```go
// Create fetchers
primaryDB := NewDBFetcher(primaryConn, 1)   // Priority 1 (tried first)
replicaDB := NewDBFetcher(replicaConn, 2)   // Priority 2 (fallback)

// Create fetcher chain with sequential fallback
chain := datapipe.NewFetcherChain[User, *UserRequest](
    datapipe.ChainWithFetcher(primaryDB),
    datapipe.ChainWithFetcher(replicaDB),
    datapipe.ChainWithFallbackMode(datapipe.FallbackSequential),
)

// Create pipeline
pipeline := datapipe.New[User, *UserRequest, *datapipe.CollectionResponse[User]](
    "user_fetch",
    datapipe.WithFetcherChain(chain),
    datapipe.WithTimeout(5*time.Second),
    datapipe.WithResponseBuilder(&datapipe.DefaultResponseBuilder[User, *UserRequest]{}),
)

// Execute
resp, err := pipeline.Execute(ctx, &UserRequest{Limit: 10})
```

## Fallback Modes

| Mode | Behavior |
|------|----------|
| `FallbackSequential` | Try fetchers in priority order, continue if insufficient results |
| `FallbackFirstSuccess` | Use first fetcher that returns any results |
| `FallbackParallel` | Query all fetchers concurrently, use best by priority |

```go
// Sequential: AppDB -> TiDB -> Elasticsearch
chain := datapipe.NewFetcherChain[T, Req](
    datapipe.ChainWithFetcher(appDBFetcher),
    datapipe.ChainWithFetcher(tiDBFetcher),
    datapipe.ChainWithFetcher(esFetcher),
    datapipe.ChainWithFallbackMode(datapipe.FallbackSequential),
)

// Parallel: Query all, use highest priority with results
chain := datapipe.NewFetcherChain[T, Req](
    datapipe.ChainWithFetcher(primaryDB),
    datapipe.ChainWithFetcher(replicaDB),
    datapipe.ChainWithFallbackMode(datapipe.FallbackParallel),
)
```

## Conditional Fetching with CanHandle

Route requests to appropriate fetchers:

```go
type ESFetcher struct {
    datapipe.BaseFetcher[Payout, *PayoutRequest]
}

func (f *ESFetcher) CanHandle(req *PayoutRequest) bool {
    // Only handle search queries
    return req.HasSearchQuery()
}

func (f *ESFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]Payout, bool, error) {
    // Full-text search in Elasticsearch
}
```

## Enrichment

Add parallel enrichment to augment fetched data from multiple services:

```go
// Define enrichers
fundAccountEnricher := &FundAccountEnricher{
    BaseEnricher: datapipe.NewBaseEnricher[*Payout]("fund_account",
        datapipe.EnricherWithExpandKey("fund_account"),
        datapipe.EnricherWithAllowedAuths("private", "admin"),
    ),
}

userEnricher := &UserEnricher{
    BaseEnricher: datapipe.NewBaseEnricher[*Payout]("user",
        datapipe.EnricherWithExpandKey("user"),
    ),
}

// Create pipeline with enrichment
pipeline := datapipe.New[*Payout, *PayoutRequest, *datapipe.CollectionResponse[*Payout]](
    "payout_fetch",
    datapipe.WithFetchers(dbFetcher),
    datapipe.WithParallelEnrichment(true, 5, fundAccountEnricher, userEnricher),
    datapipe.WithResponseBuilder(...),
)
```

### Enricher Options

| Option | Description |
|--------|-------------|
| `EnricherWithExpandKey(key)` | Only enrich when key is in expand params |
| `EnricherWithAllowedAuths(auths...)` | Only enrich for specific auth types |
| `EnricherWithRequired(bool)` | If true, enrichment failure stops pipeline |

## Pipeline Options

```go
pipeline := datapipe.New[User, *UserRequest, *datapipe.CollectionResponse[User]](
    "my_pipeline",
    
    // Fetcher options
    datapipe.WithFetcherChain(chain),
    datapipe.WithFetchers(fetcher1, fetcher2),
    datapipe.WithFetchersAndMode(datapipe.FallbackParallel, fetcher1, fetcher2),
    
    // Enricher options
    datapipe.WithEnrichers(enricher1, enricher2),
    datapipe.WithParallelEnrichment(true, 5, enricher1, enricher2),
    datapipe.WithEnricherComposite(compositeEnricher),
    
    // Validation
    datapipe.WithValidation(validatorFunc),
    
    // Custom stages
    datapipe.WithStage(transformStage),
    
    // Configuration
    datapipe.WithTimeout(5*time.Second),
    datapipe.WithFailFast(true),
    
    // Observability
    datapipe.WithMetrics(metricsImpl),
    datapipe.WithMiddleware(loggingMiddleware),
    
    // Response
    datapipe.WithResponseBuilder(responseBuilder),
)
```

## Examples

See the [examples](examples/) directory:

- [multi_source](examples/multi_source/) - Multi-source fetching with fallback
- [with_enrichment](examples/with_enrichment/) - Parallel enrichment with auth/expand filtering

## Current Implementation

### Phase 1: Core Foundation
- Entity, Request, Response interfaces
- Pipeline with stage execution
- State management
- Middleware support
- Error types

### Phase 2: Fetcher System
- Fetcher interface
- FetcherChain with fallback modes (Sequential, FirstSuccess, Parallel)
- FetchStage
- Pipeline integration

### Phase 3: Enrichment System
- Enricher interface with auth/expand filtering
- CompositeEnricher with parallel/sequential execution
- EnrichStage
- Worker-limited concurrent enrichment

### Upcoming Phases
- **Phase 4**: ResponseBuilder variations, query specification
- **Phase 5**: Advanced middleware
- **Phase 6**: Built-in fetchers (ES, DB, Cache)

## License

MIT License - see [LICENSE](LICENSE) for details.

