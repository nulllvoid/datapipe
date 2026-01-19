package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nulllvoid/datapipe"
)

type Payout struct {
	ID        string
	Amount    int64
	Status    string
	CreatedAt time.Time
}

func (p Payout) GetID() string { return p.ID }

type PayoutRequest struct {
	MerchantID string
	Status     string
	Query      string
	Limit      int
	Offset     int
}

func (r *PayoutRequest) GetLimit() int           { return r.Limit }
func (r *PayoutRequest) GetOffset() int          { return r.Offset }
func (r *PayoutRequest) Clone() datapipe.Request { copy := *r; return &copy }
func (r *PayoutRequest) GetSearchQuery() string  { return r.Query }
func (r *PayoutRequest) HasSearchQuery() bool    { return r.Query != "" }

type AppDBFetcher struct {
	datapipe.BaseFetcher[Payout, *PayoutRequest]
	failRate  int
	callCount int
}

func NewAppDBFetcher(priority int) *AppDBFetcher {
	return &AppDBFetcher{
		BaseFetcher: datapipe.NewBaseFetcher[Payout, *PayoutRequest]("app_db", priority),
		failRate:    0,
	}
}

func (f *AppDBFetcher) CanHandle(req *PayoutRequest) bool {
	return !req.HasSearchQuery()
}

func (f *AppDBFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]Payout, bool, error) {
	f.callCount++
	fmt.Printf("[AppDB] Fetching payouts for merchant %s (attempt %d)\n", req.MerchantID, f.callCount)

	if f.failRate > 0 && f.callCount <= f.failRate {
		fmt.Printf("[AppDB] Simulating failure\n")
		return nil, false, errors.New("app_db connection timeout")
	}

	payouts := []Payout{
		{ID: "pout_1", Amount: 10000, Status: "processed", CreatedAt: time.Now()},
		{ID: "pout_2", Amount: 25000, Status: "processed", CreatedAt: time.Now()},
	}

	hasMore := len(payouts) >= req.Limit
	return payouts, hasMore, nil
}

type TiDBFetcher struct {
	datapipe.BaseFetcher[Payout, *PayoutRequest]
}

func NewTiDBFetcher(priority int) *TiDBFetcher {
	return &TiDBFetcher{
		BaseFetcher: datapipe.NewBaseFetcher[Payout, *PayoutRequest]("tidb", priority),
	}
}

func (f *TiDBFetcher) CanHandle(req *PayoutRequest) bool {
	return !req.HasSearchQuery()
}

func (f *TiDBFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]Payout, bool, error) {
	fmt.Printf("[TiDB] Fetching payouts for merchant %s (fallback)\n", req.MerchantID)

	payouts := []Payout{
		{ID: "pout_3", Amount: 15000, Status: "processed", CreatedAt: time.Now().Add(-time.Hour)},
		{ID: "pout_4", Amount: 30000, Status: "pending", CreatedAt: time.Now().Add(-2 * time.Hour)},
		{ID: "pout_5", Amount: 50000, Status: "processed", CreatedAt: time.Now().Add(-3 * time.Hour)},
	}

	return payouts, false, nil
}

type ESFetcher struct {
	datapipe.BaseFetcher[Payout, *PayoutRequest]
}

func NewESFetcher(priority int) *ESFetcher {
	return &ESFetcher{
		BaseFetcher: datapipe.NewBaseFetcher[Payout, *PayoutRequest]("elasticsearch", priority),
	}
}

func (f *ESFetcher) CanHandle(req *PayoutRequest) bool {
	return req.HasSearchQuery()
}

func (f *ESFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]Payout, bool, error) {
	fmt.Printf("[Elasticsearch] Searching payouts with query: %q\n", req.Query)

	payouts := []Payout{
		{ID: "pout_search_1", Amount: 12000, Status: "processed", CreatedAt: time.Now()},
	}

	return payouts, false, nil
}

func main() {
	fmt.Println("=== DataPipe Multi-Source Example ===")
	fmt.Println()

	appDBFetcher := NewAppDBFetcher(1)
	tiDBFetcher := NewTiDBFetcher(2)
	esFetcher := NewESFetcher(3)

	chain := datapipe.NewFetcherChain[Payout, *PayoutRequest](
		datapipe.ChainWithFetcher[Payout, *PayoutRequest](appDBFetcher),
		datapipe.ChainWithFetcher[Payout, *PayoutRequest](tiDBFetcher),
		datapipe.ChainWithFetcher[Payout, *PayoutRequest](esFetcher),
		datapipe.ChainWithFallbackMode[Payout, *PayoutRequest](datapipe.FallbackSequential),
	)

	pipeline := datapipe.New[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
		"payout_fetch",
		datapipe.WithValidation[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
			func(req *PayoutRequest) error {
				if req.MerchantID == "" {
					return datapipe.NewValidationError("merchant_id", "is required")
				}
				return nil
			},
		),
		datapipe.WithFetcherChain[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](chain),
		datapipe.WithTimeout[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](5*time.Second),
		datapipe.WithResponseBuilder[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
			&datapipe.DefaultResponseBuilder[Payout, *PayoutRequest]{},
		),
	)

	fmt.Println("--- Scenario 1: Normal fetch (AppDB succeeds) ---")
	resp, err := pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID: "merchant_123",
		Status:     "processed",
		Limit:      10,
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 2: Search query (routes to Elasticsearch) ---")
	resp, err = pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID: "merchant_123",
		Query:      "invoice payment",
		Limit:      10,
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 3: AppDB fails, fallback to TiDB ---")
	appDBFetcher.failRate = 1
	appDBFetcher.callCount = 0

	resp, err = pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID: "merchant_456",
		Status:     "pending",
		Limit:      10,
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 4: Parallel fetching mode ---")
	parallelChain := datapipe.NewFetcherChain[Payout, *PayoutRequest](
		datapipe.ChainWithFetcher[Payout, *PayoutRequest](NewAppDBFetcher(1)),
		datapipe.ChainWithFetcher[Payout, *PayoutRequest](NewTiDBFetcher(2)),
		datapipe.ChainWithFallbackMode[Payout, *PayoutRequest](datapipe.FallbackParallel),
	)

	parallelPipeline := datapipe.New[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
		"payout_parallel_fetch",
		datapipe.WithFetcherChain[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](parallelChain),
		datapipe.WithResponseBuilder[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
			&datapipe.DefaultResponseBuilder[Payout, *PayoutRequest]{},
		),
	)

	resp, err = parallelPipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID: "merchant_789",
		Limit:      10,
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)
}

func printResponse(resp *datapipe.CollectionResponse[Payout]) {
	fmt.Printf("\nResults:\n")
	fmt.Printf("  Count: %d\n", resp.GetCount())
	fmt.Printf("  HasMore: %v\n", resp.HasMore())
	fmt.Printf("  Items:\n")
	for _, p := range resp.GetItems() {
		fmt.Printf("    - %s: %d cents (%s)\n", p.ID, p.Amount, p.Status)
	}
}
