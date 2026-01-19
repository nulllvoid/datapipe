package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nulllvoid/datapipe"
)

type Payout struct {
	ID            string
	Amount        int64
	Status        string
	FundAccountID string
	UserID        string
	FundAccount   *FundAccount
	User          *User
}

func (p Payout) GetID() string { return p.ID }

type FundAccount struct {
	ID          string
	AccountType string
	BankName    string
}

type User struct {
	ID    string
	Name  string
	Email string
}

type PayoutRequest struct {
	MerchantID   string
	Limit        int
	Offset       int
	AuthType     string
	ExpandParams []string
}

func (r *PayoutRequest) GetLimit() int              { return r.Limit }
func (r *PayoutRequest) GetOffset() int             { return r.Offset }
func (r *PayoutRequest) Clone() datapipe.Request    { copy := *r; return &copy }
func (r *PayoutRequest) GetAuthType() string        { return r.AuthType }
func (r *PayoutRequest) GetExpandParams() []string  { return r.ExpandParams }

type PayoutFetcher struct {
	datapipe.BaseFetcher[Payout, *PayoutRequest]
}

func NewPayoutFetcher() *PayoutFetcher {
	return &PayoutFetcher{
		BaseFetcher: datapipe.NewBaseFetcher[Payout, *PayoutRequest]("database", 1),
	}
}

func (f *PayoutFetcher) Fetch(ctx context.Context, req *PayoutRequest) ([]Payout, bool, error) {
	fmt.Printf("[Database] Fetching payouts for merchant %s\n", req.MerchantID)

	payouts := []Payout{
		{ID: "pout_1", Amount: 10000, Status: "processed", FundAccountID: "fa_1", UserID: "user_1"},
		{ID: "pout_2", Amount: 25000, Status: "processed", FundAccountID: "fa_2", UserID: "user_2"},
		{ID: "pout_3", Amount: 50000, Status: "pending", FundAccountID: "fa_1", UserID: "user_1"},
	}

	return payouts, false, nil
}

type FundAccountEnricher struct {
	datapipe.BaseEnricher[Payout]
}

func NewFundAccountEnricher() *FundAccountEnricher {
	return &FundAccountEnricher{
		BaseEnricher: datapipe.NewBaseEnricher[Payout]("fund_account",
			datapipe.EnricherWithExpandKey[Payout]("fund_account"),
			datapipe.EnricherWithAllowedAuths[Payout]("private", "admin"),
		),
	}
}

func (e *FundAccountEnricher) Enrich(ctx context.Context, items []Payout) error {
	fmt.Printf("[FundAccountEnricher] Enriching %d payouts\n", len(items))

	faStore := map[string]*FundAccount{
		"fa_1": {ID: "fa_1", AccountType: "bank_account", BankName: "HDFC Bank"},
		"fa_2": {ID: "fa_2", AccountType: "vpa", BankName: "UPI"},
	}

	for i := range items {
		if fa, ok := faStore[items[i].FundAccountID]; ok {
			items[i].FundAccount = fa
		}
	}

	return nil
}

type UserEnricher struct {
	datapipe.BaseEnricher[Payout]
}

func NewUserEnricher() *UserEnricher {
	return &UserEnricher{
		BaseEnricher: datapipe.NewBaseEnricher[Payout]("user",
			datapipe.EnricherWithExpandKey[Payout]("user"),
		),
	}
}

func (e *UserEnricher) Enrich(ctx context.Context, items []Payout) error {
	fmt.Printf("[UserEnricher] Enriching %d payouts\n", len(items))

	userStore := map[string]*User{
		"user_1": {ID: "user_1", Name: "Alice Johnson", Email: "alice@example.com"},
		"user_2": {ID: "user_2", Name: "Bob Smith", Email: "bob@example.com"},
	}

	for i := range items {
		if user, ok := userStore[items[i].UserID]; ok {
			items[i].User = user
		}
	}

	return nil
}

type TransactionEnricher struct {
	datapipe.BaseEnricher[Payout]
}

func NewTransactionEnricher() *TransactionEnricher {
	return &TransactionEnricher{
		BaseEnricher: datapipe.NewBaseEnricher[Payout]("transaction",
			datapipe.EnricherWithExpandKey[Payout]("transaction"),
			datapipe.EnricherWithAllowedAuths[Payout]("admin"),
		),
	}
}

func (e *TransactionEnricher) Enrich(ctx context.Context, items []Payout) error {
	fmt.Printf("[TransactionEnricher] Enriching %d payouts (admin only)\n", len(items))
	return nil
}

func main() {
	fmt.Println("=== DataPipe Enrichment Example ===")
	fmt.Println()

	fetcher := NewPayoutFetcher()
	fundAccountEnricher := NewFundAccountEnricher()
	userEnricher := NewUserEnricher()
	transactionEnricher := NewTransactionEnricher()

	pipeline := datapipe.New[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
		"payout_fetch",
		datapipe.WithFetchers[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](fetcher),
		datapipe.WithParallelEnrichment[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
			true, 5,
			fundAccountEnricher,
			userEnricher,
			transactionEnricher,
		),
		datapipe.WithTimeout[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](5*time.Second),
		datapipe.WithResponseBuilder[Payout, *PayoutRequest, *datapipe.CollectionResponse[Payout]](
			&datapipe.DefaultResponseBuilder[Payout, *PayoutRequest]{},
		),
	)

	fmt.Println("--- Scenario 1: Private auth, expand fund_account only ---")
	resp, err := pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID:   "merchant_123",
		Limit:        10,
		AuthType:     "private",
		ExpandParams: []string{"fund_account"},
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 2: Private auth, expand fund_account and user ---")
	resp, err = pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID:   "merchant_123",
		Limit:        10,
		AuthType:     "private",
		ExpandParams: []string{"fund_account", "user"},
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 3: Admin auth, expand all ---")
	resp, err = pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID:   "merchant_123",
		Limit:        10,
		AuthType:     "admin",
		ExpandParams: []string{"fund_account", "user", "transaction"},
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)

	fmt.Println("\n--- Scenario 4: Public auth, try to expand fund_account (should be denied) ---")
	resp, err = pipeline.Execute(context.Background(), &PayoutRequest{
		MerchantID:   "merchant_123",
		Limit:        10,
		AuthType:     "public",
		ExpandParams: []string{"fund_account", "user"},
	})
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
	printResponse(resp)
}

func printResponse(resp *datapipe.CollectionResponse[Payout]) {
	fmt.Printf("\nResults (count: %d):\n", resp.GetCount())
	for _, p := range resp.GetItems() {
		fmt.Printf("  - %s: %d cents (%s)\n", p.ID, p.Amount, p.Status)
		if p.FundAccount != nil {
			fmt.Printf("      FundAccount: %s (%s - %s)\n",
				p.FundAccount.ID, p.FundAccount.AccountType, p.FundAccount.BankName)
		}
		if p.User != nil {
			fmt.Printf("      User: %s (%s)\n", p.User.Name, p.User.Email)
		}
	}
}

