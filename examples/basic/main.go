package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nulllvoid/datapipe"
)

type User struct {
	ID        string
	Name      string
	Email     string
	CreatedAt time.Time
}

func (u User) GetID() string { return u.ID }

type UserRequest struct {
	MerchantID string
	Status     string
	Limit      int
	Offset     int
}

func (r *UserRequest) GetLimit() int           { return r.Limit }
func (r *UserRequest) GetOffset() int          { return r.Offset }
func (r *UserRequest) Clone() datapipe.Request { copy := *r; return &copy }

func main() {
	fetchStage := datapipe.NewStage[User, *UserRequest](
		"fetch",
		true,
		func(ctx context.Context, state *datapipe.State[User, *UserRequest]) error {
			req := state.Request()
			fmt.Printf("Fetching users for merchant: %s (limit: %d, offset: %d)\n",
				req.MerchantID, req.Limit, req.Offset)

			users := []User{
				{ID: "user_1", Name: "Alice", Email: "alice@example.com", CreatedAt: time.Now()},
				{ID: "user_2", Name: "Bob", Email: "bob@example.com", CreatedAt: time.Now()},
				{ID: "user_3", Name: "Charlie", Email: "charlie@example.com", CreatedAt: time.Now()},
			}

			hasMore := len(users) >= req.Limit
			state.SetResults(users, hasMore, "mock_db")
			return nil
		},
	)

	transformStage := datapipe.NewStage[User, *UserRequest](
		"transform",
		false,
		func(ctx context.Context, state *datapipe.State[User, *UserRequest]) error {
			fmt.Printf("Transforming %d users\n", state.ResultCount())
			return nil
		},
	)

	pipeline := datapipe.New[User, *UserRequest, *datapipe.CollectionResponse[User]](
		"user_fetch",
		datapipe.WithValidation[User, *UserRequest, *datapipe.CollectionResponse[User]](
			func(req *UserRequest) error {
				if req.Limit <= 0 || req.Limit > 100 {
					return datapipe.NewValidationError("limit", "must be between 1 and 100")
				}
				if req.MerchantID == "" {
					return datapipe.NewValidationError("merchant_id", "is required")
				}
				return nil
			},
		),
		datapipe.WithStage[User, *UserRequest, *datapipe.CollectionResponse[User]](fetchStage),
		datapipe.WithStage[User, *UserRequest, *datapipe.CollectionResponse[User]](transformStage),
		datapipe.WithTimeout[User, *UserRequest, *datapipe.CollectionResponse[User]](5*time.Second),
		datapipe.WithResponseBuilder[User, *UserRequest, *datapipe.CollectionResponse[User]](
			&datapipe.DefaultResponseBuilder[User, *UserRequest]{},
		),
	)

	ctx := context.Background()
	req := &UserRequest{
		MerchantID: "merchant_123",
		Status:     "active",
		Limit:      10,
		Offset:     0,
	}

	resp, err := pipeline.Execute(ctx, req)
	if err != nil {
		log.Fatalf("Pipeline execution failed: %v", err)
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Entity: %s\n", resp.Entity)
	fmt.Printf("  Count: %d\n", resp.GetCount())
	fmt.Printf("  HasMore: %v\n", resp.HasMore())
	fmt.Printf("  Items:\n")
	for _, user := range resp.GetItems() {
		fmt.Printf("    - %s: %s (%s)\n", user.ID, user.Name, user.Email)
	}
}
