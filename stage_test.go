package datapipe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestFunctionalStage_Name(t *testing.T) {
	t.Parallel()

	stage := datapipe.NewStage[testEntity, *testRequest](
		"test_stage",
		true,
		func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
			return nil
		},
	)

	if stage.Name() != "test_stage" {
		t.Errorf("Name() = %v, want test_stage", stage.Name())
	}
}

func TestFunctionalStage_Required(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		required bool
	}{
		{"required stage", true},
		{"optional stage", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stage := datapipe.NewStage[testEntity, *testRequest](
				"stage",
				tt.required,
				func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
					return nil
				},
			)

			if stage.Required() != tt.required {
				t.Errorf("Required() = %v, want %v", stage.Required(), tt.required)
			}
		})
	}
}

func TestFunctionalStage_Execute(t *testing.T) {
	t.Parallel()

	t.Run("successful execution", func(t *testing.T) {
		t.Parallel()

		executed := false
		stage := datapipe.NewStage[testEntity, *testRequest](
			"test",
			true,
			func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
				executed = true
				state.SetResults([]testEntity{{ID: "1"}}, false, "test")
				return nil
			},
		)

		state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
		err := stage.Execute(context.Background(), state)

		if err != nil {
			t.Errorf("Execute() error = %v, want nil", err)
		}
		if !executed {
			t.Error("Stage function was not executed")
		}
		if state.ResultCount() != 1 {
			t.Error("Stage should have modified state")
		}
	})

	t.Run("execution with error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stage error")
		stage := datapipe.NewStage[testEntity, *testRequest](
			"error_stage",
			true,
			func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
				return expectedErr
			},
		)

		state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
		err := stage.Execute(context.Background(), state)

		if err != expectedErr {
			t.Errorf("Execute() error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()

		stage := datapipe.NewStage[testEntity, *testRequest](
			"ctx_stage",
			true,
			func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
				return ctx.Err()
			},
		)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
		err := stage.Execute(ctx, state)

		if err != context.Canceled {
			t.Errorf("Execute() error = %v, want context.Canceled", err)
		}
	})
}

