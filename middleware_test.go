package datapipe_test

import (
	"context"
	"testing"

	"github.com/nulllvoid/datapipe"
)

type mockLogger struct {
	infoCalls  [][]interface{}
	errorCalls [][]interface{}
}

func (l *mockLogger) Info(msg string, keyvals ...interface{}) {
	l.infoCalls = append(l.infoCalls, append([]interface{}{msg}, keyvals...))
}

func (l *mockLogger) Error(msg string, keyvals ...interface{}) {
	l.errorCalls = append(l.errorCalls, append([]interface{}{msg}, keyvals...))
}

func TestLoggingMiddleware(t *testing.T) {
	t.Parallel()

	logger := &mockLogger{}
	middleware := datapipe.LoggingMiddleware[testEntity, *testRequest](logger)

	stageFn := func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
		state.SetResults([]testEntity{{ID: "1"}}, false, "test")
		return nil
	}

	wrapped := middleware("test_stage", stageFn)
	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	err := wrapped(context.Background(), state)

	if err != nil {
		t.Fatalf("Wrapped function error = %v", err)
	}
	if len(logger.infoCalls) != 1 {
		t.Errorf("Info calls = %d, want 1", len(logger.infoCalls))
	}
	if logger.infoCalls[0][0] != "stage completed" {
		t.Errorf("Info message = %v, want 'stage completed'", logger.infoCalls[0][0])
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	t.Parallel()

	middleware := datapipe.RecoveryMiddleware[testEntity, *testRequest]()

	t.Run("recovers from panic", func(t *testing.T) {
		t.Parallel()

		stageFn := func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
			panic("test panic")
		}

		wrapped := middleware("panic_stage", stageFn)
		state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

		err := wrapped(context.Background(), state)

		if err == nil {
			t.Error("Should return error after panic")
		}
	})

	t.Run("passes through normal execution", func(t *testing.T) {
		t.Parallel()

		executed := false
		stageFn := func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
			executed = true
			return nil
		}

		wrapped := middleware("normal_stage", stageFn)
		state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

		err := wrapped(context.Background(), state)

		if err != nil {
			t.Errorf("Error = %v, want nil", err)
		}
		if !executed {
			t.Error("Stage should have executed")
		}
	})
}

