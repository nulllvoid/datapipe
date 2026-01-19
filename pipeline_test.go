package datapipe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nulllvoid/datapipe"
)

func TestPipeline_New(t *testing.T) {
	t.Parallel()

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test_pipeline",
	)

	if pipeline.Name() != "test_pipeline" {
		t.Errorf("Name() = %v, want test_pipeline", pipeline.Name())
	}
	if pipeline.StageCount() != 0 {
		t.Errorf("StageCount() = %v, want 0", pipeline.StageCount())
	}
}

func TestPipeline_AddStage(t *testing.T) {
	t.Parallel()

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
	)

	stage := datapipe.NewStage[testEntity, *testRequest](
		"stage1",
		true,
		func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
			return nil
		},
	)

	pipeline.AddStage(stage)

	if pipeline.StageCount() != 1 {
		t.Errorf("StageCount() = %v, want 1", pipeline.StageCount())
	}
}

func TestPipeline_Execute(t *testing.T) {
	t.Parallel()

	t.Run("successful execution with response builder", func(t *testing.T) {
		t.Parallel()

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"fetch",
					true,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						state.SetResults([]testEntity{{ID: "1"}, {ID: "2"}}, true, "test")
						return nil
					},
				),
			),
			datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
			),
		)

		resp, err := pipeline.Execute(context.Background(), &testRequest{Limit: 10})

		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if resp.GetCount() != 2 {
			t.Errorf("Count = %v, want 2", resp.GetCount())
		}
		if !resp.HasMore() {
			t.Error("HasMore should be true")
		}
	})

	t.Run("required stage error stops pipeline", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("required stage failed")
		executed := false

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"failing",
					true,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						return expectedErr
					},
				),
			),
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"after",
					true,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						executed = true
						return nil
					},
				),
			),
		)

		_, err := pipeline.Execute(context.Background(), &testRequest{})

		if err == nil {
			t.Fatal("Execute() should return error")
		}
		if executed {
			t.Error("Stage after failed required stage should not execute")
		}
	})

	t.Run("optional stage error continues pipeline", func(t *testing.T) {
		t.Parallel()

		executed := false

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"optional",
					false,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						return errors.New("optional failed")
					},
				),
			),
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"next",
					true,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						executed = true
						return nil
					},
				),
			),
			datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
			),
		)

		_, err := pipeline.Execute(context.Background(), &testRequest{})

		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if !executed {
			t.Error("Stage after optional stage should execute")
		}
	})

	t.Run("timeout cancels execution", func(t *testing.T) {
		t.Parallel()

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithTimeout[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				10*time.Millisecond,
			),
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"slow",
					true,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						select {
						case <-time.After(100 * time.Millisecond):
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					},
				),
			),
		)

		_, err := pipeline.Execute(context.Background(), &testRequest{})

		if err == nil {
			t.Fatal("Execute() should return timeout error")
		}
	})

	t.Run("fail fast mode", func(t *testing.T) {
		t.Parallel()

		executed := false

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithFailFast[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](true),
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"failing",
					false,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						return errors.New("fail")
					},
				),
			),
			datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				datapipe.NewStage[testEntity, *testRequest](
					"after",
					false,
					func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
						executed = true
						return nil
					},
				),
			),
		)

		_, err := pipeline.Execute(context.Background(), &testRequest{})

		if err == nil {
			t.Fatal("Execute() should return error in fail fast mode")
		}
		if executed {
			t.Error("Stage after failed stage should not execute in fail fast mode")
		}
	})
}

func TestPipeline_WithValidation(t *testing.T) {
	t.Parallel()

	t.Run("validation passes", func(t *testing.T) {
		t.Parallel()

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithValidation[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				func(req *testRequest) error {
					if req.Limit <= 0 {
						return datapipe.NewValidationError("limit", "must be positive")
					}
					return nil
				},
			),
			datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
			),
		)

		resp, err := pipeline.Execute(context.Background(), &testRequest{Limit: 10})

		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if resp == nil {
			t.Error("Response should not be nil")
		}
	})

	t.Run("validation fails", func(t *testing.T) {
		t.Parallel()

		pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			"test",
			datapipe.WithValidation[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
				func(req *testRequest) error {
					if req.Limit <= 0 {
						return datapipe.NewValidationError("limit", "must be positive")
					}
					return nil
				},
			),
		)

		_, err := pipeline.Execute(context.Background(), &testRequest{Limit: 0})

		if err == nil {
			t.Fatal("Execute() should return validation error")
		}
	})
}

func TestPipeline_WithMiddleware(t *testing.T) {
	t.Parallel()

	var executionOrder []string

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithMiddleware[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			func(stageName string, next datapipe.StageFunc[testEntity, *testRequest]) datapipe.StageFunc[testEntity, *testRequest] {
				return func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
					executionOrder = append(executionOrder, "before:"+stageName)
					err := next(ctx, state)
					executionOrder = append(executionOrder, "after:"+stageName)
					return err
				}
			},
		),
		datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			datapipe.NewStage[testEntity, *testRequest](
				"stage1",
				true,
				func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
					executionOrder = append(executionOrder, "execute:stage1")
					return nil
				},
			),
		),
		datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
		),
	)

	_, _ = pipeline.Execute(context.Background(), &testRequest{})

	expected := []string{"before:stage1", "execute:stage1", "after:stage1"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("Execution order length = %v, want %v", len(executionOrder), len(expected))
	}
	for i, v := range expected {
		if executionOrder[i] != v {
			t.Errorf("Execution order[%d] = %v, want %v", i, executionOrder[i], v)
		}
	}
}

type mockMetrics struct {
	stageDurations  map[string]time.Duration
	fetchSources    map[string]string
	resultCounts    map[string]int
	errorRecordings map[string]int
}

func newMockMetrics() *mockMetrics {
	return &mockMetrics{
		stageDurations:  make(map[string]time.Duration),
		fetchSources:    make(map[string]string),
		resultCounts:    make(map[string]int),
		errorRecordings: make(map[string]int),
	}
}

func (m *mockMetrics) RecordStageDuration(pipeline, stage string, duration time.Duration) {
	m.stageDurations[pipeline+":"+stage] = duration
}

func (m *mockMetrics) RecordFetchSource(pipeline, source string) {
	m.fetchSources[pipeline] = source
}

func (m *mockMetrics) RecordResultCount(pipeline string, count int) {
	m.resultCounts[pipeline] = count
}

func (m *mockMetrics) RecordError(pipeline, stage, errorType string) {
	m.errorRecordings[pipeline+":"+stage] = 1
}

func TestPipeline_WithMetrics(t *testing.T) {
	t.Parallel()

	metrics := newMockMetrics()

	pipeline := datapipe.New[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
		"test",
		datapipe.WithMetrics[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](metrics),
		datapipe.WithStage[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			datapipe.NewStage[testEntity, *testRequest](
				"fetch",
				true,
				func(ctx context.Context, state *datapipe.State[testEntity, *testRequest]) error {
					state.SetResults([]testEntity{{ID: "1"}}, false, "db")
					return nil
				},
			),
		),
		datapipe.WithResponseBuilder[testEntity, *testRequest, *datapipe.CollectionResponse[testEntity]](
			&datapipe.DefaultResponseBuilder[testEntity, *testRequest]{},
		),
	)

	_, _ = pipeline.Execute(context.Background(), &testRequest{})

	if _, ok := metrics.stageDurations["test:fetch"]; !ok {
		t.Error("Stage duration should be recorded")
	}
	if metrics.fetchSources["test"] != "db" {
		t.Errorf("Fetch source = %v, want db", metrics.fetchSources["test"])
	}
	if metrics.resultCounts["test"] != 1 {
		t.Errorf("Result count = %v, want 1", metrics.resultCounts["test"])
	}
}

