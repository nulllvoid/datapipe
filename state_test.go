package datapipe_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/nulllvoid/datapipe"
)

func TestState_NewState(t *testing.T) {
	t.Parallel()

	req := &testRequest{Limit: 10}
	state := datapipe.NewState[testEntity, *testRequest](req)

	if state.Request() != req {
		t.Error("NewState did not set request correctly")
	}
	if len(state.Results()) != 0 {
		t.Error("NewState should initialize empty results")
	}
	if state.HasMore() {
		t.Error("NewState should initialize hasMore as false")
	}
	if state.Source() != "" {
		t.Error("NewState should initialize source as empty")
	}
}

func TestState_SetResults(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	items := []testEntity{{ID: "1"}, {ID: "2"}}

	state.SetResults(items, true, "test_source")

	if len(state.Results()) != 2 {
		t.Errorf("Results length = %v, want 2", len(state.Results()))
	}
	if !state.HasMore() {
		t.Error("HasMore should be true")
	}
	if state.Source() != "test_source" {
		t.Errorf("Source = %v, want test_source", state.Source())
	}
}

func TestState_AppendResults(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	state.SetResults([]testEntity{{ID: "1"}}, false, "source1")
	state.AppendResults([]testEntity{{ID: "2"}, {ID: "3"}})

	if len(state.Results()) != 3 {
		t.Errorf("Results length = %v, want 3", len(state.Results()))
	}
}

func TestState_Metadata(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	state.SetMetadata("key1", "value1")
	state.SetMetadata("key2", 123)

	val1, ok1 := state.GetMetadata("key1")
	if !ok1 || val1 != "value1" {
		t.Error("GetMetadata failed for key1")
	}

	val2, ok2 := state.GetMetadata("key2")
	if !ok2 || val2 != 123 {
		t.Error("GetMetadata failed for key2")
	}

	_, ok3 := state.GetMetadata("nonexistent")
	if ok3 {
		t.Error("GetMetadata should return false for nonexistent key")
	}
}

func TestState_Errors(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	if state.HasErrors() {
		t.Error("New state should not have errors")
	}

	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	state.AddError(err1)
	state.AddError(err2)

	if !state.HasErrors() {
		t.Error("State should have errors after AddError")
	}
	if len(state.Errors()) != 2 {
		t.Errorf("Errors length = %v, want 2", len(state.Errors()))
	}
}

func TestState_ResultCount(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})

	if state.ResultCount() != 0 {
		t.Error("New state should have 0 results")
	}

	state.SetResults([]testEntity{{ID: "1"}, {ID: "2"}}, false, "")

	if state.ResultCount() != 2 {
		t.Errorf("ResultCount = %v, want 2", state.ResultCount())
	}
}

func TestState_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	state := datapipe.NewState[testEntity, *testRequest](&testRequest{})
	var wg sync.WaitGroup
	iterations := 100

	for i := 0; i < iterations; i++ {
		wg.Add(3)

		go func(id int) {
			defer wg.Done()
			state.SetMetadata("key", id)
		}(i)

		go func() {
			defer wg.Done()
			state.AddError(errors.New("concurrent error"))
		}()

		go func() {
			defer wg.Done()
			_ = state.Results()
			_ = state.HasMore()
			_ = state.Source()
		}()
	}

	wg.Wait()

	if len(state.Errors()) != iterations {
		t.Errorf("Expected %d errors, got %d", iterations, len(state.Errors()))
	}
}

func TestState_SetRequest(t *testing.T) {
	t.Parallel()

	req1 := &testRequest{Limit: 10}
	req2 := &testRequest{Limit: 20}
	state := datapipe.NewState[testEntity, *testRequest](req1)

	if state.Request().Limit != 10 {
		t.Error("Initial request not set correctly")
	}

	state.SetRequest(req2)

	if state.Request().Limit != 20 {
		t.Error("SetRequest did not update request")
	}
}

