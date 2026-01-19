package datapipe_test

import (
	"testing"
	"time"

	"github.com/nulllvoid/datapipe"
)

func TestNoopMetrics(t *testing.T) {
	t.Parallel()

	metrics := datapipe.NoopMetrics{}

	metrics.RecordStageDuration("pipeline", "stage", time.Second)
	metrics.RecordFetchSource("pipeline", "source")
	metrics.RecordResultCount("pipeline", 10)
	metrics.RecordError("pipeline", "stage", "error")
}
