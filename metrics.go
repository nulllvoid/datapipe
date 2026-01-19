package datapipe

import "time"

type Metrics interface {
	RecordStageDuration(pipeline, stage string, duration time.Duration)
	RecordFetchSource(pipeline, source string)
	RecordResultCount(pipeline string, count int)
	RecordError(pipeline, stage, errorType string)
}

type NoopMetrics struct{}

func (NoopMetrics) RecordStageDuration(string, string, time.Duration) {}
func (NoopMetrics) RecordFetchSource(string, string)                  {}
func (NoopMetrics) RecordResultCount(string, int)                     {}
func (NoopMetrics) RecordError(string, string, string)                {}

