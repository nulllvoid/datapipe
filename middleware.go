package datapipe

import (
	"context"
	"time"
)

type StageFunc[T Entity, Req Request] func(ctx context.Context, state *State[T, Req]) error

type Middleware[T Entity, Req Request] func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req]

type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

func LoggingMiddleware[T Entity, Req Request](logger Logger) Middleware[T, Req] {
	return func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req] {
		return func(ctx context.Context, state *State[T, Req]) error {
			start := time.Now()
			err := next(ctx, state)
			logger.Info("stage completed",
				"stage", stageName,
				"duration", time.Since(start),
				"results", state.ResultCount(),
				"error", err,
			)
			return err
		}
	}
}

func RecoveryMiddleware[T Entity, Req Request]() Middleware[T, Req] {
	return func(stageName string, next StageFunc[T, Req]) StageFunc[T, Req] {
		return func(ctx context.Context, state *State[T, Req]) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = NewPipelineError("", stageName, "panic", nil)
				}
			}()
			return next(ctx, state)
		}
	}
}

