package datapipe

import "time"

type Config struct {
	Timeout    time.Duration
	FailFast   bool
	MaxRetries int
	RetryDelay time.Duration
}

func DefaultConfig() Config {
	return Config{
		Timeout:    30 * time.Second,
		FailFast:   false,
		MaxRetries: 0,
		RetryDelay: 100 * time.Millisecond,
	}
}
