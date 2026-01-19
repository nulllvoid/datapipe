package datapipe

import (
	"errors"
	"fmt"
)

var (
	ErrNoFetcherAvailable = errors.New("no fetcher available for request")
	ErrValidationFailed   = errors.New("validation failed")
	ErrFetchFailed        = errors.New("fetch operation failed")
	ErrEnrichmentFailed   = errors.New("enrichment failed")
	ErrTimeout            = errors.New("operation timed out")
	ErrCanceled           = errors.New("operation canceled")
)

type PipelineError struct {
	Pipeline string
	Stage    string
	Op       string
	Err      error
}

func (e *PipelineError) Error() string {
	if e.Stage != "" {
		return fmt.Sprintf("pipeline %s: stage %s: %s: %v", e.Pipeline, e.Stage, e.Op, e.Err)
	}
	return fmt.Sprintf("pipeline %s: %s: %v", e.Pipeline, e.Op, e.Err)
}

func (e *PipelineError) Unwrap() error {
	return e.Err
}

func NewPipelineError(pipeline, stage, op string, err error) *PipelineError {
	return &PipelineError{Pipeline: pipeline, Stage: stage, Op: op, Err: err}
}

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: field '%s': %s", e.Field, e.Message)
}

func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}

type FetchError struct {
	Source  string
	Message string
	Err     error
}

func (e *FetchError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("fetch error from %s: %s: %v", e.Source, e.Message, e.Err)
	}
	return fmt.Sprintf("fetch error from %s: %s", e.Source, e.Message)
}

func (e *FetchError) Unwrap() error {
	return e.Err
}

func NewFetchError(source, message string, err error) *FetchError {
	return &FetchError{Source: source, Message: message, Err: err}
}

type EnrichmentError struct {
	Enricher string
	Message  string
	Err      error
}

func (e *EnrichmentError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("enrichment error from %s: %s: %v", e.Enricher, e.Message, e.Err)
	}
	return fmt.Sprintf("enrichment error from %s: %s", e.Enricher, e.Message)
}

func (e *EnrichmentError) Unwrap() error {
	return e.Err
}

func NewEnrichmentError(enricher, message string, err error) *EnrichmentError {
	return &EnrichmentError{Enricher: enricher, Message: message, Err: err}
}
