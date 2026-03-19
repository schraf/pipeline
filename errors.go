package pipeline

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidChannel = errors.New("channel link size mismatch")
)

// ╭────────────────────────────────────────────────────────────────────╮
// │ Skip Error                                                         │
// ╰────────────────────────────────────────────────────────────────────╯

// SkipError signals that the current item should be skipped without killing
// the pipeline. The stage continues processing subsequent items normally.
// The wrapped error is silently discarded and does not appear in the
// pipeline's returned error.
type SkipError struct {
	Err error
}

func (e *SkipError) Error() string { return e.Err.Error() }
func (e *SkipError) Unwrap() error { return e.Err }

// Skip wraps an error as a SkipError. When returned from a Transformer or
// Expander, the current item is dropped and processing continues with the
// next item.
func Skip(err error) error {
	return &SkipError{Err: err}
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Drain Error                                                        │
// ╰────────────────────────────────────────────────────────────────────╯

// DrainError signals that the current item should be skipped AND the stage
// should stop processing new items. The stage drains its input channel to
// unblock upstream goroutines, then shuts down gracefully. The wrapped error
// is silently discarded and does not appear in the pipeline's returned error.
type DrainError struct {
	Err error
}

func (e *DrainError) Error() string { return e.Err.Error() }
func (e *DrainError) Unwrap() error { return e.Err }

// Drain wraps an error as a DrainError. When returned from a Transformer or
// Expander, the current item is dropped and the stage drains all remaining
// input without processing it, allowing upstream stages to complete and
// downstream stages to flush what they already have.
func Drain(err error) error {
	return &DrainError{Err: err}
}

// ╭────────────────────────────────────────────────────────────────────╮
// │ Stage Error                                                        │
// ╰────────────────────────────────────────────────────────────────────╯

// StageError signals that the error originated from a pipeline stage. The
// error will include the name of the stage that the error occurred in.
type StageError struct {
	StageName string
	Err       error
}

func (e *StageError) Error() string { return fmt.Sprintf("stage %s: %v", e.StageName, e.Err) }
func (e *StageError) Unwrap() error { return e.Err }

// ErrorInStage wraps an error as a StageError
func ErrorInStage(name string, err error) error {
	return &StageError{StageName: name, Err: err}
}
