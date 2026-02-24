package pipeline

import "errors"

// DrainChannel all values from the upstream channel
func DrainChannel[T any](in <-chan T) {
	for range in {
	}
}

func IsDrainError(err error) bool {
	var drain *DrainError
	return errors.As(err, &drain)
}

func IsSkipError(err error) bool {
	var skip *SkipError
	return errors.As(err, &skip)
}
