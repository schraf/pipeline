package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Filter reads values from the input channel, applies the filter predicate,  ║
// ║and forwards only values that satisfy the predicate to the output channel. ║
// ║It respects context cancellation and stops processing on error.            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type Filter[T any] func(context.Context, T) (bool, error)

type FilterStage[T any] struct {
	Name   string
	Buffer int
	Filter Filter[T]
}

func (s FilterStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan T {
	out := make(chan T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		for input := range in {
			shouldForward, err := s.Filter(ctx, input)
			if err != nil {
				return err
			}

			if !shouldForward {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- input:
			}
		}

		return nil
	})

	return out
}
