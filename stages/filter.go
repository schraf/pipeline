package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Filter reads values from the input channel, applies the filter predicate,  ║
// ║and forwards only values that satisfy the predicate to the output channel. ║
// ║It respects context cancellation and stops processing on error.            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type Filter[T any] func(context.Context, T) (bool, error)

type FilterStage[T any] struct {
	Name   string
	Buffer uint
	Filter Filter[T]
}

func (s FilterStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan T {
	if s.Filter == nil {
		panic("FilterStage: Filter must not be nil")
	}

	out := make(chan T, s.Buffer)
	pipeline.RegisterChannel(ctx.Telemetry(), s.Name, out)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)
		defer pipeline.DrainChannel(in)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					return nil
				}

				shouldForward, err := s.Filter(ctx, input)
				if err != nil {
					if pipeline.IsDrainError(err) {
						return nil
					}

					return pipeline.ErrorInStage(s.Name, err)
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
		}
	})

	return out
}
