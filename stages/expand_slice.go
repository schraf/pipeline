package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║ExpandSlice reads slices from the input channel and forwards all items     ║
// ║from each slice to the output channel individually.                        ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type ExpandSliceStage[T any] struct {
	Name   string
	Buffer int
}

func (s ExpandSliceStage[T]) Create(ctx pipeline.Context, in <-chan []T) <-chan T {
	out := make(chan T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		for input := range in {
			for _, output := range input {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- output:
				}
			}
		}

		return nil
	})

	return out
}
