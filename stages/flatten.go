package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Flatten takes an input channel of slices and emits each element of each    ║
// ║slice as an individual item on the output channel. It continues until the  ║
// ║input channel is closed or the context is cancelled.                       ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type FlattenStage[T any] struct {
	Name   string
	Buffer uint
}

func (s FlattenStage[T]) Create(ctx pipeline.Context, in <-chan []T) <-chan T {
	out := make(chan T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case slice, ok := <-in:
				if !ok {
					return nil
				}

				for _, item := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- item:
					}
				}
			}
		}
	})

	return out
}
