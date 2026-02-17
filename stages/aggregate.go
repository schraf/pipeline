package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Aggregate consumes all values from the input channel and sends the         ║
// ║collected slice of values as a single item on the output channel.          ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type AggregateStage[T any] struct {
	Name   string
	Buffer int
}

func (s AggregateStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan []T {
	out := make(chan []T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		inputs := []T{}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					// Input channel closed, send aggregated result
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- inputs:
					}
					return nil
				}

				inputs = append(inputs, input)
			}
		}
	})

	return out
}
