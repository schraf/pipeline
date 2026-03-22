package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Limit reads values from the input channel, forwards at most n values to    ║
// ║the output channel, and then returns. It respects context cancellation     ║
// ║while reading and forwarding values.                                       ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type LimitStage[T any] struct {
	Name   string
	Buffer uint
	Limit  uint
}

func (s LimitStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan T {
	out := make(chan T, s.Buffer)
	pipeline.RegisterChannel(ctx.Telemetry(), s.Name, out)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)
		defer pipeline.DrainChannel(in)

		if s.Limit == 0 {
			return nil
		}

		count := 0

		for {
			if count >= int(s.Limit) {
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					return nil
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- input:
				}

				count++
			}
		}

		return nil
	})

	return out
}
