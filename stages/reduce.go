package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════════╗
// ║Reduce processes values from the input channel incrementally using a reducer   ║
// ║function, combining them with an accumulator. This allows aggregating results  ║
// ║as they come in without keeping all values in memory. The reducer function     ║
// ║takes the current accumulator and the next value, and returns the updated      ║
// ║accumulator. The final accumulated result is sent to the output channel.       ║
// ╚═══════════════════════════════════════════════════════════════════════════════╝

type Reducer[T any, Acc any] func(context.Context, Acc, T) (Acc, error)

type ReduceStage[T any, Acc any] struct {
	Name    string
	Buffer  int
	Initial Acc
	Reducer Reducer[T, Acc]
}

func (s ReduceStage[T, Acc]) Create(ctx pipeline.Context, in <-chan T) <-chan Acc {
	out := make(chan Acc, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		accumulator := s.Initial

		for input := range in {
			var err error
			accumulator, err = s.Reducer(ctx, accumulator, input)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Continue processing
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- accumulator:
		}

		return nil
	})

	return out
}
