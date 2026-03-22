package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
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
	Buffer  uint
	Initial Acc
	Reducer Reducer[T, Acc]
}

func (s ReduceStage[T, Acc]) Create(ctx pipeline.Context, in <-chan T) <-chan Acc {
	if s.Reducer == nil {
		panic("ReduceStage: Reducer must not be nil")
	}

	out := make(chan Acc, s.Buffer)
	pipeline.RegisterChannel(ctx.Telemetry(), s.Name, out)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)
		defer pipeline.DrainChannel(in)

		accumulator := s.Initial

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- accumulator:
					}

					return nil
				}

				var err error
				accumulator, err = s.Reducer(ctx, accumulator, input)
				if err != nil {
					if pipeline.IsDrainError(err) {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case out <- accumulator:
						}

						return nil
					}

					return pipeline.ErrorInStage(s.Name, err)
				}
			}
		}
	})

	return out
}
