package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
)

// ╔════════════════════════════════════════════════════════════════════════════════════╗
// ║WindowedReduce processes values from the input channel incrementally using a        ║
// ║windowed reducer function. Unlike standard Reduce, this stage can emit intermediate ║
// ║results based on the logic defined in the reducer. The reducer returns the next     ║
// ║accumulator state, a value to emit, and a boolean flag indicating if emission       ║
// ║should occur. This allows for implementing windows (e.g., count, time, or value     ║
// ║based) that reset or update state after emitting.                                   ║
// ╚════════════════════════════════════════════════════════════════════════════════════╝

// WindowedReducer is a function that takes a context, the current accumulator, and a new input value.
// It returns:
// - nextAcc: The updated accumulator state to be used for the next iteration.
// - output: The value to be sent to the output channel (only if emit is true).
// - emit: A boolean flag indicating whether 'output' should be sent downstream.
// - err: An error if processing fails.
type WindowedReducer[T any, Acc any] func(context.Context, Acc, T) (nextAcc Acc, output Acc, emit bool, err error)

type WindowedReduceStage[T any, Acc any] struct {
	Name    string
	Buffer  uint
	Initial Acc
	Reducer WindowedReducer[T, Acc]
}

func (s WindowedReduceStage[T, Acc]) Create(ctx pipeline.Context, in <-chan T) <-chan Acc {
	if s.Reducer == nil {
		panic("WindowedReduceStage: Reducer must not be nil")
	}

	out := make(chan Acc, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

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

				nextAcc, output, emit, err := s.Reducer(ctx, accumulator, input)
				if err != nil {
					return err
				}

				if emit {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- output:
					}
				}

				accumulator = nextAcc
			}
		}
	})

	return out
}
