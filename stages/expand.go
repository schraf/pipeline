package stages

import (
	"context"
	"iter"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════════╗
// ║Expand reads values from the input channel, applies the expander function to   ║
// ║each value, and forwards all items from the returned iterator to the output    ║
// ║channel. For each input item, the expander returns an iterator of output items,║
// ║which are all sent to the output channel. Processing continues until the       ║
// ║context is done or the input channel is closed. This allows for lazy evaluation║
// ║and avoids loading all expanded items into memory at once.                     ║
// ╚═══════════════════════════════════════════════════════════════════════════════╝

type Expander[In any, Out any] func(context.Context, In) iter.Seq2[Out, error]

type ExpandStage[In any, Out any] struct {
	Name     string
	Buffer   uint
	Expander Expander[In, Out]
}

func (s ExpandStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	if s.Expander == nil {
		panic("ExpandStage: Expander must not be nil")
	}

	out := make(chan Out, s.Buffer)

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

				seq := s.Expander(ctx, input)

				shouldDrain := false

				for output, err := range seq {
					if err != nil {
						if pipeline.IsDrainError(err) {
							shouldDrain = true
							break
						}

						if pipeline.IsSkipError(err) {
							break
						}

						return pipeline.ErrorInStage(s.Name, err)
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- output:
					}
				}

				if shouldDrain {
					return nil
				}
			}
		}
	})

	return out
}
