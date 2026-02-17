package stages

import (
	"context"
	"iter"
	"runtime/trace"

	"github.com/schraf/pipeline/v3"
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
	name     string
	expander Expander[In, Out]
}

func (s ExpandStage[In, Out]) Create(ctx pipeline.Context, in <-chan In, buffer int) chan<- Out {
	ctx.group.Add(1)
	out := make(chan out, buffer)

	go func() {
		defer close(out)
		defer ctx.group.Done()
		defer trace.StartRegion(ctx.parent, s.name).End()

		for input := range in {
			seq := s.expander(ctx.parent, input)

			for output, err := range seq {
				if err != nil {
					ctx.err(err)
					return
				}

				select {
				case <-ctx.parent.Done():
					return
				case out <- output:
				}
			}
		}
	}()
}
