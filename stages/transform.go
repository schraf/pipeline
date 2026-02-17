package stages

import (
	"context"
	"errors"
	"runtime/trace"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Transform reads values from the input channel, applies the transformer     ║
// ║function, and forwards successful results to the output channel until the  ║
// ║context is done or the input channel is closed.                            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type Transformer[In any, Out any] func(context.Context, In) (*Out, error)

type TransformStage[In any, Out any] struct {
	name        string
	transformer Transformer[In, Out]
}

func (s TransformStage[In, Out]) Create(ctx pipeline.Context, in <-chan In, buffer int) chan<- Out {
	ctx.group.Add(1)
	out := make(chan out, buffer)

	go func() {
		defer close(out)
		defer ctx.group.Done()
		defer trace.StartRegion(ctx.parent, s.name).End()

		for input := range in {
			output, err := s.transformer(ctx.parent, input)
			if err != nil {
				ctx.err(err)
				return
			}
			if output == nil {
				ctx.err(errors.New("transformer returned nil output without error"))
				return
			}

			select {
			case <-ctx.parent.Done():
				return
			case out <- *output:
			}
		}
	}()

	return out
}
