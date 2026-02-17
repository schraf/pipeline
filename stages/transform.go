package stages

import (
	"context"
	"errors"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Transform reads values from the input channel, applies the transformer     ║
// ║function, and forwards successful results to the output channel until the  ║
// ║context is done or the input channel is closed.                            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type Transformer[In any, Out any] func(context.Context, In) (*Out, error)

type TransformStage[In any, Out any] struct {
	Name        string
	Buffer      int
	Transformer Transformer[In, Out]
}

func (s TransformStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	out := make(chan Out, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		for input := range in {
			output, err := s.Transformer(ctx, input)
			if err != nil {
				return err
			}
			if output == nil {
				return errors.New("transformer returned nil output without error")
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- *output:
			}
		}

		return nil
	})

	return out
}
