package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Transform reads values from the input channel, applies the transformer     ║
// ║function, and forwards successful results to the output channel until the  ║
// ║context is done or the input channel is closed.                            ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type Transformer[In any, Out any] func(context.Context, In) (Out, error)

type TransformStage[In any, Out any] struct {
	Name        string
	Buffer      uint
	Transformer Transformer[In, Out]
}

func (s TransformStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	if s.Transformer == nil {
		panic("TransformStage: Transformer must not be nil")
	}

	out := make(chan Out, s.Buffer)
	pipeline.RegisterChannel(ctx.Telemetry(), s.Name, out)

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

				output, err := s.Transformer(ctx, input)
				if err != nil {
					if pipeline.IsDrainError(err) {
						return nil
					}

					if pipeline.IsSkipError(err) {
						continue
					}

					return pipeline.ErrorInStage(s.Name, err)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- output:
				}
			}
		}
	})

	return out
}
