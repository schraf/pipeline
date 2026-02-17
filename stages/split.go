package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════════╗
// ║Split routes each value read from the input channel to exactly one of the      ║
// ║output channels, as determined by the selector function. The selector must     ║
// ║return a valid index into the output channels. Panics if the selector returns  ║
// ║an invalid index.                                                              ║
// ╚═══════════════════════════════════════════════════════════════════════════════╝

type Selector[T any] func(context.Context, T) int

type SplitStage[T any] struct {
	Name        string
	OutputCount int
	Buffer      int
	Selector    Selector[T]
}

func (s SplitStage[T]) Create(ctx pipeline.Context, in <-chan T) pipeline.MultiChannelReceiver[T] {
	outputs := make([]chan T, s.OutputCount)
	for i := range outputs {
		outputs[i] = make(chan T, s.Buffer)
	}

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer func() {
			for _, outputChannel := range outputs {
				close(outputChannel)
			}
		}()

		for input := range in {
			index := s.Selector(ctx, input)
			if index < 0 || index >= len(outputs) {
				panic("SplitStage: selector returned invalid index")
			}
			outputChannel := outputs[index]

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputChannel <- input:
			}
		}

		return nil
	})

	return pipeline.MultiChannelReceiver[T](outputs)
}
