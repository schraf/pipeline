package stages

import (
	"context"
	"fmt"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════════╗
// ║Split routes each value read from the input channel to exactly one of the      ║
// ║output channels, as determined by the selector function. The selector must     ║
// ║return a valid index into the output channels. Returns an error if the         ║
// ║selector returns an invalid index.                                             ║
// ╚═══════════════════════════════════════════════════════════════════════════════╝

type Selector[T any] func(context.Context, T) int

type SplitStage[T any] struct {
	Name        string
	OutputCount uint
	Buffer      uint
	Selector    Selector[T]
}

func (s SplitStage[T]) Create(ctx pipeline.Context, in <-chan T) pipeline.MultiChannelReceiver[T] {
	if s.Selector == nil {
		panic("SplitStage: Selector must not be nil")
	}

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

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					return nil
				}

				index := s.Selector(ctx, input)
				if index < 0 || index >= len(outputs) {
					err := fmt.Errorf("SplitStage: selector returned invalid index %d (output count: %d)", index, len(outputs))
					return pipeline.ErrorInStage(s.Name, err)
				}
				outputChannel := outputs[index]

				select {
				case <-ctx.Done():
					return ctx.Err()
				case outputChannel <- input:
				}
			}
		}
	})

	return pipeline.NewMultiChannelReceiver[T](outputs...)
}
