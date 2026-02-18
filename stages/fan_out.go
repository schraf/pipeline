package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
	"golang.org/x/sync/errgroup"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║FanOut distributes items from a single input channel to multiple output    ║
// ║channels, sending each item to all output channels.                        ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type FanOutStage[T any] struct {
	Name        string
	OutputCount int
	Buffer      int
}

func (s FanOutStage[T]) Create(ctx pipeline.Context, in <-chan T) pipeline.MultiChannelReceiver[T] {
	outputs := make([]chan T, s.OutputCount)
	for i := range outputs {
		outputs[i] = make(chan T, s.Buffer)
	}

	ctx.Go(s.Name, func(pctx context.Context) error {
		defer func() {
			for _, outputChannel := range outputs {
				close(outputChannel)
			}
		}()

		group, gctx := errgroup.WithContext(pctx)

		for input := range in {
			capturedInput := input

			for _, outputChannel := range outputs {
				capturedOutputChannel := outputChannel

				group.Go(func() error {
					select {
					case <-gctx.Done():
						return gctx.Err()
					case capturedOutputChannel <- capturedInput:
					}

					return nil
				})
			}
		}

		return group.Wait()
	})

	return pipeline.NewMultiChannelReceiver[T](outputs...)
}
