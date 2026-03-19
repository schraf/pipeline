package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
	"golang.org/x/sync/errgroup"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║FanOut distributes items from a single input channel to multiple output    ║
// ║channels, sending each item to all output channels.                        ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type FanOutStage[T any] struct {
	Name        string
	OutputCount uint
	Buffer      uint
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
		defer pipeline.DrainChannel(in)

		group, gctx := errgroup.WithContext(pctx)
		group.SetLimit(int(s.OutputCount))

		for {
			select {
			case <-pctx.Done():
				return group.Wait()
			case input, ok := <-in:
				if !ok {
					return group.Wait()
				}

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
		}
	})

	return pipeline.NewMultiChannelReceiver[T](outputs...)
}
