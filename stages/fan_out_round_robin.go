package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔═══════════════════════════════════════════════════════════════════════════════╗
// ║FanOutRoundRobin distributes items from a single input channel to multiple     ║
// ║output channels using round-robin distribution, sending each item to only one  ║
// ║output channel. Panics if OutputCount is zero.                                 ║
// ╚═══════════════════════════════════════════════════════════════════════════════╝

type FanOutRoundRobinStage[T any] struct {
	Name        string
	OutputCount uint
	Buffer      uint
}

func (s FanOutRoundRobinStage[T]) Create(ctx pipeline.Context, in <-chan T) pipeline.MultiChannelReceiver[T] {
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

		index := 0

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					return nil
				}

				outputChannel := outputs[index]

				select {
				case <-ctx.Done():
					return ctx.Err()
				case outputChannel <- input:
				}

				index++
				if index >= len(outputs) {
					index = 0
				}
			}
		}
	})

	return pipeline.NewMultiChannelReceiver[T](outputs...)
}
