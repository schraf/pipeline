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
	OutputCount int
	Buffer      int
}

func (s FanOutRoundRobinStage[T]) Create(ctx pipeline.Context, in <-chan T) pipeline.MultiChannelReceiver[T] {
	if s.OutputCount == 0 {
		panic("FanOutRoundRobinStage: OutputCount must be greater than zero")
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

		index := 0

		for input := range in {
			outputChannel := outputs[index%len(outputs)]

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputChannel <- input:
			}

			index++
		}

		return nil
	})

	return pipeline.NewMultiChannelReceiver[T](outputs...)
}
