package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Chain connects the outputs of the previous pipeline to the inputs of a new pipeline
// created from the provided configuration. It manages the data transfer between
// the pipelines automatically.
//
// The connection handles data transfer:
// - Output/input counts must match for a 1-to-1 connection.
//
// Data transfer stops and the new pipeline's inputs are closed when the previous
// pipeline's outputs are closed.
func Chain[In any, Middle any, Out any](
	ctx context.Context,
	prev *Pipeline[In, Middle],
	cfg PipelineConfig[Middle, Out],
) (*Pipeline[Middle, Out], context.Context, error) {
	// Normalize input channels to match NewPipeline behavior for validation
	inputChannels := cfg.InputChannels
	if inputChannels == 0 {
		inputChannels = 1
	}

	if prev.Outputs().Len() != inputChannels {
		return nil, ctx, fmt.Errorf("pipeline chaining requires matching channel counts: %d outputs != %d inputs", prev.Outputs().Len(), inputChannels)
	}

	nextPipeline, nextCtx := NewPipeline(ctx, cfg)

	go connect(nextCtx, prev.Outputs(), nextPipeline.Inputs())

	return nextPipeline, nextCtx, nil
}

func connect[T any](ctx context.Context, src MultiChannelReceiver[T], dest MultiChannelSender[T]) {
	var group sync.WaitGroup

	// Ensure destination channels are closed when we're done
	defer func() {
		for i := 0; i < dest.Len(); i++ {
			close(dest.At(i))
		}
	}()

	// 1-to-1 connection
	for i := 0; i < src.Len(); i++ {
		group.Add(1)

		go func(in <-chan T, out chan<- T) {
			defer group.Done()
			for val := range in {
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}(src.At(i), dest.At(i))
	}

	group.Wait()
}
