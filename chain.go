package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Chain connects the outputs of the previous pipeline to the inputs of new pipelines
// created from the provided configurations. It manages the data transfer between
// the pipelines automatically.
//
// The connection handles data transfer:
// - Output/input counts must match for a 1-to-1 connection.
// - Outputs will fan-out to each new pipeline
//
// Data transfer stops and the new pipelines' inputs are closed when the previous
// pipeline's outputs are closed.
func Chain[In any, Middle any, Out any](
	prev *Pipeline[In, Middle],
	configs ...PipelineConfig[Middle, Out],
) ([]*Pipeline[Middle, Out], error) {
	var pipelines []*Pipeline[Middle, Out]
	var inputs []MultiChannelSender[Middle]

	for _, config := range configs {
		inputChannels := config.InputChannels

		if inputChannels == 0 {
			inputChannels = 1
		}

		if prev.Outputs().Len() != inputChannels {
			return nil, fmt.Errorf("pipeline chaining requires matching channel counts: %d outputs != %d inputs", prev.Outputs().Len(), inputChannels)
		}

		pipeline, _ := NewPipeline(prev.ctx, config)

		pipelines = append(pipelines, pipeline)
		inputs = append(inputs, pipeline.Inputs())
	}

	go connect(prev.ctx, prev.Outputs(), inputs)

	return pipelines, nil
}

func connect[T any](ctx context.Context, source MultiChannelReceiver[T], destinations []MultiChannelSender[T]) {
	var group sync.WaitGroup

	// Ensure destination channels are closed when we're done
	defer func() {
		for _, destination := range destinations {
			for input := range destination.Iter() {
				close(input)
			}
		}
	}()

	for i := 0; i < source.Len(); i++ {
		group.Add(1)

		var outputs []chan<- T

		for _, destination := range destinations {
			outputs = append(outputs, destination.At(i))
		}

		go func(input <-chan T, outputs []chan<- T) {
			defer group.Done()

			// send value to each destination
			for value := range input {
				for _, output := range outputs {
					select {
					case <-ctx.Done():
						return
					case output <- value:
					}
				}
			}
		}(source.At(i), outputs)
	}

	group.Wait()
}
