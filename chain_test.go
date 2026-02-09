package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChain_Simple(t *testing.T) {
	// First pipeline: Multiplies by 2
	cfg1 := PipelineConfig[int, int]{
		Name:            "p1",
		InputBufferSize: 5,
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			Transform("p1-transform", pipe, func(_ context.Context, x int) (*int, error) {
				res := x * 2
				return &res, nil
			}, in.At(0), out.At(0))
		},
	}

	// Second pipeline: Adds 1
	cfg2 := PipelineConfig[int, int]{
		Name:            "p2",
		InputBufferSize: 5,
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			Transform("p2-transform", pipe, func(_ context.Context, x int) (*int, error) {
				res := x + 1
				return &res, nil
			}, in.At(0), out.At(0))
		},
	}

	ctx := context.Background()
	p1, _ := NewPipeline(ctx, cfg1)
	p2, _, err := Chain(ctx, p1, cfg2)

	require.NoError(t, err)

	// Start both pipelines
	p1.Start()
	p2.Start()

	// Feed inputs to p1
	p1.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5)
	p1.CloseAllInputs()

	// Collect results from p2
	var results []int
	for v := range p2.Outputs().At(0) {
		results = append(results, v)
	}

	// Wait for p2 to finish
	require.NoError(t, p2.Wait())

	// Expected: (x * 2) + 1
	// 1 -> 3
	// 2 -> 5
	// 3 -> 7
	// 4 -> 9
	// 5 -> 11
	expected := []int{3, 5, 7, 9, 11}
	assert.Equal(t, expected, results)
}

func TestChain_Cancellation(t *testing.T) {
	// P1: Infinite generator
	cfg1 := PipelineConfig[int, int]{
		Name: "p1",
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			go func() {
				defer close(out.At(0))
				i := 0
				for {
					select {
					case <-pipe.ctx.Done():
						return
					case out.At(0) <- i:
						i++
					}
				}
			}()
		},
	}

	// P2: Consumes a few then cancels
	cfg2 := PipelineConfig[int, int]{
		Name: "p2",
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			Limit("p2-limit", pipe, 5, in.At(0), out.At(0))
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1, _ := NewPipeline(ctx, cfg1)
	p2, _, err := Chain(ctx, p1, cfg2)

	require.NoError(t, err)

	p1.Start()
	p2.Start()

	// P2 should finish after limit
	// Wait logic inside Limit closes output after limit is reached, but doesn't necessarily cancel context?
	// Limit function respects cancellation but returns naturally.
	// So p2.Wait() should return nil.

	// Consume p2 output
	count := 0
	for range p2.Outputs().At(0) {
		count++
	}
	assert.Equal(t, 5, count)

	err = p2.Wait()
	require.NoError(t, err)

	// Cleanup
	cancel()
}

func TestChain_DirectConnection(t *testing.T) {
	// P1: 2 Outputs
	cfg1 := PipelineConfig[int, int]{
		Name:           "p1",
		OutputChannels: 2,
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			FanOutRoundRobin("p1-fanout", pipe, in.At(0), out.At(0), out.At(1))
		},
	}

	// P2: 2 Inputs (should be 1-to-1 connected)
	cfg2 := PipelineConfig[int, int]{
		Name:          "p2",
		InputChannels: 2,
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
			// Process inputs separately to verify mapping
			// Input 0 -> Output 0
			// Input 1 -> Output 0
			FanIn("p2-fanin", pipe, out.At(0), in.At(0), in.At(1))
		},
	}

	ctx := context.Background()
	p1, _ := NewPipeline(ctx, cfg1)
	p2, _, err := Chain(ctx, p1, cfg2)

	require.NoError(t, err)

	p1.Start()
	p2.Start()

	go func() {
		defer p1.CloseAllInputs()
		// Send 10 items
		for i := 1; i <= 10; i++ {
			p1.Inputs().At(0) <- i
		}
	}()

	var results []int
	for v := range p2.Outputs().At(0) {
		results = append(results, v)
	}

	require.NoError(t, p2.Wait())

	assert.Len(t, results, 10)
}

func TestChain_Mismatch_Panic(t *testing.T) {
	// P1: 1 Output
	cfg1 := PipelineConfig[int, int]{
		Name: "p1",
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
		},
	}

	// P2: 2 Inputs
	cfg2 := PipelineConfig[int, int]{
		Name:          "p2",
		InputChannels: 2,
		Executor: func(pipe *Pipe, in MultiChannelReceiver[int], out MultiChannelSender[int]) {
		},
	}

	ctx := context.Background()
	p1, _ := NewPipeline(ctx, cfg1)

	_, _, err := Chain(ctx, p1, cfg2)
	assert.Error(t, err)
}
