package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanInStage_Success(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    3,
		InputBufferSize:  3,
		OutputBufferSize: 10,
	})

	stage := stages.FanInStage[int]{
		Name:   "test-fan-in",
		Buffer: 10,
	}

	out := stage.Create(p.Context(), p.Inputs().Receivers())

	// Send values to each input channel using round-robin
	p.Inputs().SendRoundRobin(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	// Check all values are present (order may vary due to concurrency)
	assertUnorderedEqual(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, results)
}

func TestFanInStage_SingleInput(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    1,
		InputBufferSize:  5,
		OutputBufferSize: 5,
	})

	stage := stages.FanInStage[int]{
		Name:   "test-fan-in",
		Buffer: 5,
	}

	out := stage.Create(p.Context(), p.Inputs().Receivers())

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, results)
}
