package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/require"
)

func TestFanInStage(t *testing.T) {
	ctx := context.Background()

	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputChannels:    3,
		InputBufferSize:  3,
		OutputBufferSize: 10,
		Composer: func(composer pipeline.Composer[int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			outputs.Link(ctx, 0, stages.FanInStage[int]{
				Name:   "test-fan-in",
				Buffer: 10,
			}.Create(ctx, inputs))
		},
	})

	// Send values to each input channel using round-robin
	p.Inputs().SendRoundRobin(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	results := p.Outputs().SinkAt(ctx, 0)

	// Check all values are present (order may vary due to concurrency)
	assertUnorderedEqual(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, results)
}
