package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/require"
)

func TestFanOutStage(t *testing.T) {
	ctx := context.Background()

	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputBufferSize:  3,
		OutputChannels:   3,
		OutputBufferSize: 3,
		Composer: func(composer pipeline.Composer[int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			outputs.LinkAll(ctx, stages.FanOutStage[int]{
				Name:        "test-fan-out",
				OutputCount: 3,
				Buffer:      3,
			}.Create(ctx, inputs.At(0)))
		},
	})

	p.Inputs().Send(ctx, 0, 1, 2, 3)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	results1 := p.Outputs().SinkAt(ctx, 0)
	results2 := p.Outputs().SinkAt(ctx, 1)
	results3 := p.Outputs().SinkAt(ctx, 2)

	expected := []int{1, 2, 3}
	assertUnorderedEqual(t, expected, results1)
	assertUnorderedEqual(t, expected, results2)
	assertUnorderedEqual(t, expected, results3)
}
