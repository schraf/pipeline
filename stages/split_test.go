package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStage(t *testing.T) {
	ctx := context.Background()

	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputBufferSize:  6,
		OutputChannels:   3,
		OutputBufferSize: 3,
		Composer: func(composer pipeline.Composer[int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			outputs.LinkAll(ctx, stages.SplitStage[int]{
				Name:        "test-split",
				OutputCount: 3,
				Buffer:      3,
				Selector: func(_ context.Context, x int) int {
					return (x - 1) % 3
				},
			}.Create(ctx, inputs.At(0)))
		},
	})

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6)

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	results1 := p.Outputs().SinkAt(ctx, 0)
	results2 := p.Outputs().SinkAt(ctx, 1)
	results3 := p.Outputs().SinkAt(ctx, 2)

	expected1 := []int{1, 4}
	expected2 := []int{2, 5}
	expected3 := []int{3, 6}

	assert.Equal(t, expected1, results1, "out1: unexpected values")
	assert.Equal(t, expected2, results2, "out2: unexpected values")
	assert.Equal(t, expected3, results3, "out3: unexpected values")
}
