package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
	"github.com/stretchr/testify/assert"
)

func TestFlattenStage(t *testing.T) {
	results := runStageTest(t, [][]int{{1, 2}, {3, 4, 5}, {6}},
		func(composer pipeline.Composer[[]int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.FlattenStage[int]{
				Name:   "test-flatten",
				Buffer: 10,
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expected, results)
}
