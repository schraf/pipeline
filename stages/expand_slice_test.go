package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestExpandSliceStage_MultipleSlices(t *testing.T) {
	results := runStageTest(t, [][]int{{1, 2}, {3, 4, 5}, {6}},
		func(composer pipeline.Composer[[]int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			outputs.Link(ctx, 0, stages.ExpandSliceStage[int]{
				Name:   "test-expand-slice",
				Buffer: 10,
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expected, results)
}
