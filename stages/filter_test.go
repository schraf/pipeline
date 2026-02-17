package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestFilterStage(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(composer pipeline.Composer[int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			outputs.Link(ctx, 0, stages.FilterStage[int]{
				Name:   "test-filter",
				Buffer: 10,
				Filter: func(_ context.Context, x int) (bool, error) {
					return x%2 == 0, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}
