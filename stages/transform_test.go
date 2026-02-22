package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
	"github.com/stretchr/testify/assert"
)

func TestTransformStage(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.TransformStage[int, int]{
				Name:   "test-transform",
				Buffer: 5,
				Transformer: func(_ context.Context, x int) (int, error) {
					result := x * 2
					return result, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}
