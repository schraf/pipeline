package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestReduceStage(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ReduceStage[int, int]{
				Name:    "test-reduce-sum",
				Buffer:  1,
				Initial: 0,
				Reducer: func(_ context.Context, acc int, val int) (int, error) {
					return acc + val, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	assert.Equal(t, []int{15}, results) // 1+2+3+4+5
}
