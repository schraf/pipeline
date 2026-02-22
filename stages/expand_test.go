package stages_test

import (
	"context"
	"iter"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestExpandStage(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ExpandStage[int, int]{
				Name:   "test-expand",
				Buffer: 10,
				Expander: func(ctx context.Context, x int) iter.Seq2[int, error] {
					// For each input, output x, x*2, x*3
					return func(yield func(int, error) bool) {
						yield(x, nil)
						yield(x*2, nil)
						yield(x*3, nil)
					}
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{1, 2, 3, 2, 4, 6, 3, 6, 9}
	assert.Equal(t, expected, results)
}
