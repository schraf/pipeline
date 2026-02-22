package stages_test

import (
	"context"
	"testing"
	"time"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
)

func TestParallelTransformStage(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform",
				Buffer:  10,
				Workers: 3,
				Transformer: func(_ context.Context, x int) (int, error) {
					time.Sleep(10 * time.Millisecond)
					result := x * 2
					return result, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	// Results may be out of order due to parallel processing
	expected := make([]int, 10)
	for i := 0; i < 10; i++ {
		expected[i] = (i + 1) * 2
	}
	assertUnorderedEqual(t, expected, results)
}
