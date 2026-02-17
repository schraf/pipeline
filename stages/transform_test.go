package stages_test

import (
	"context"
	"errors"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.TransformStage[int, int]{
				Name:   "test-transform",
				Buffer: 5,
				Transformer: func(_ context.Context, x int) (*int, error) {
					result := x * 2
					return &result, nil
				},
			}.Create(pctx, in)
		},
	)

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestTransformStage_Error(t *testing.T) {
	expectedErr := errors.New("transform error")

	runStageErrorTest(t, []int{1, 2, 3, 4, 5}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.TransformStage[int, int]{
				Name:   "test-transform",
				Buffer: 5,
				Transformer: func(_ context.Context, x int) (*int, error) {
					if x == 3 {
						return nil, expectedErr
					}
					result := x * 2
					return &result, nil
				},
			}.Create(pctx, in)
		},
	)
}

func TestTransformStage_NilOutput(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 5,
	})

	stage := stages.TransformStage[int, int]{
		Name:   "test-transform",
		Buffer: 5,
		Transformer: func(_ context.Context, x int) (*int, error) {
			return nil, nil // Returns nil without error
		},
	}

	_ = stage.Create(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, 1)
	p.CloseAllInputs()
	p.Start()

	err := p.Wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil output")
}
