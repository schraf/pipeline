package stages_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelTransformStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform",
				Buffer:  10,
				Workers: 3,
				Transformer: func(_ context.Context, x int) (*int, error) {
					time.Sleep(10 * time.Millisecond)
					result := x * 2
					return &result, nil
				},
			}.Create(pctx, in)
		},
	)

	// Results may be out of order due to parallel processing
	expected := make([]int, 10)
	for i := 0; i < 10; i++ {
		expected[i] = (i + 1) * 2
	}
	assertUnorderedEqual(t, expected, results)
}

func TestParallelTransformStage_Error(t *testing.T) {
	expectedErr := errors.New("transform error")

	runStageErrorTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform",
				Buffer:  10,
				Workers: 3,
				Transformer: func(_ context.Context, x int) (*int, error) {
					if x == 5 {
						return nil, expectedErr
					}
					result := x * 2
					return &result, nil
				},
			}.Create(pctx, in)
		},
	)
}

func TestParallelTransformStage_NilOutput(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 5,
	})

	stage := stages.ParallelTransformStage[int, int]{
		Name:    "test-parallel-transform",
		Buffer:  5,
		Workers: 2,
		Transformer: func(_ context.Context, x int) (*int, error) {
			return nil, nil
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
