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

func TestBatchStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.BatchStage[int, int]{
				Name:      "test-batch",
				Buffer:    10,
				BatchSize: 3,
				Batcher: func(_ context.Context, batch []int) (*int, error) {
					sum := 0
					for _, v := range batch {
						sum += v
					}
					return &sum, nil
				},
			}.Create(pctx, in)
		},
	)

	// First batch: 1+2+3=6, second batch: 4+5+6=15, remaining: 7
	expected := []int{6, 15, 7}
	assert.Equal(t, expected, results)
}

func TestBatchStage_Error(t *testing.T) {
	expectedErr := errors.New("batch error")

	runStageErrorTest(t, []int{1, 2, 3, 4, 5, 6, 7}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.BatchStage[int, int]{
				Name:      "test-batch",
				Buffer:    10,
				BatchSize: 3,
				Batcher: func(_ context.Context, batch []int) (*int, error) {
					// Error on second batch
					if batch[0] == 4 {
						return nil, expectedErr
					}
					sum := 0
					for _, v := range batch {
						sum += v
					}
					return &sum, nil
				},
			}.Create(pctx, in)
		},
	)
}

func TestBatchStage_ExactMultiple(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		func(pctx pipeline.Context, in <-chan int) <-chan []int {
			return stages.BatchStage[int, []int]{
				Name:      "test-batch",
				Buffer:    5,
				BatchSize: 3,
				Batcher: func(_ context.Context, batch []int) (*[]int, error) {
					return &batch, nil
				},
			}.Create(pctx, in)
		},
	)

	assert.Len(t, results, 3)
	assert.Equal(t, []int{1, 2, 3}, results[0])
	assert.Equal(t, []int{4, 5, 6}, results[1])
	assert.Equal(t, []int{7, 8, 9}, results[2])
}

func TestBatchStage_NilOutput(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 5,
	})

	stage := stages.BatchStage[int, int]{
		Name:      "test-batch",
		Buffer:    5,
		BatchSize: 3,
		Batcher: func(_ context.Context, batch []int) (*int, error) {
			return nil, nil
		},
	}

	_ = stage.Create(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, 1, 2, 3)
	p.CloseAllInputs()
	p.Start()

	err := p.Wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil output")
}
