package stages_test

import (
	"context"
	"errors"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestFilterStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.FilterStage[int]{
				Name:   "test-filter",
				Buffer: 10,
				Filter: func(_ context.Context, x int) (bool, error) {
					return x%2 == 0, nil
				},
			}.Create(pctx, in)
		},
	)

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestFilterStage_Error(t *testing.T) {
	expectedErr := errors.New("filter error")

	runStageErrorTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.FilterStage[int]{
				Name:   "test-filter",
				Buffer: 10,
				Filter: func(_ context.Context, x int) (bool, error) {
					if x == 5 {
						return false, expectedErr
					}
					return x%2 == 0, nil
				},
			}.Create(pctx, in)
		},
	)
}

func TestFilterStage_AllFiltered(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.FilterStage[int]{
				Name:   "test-filter",
				Buffer: 5,
				Filter: func(_ context.Context, x int) (bool, error) {
					return false, nil // Filter out everything
				},
			}.Create(pctx, in)
		},
	)

	assert.Empty(t, results)
}
