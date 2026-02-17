package stages_test

import (
	"context"
	"errors"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestReduceStage_Sum(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ReduceStage[int, int]{
				Name:    "test-reduce-sum",
				Buffer:  1,
				Initial: 0,
				Reducer: func(_ context.Context, acc int, val int) (int, error) {
					return acc + val, nil
				},
			}.Create(pctx, in)
		},
	)

	assert.Equal(t, []int{15}, results) // 1+2+3+4+5
}

func TestReduceStage_StringConcatenation(t *testing.T) {
	results := runStageTest(t, []string{"hello", " ", "world", "!"},
		func(pctx pipeline.Context, in <-chan string) <-chan string {
			return stages.ReduceStage[string, string]{
				Name:    "test-reduce-string",
				Buffer:  1,
				Initial: "",
				Reducer: func(_ context.Context, acc string, val string) (string, error) {
					return acc + val, nil
				},
			}.Create(pctx, in)
		},
	)

	assert.Equal(t, []string{"hello world!"}, results)
}

func TestReduceStage_MaxValue(t *testing.T) {
	results := runStageTest(t, []int{3, 7, 2, 9, 1, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ReduceStage[int, int]{
				Name:    "test-reduce-max",
				Buffer:  1,
				Initial: 0,
				Reducer: func(_ context.Context, acc int, val int) (int, error) {
					if val > acc {
						return val, nil
					}
					return acc, nil
				},
			}.Create(pctx, in)
		},
	)

	assert.Equal(t, []int{9}, results)
}

func TestReduceStage_Error(t *testing.T) {
	expectedErr := errors.New("reduce error")

	runStageErrorTest(t, []int{1, 2, 3, 4, 5}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ReduceStage[int, int]{
				Name:    "test-reduce",
				Buffer:  1,
				Initial: 0,
				Reducer: func(_ context.Context, acc int, val int) (int, error) {
					if val == 3 {
						return 0, expectedErr
					}
					return acc + val, nil
				},
			}.Create(pctx, in)
		},
	)
}
