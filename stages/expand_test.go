package stages_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestExpandStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ExpandStage[int, int]{
				Name:   "test-expand",
				Buffer: 10,
				Expander: func(_ context.Context, x int) iter.Seq2[int, error] {
					// For each input, output x, x*2, x*3
					return func(yield func(int, error) bool) {
						yield(x, nil)
						yield(x*2, nil)
						yield(x*3, nil)
					}
				},
			}.Create(pctx, in)
		},
	)

	expected := []int{1, 2, 3, 2, 4, 6, 3, 6, 9}
	assert.Equal(t, expected, results)
}

func TestExpandStage_Error(t *testing.T) {
	expectedErr := errors.New("expand error")

	runStageErrorTest(t, []int{1, 2, 3}, expectedErr,
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ExpandStage[int, int]{
				Name:   "test-expand",
				Buffer: 10,
				Expander: func(_ context.Context, x int) iter.Seq2[int, error] {
					return func(yield func(int, error) bool) {
						if x == 2 {
							yield(0, expectedErr)
							return
						}
						yield(x, nil)
					}
				},
			}.Create(pctx, in)
		},
	)
}

func TestExpandStage_EmptyIterator(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.ExpandStage[int, int]{
				Name:   "test-expand",
				Buffer: 10,
				Expander: func(_ context.Context, x int) iter.Seq2[int, error] {
					return func(yield func(int, error) bool) {
						// Empty iterator - yield nothing
					}
				},
			}.Create(pctx, in)
		},
	)

	assert.Empty(t, results)
}
