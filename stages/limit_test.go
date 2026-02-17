package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestLimitStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.LimitStage[int]{
				Name:   "test-limit",
				Buffer: 5,
				Limit:  5,
			}.Create(pctx, in)
		},
	)

	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, results)
}

func TestLimitStage_ZeroLimit(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.LimitStage[int]{
				Name:   "test-limit",
				Buffer: 5,
				Limit:  0,
			}.Create(pctx, in)
		},
	)

	assert.Empty(t, results)
}

func TestLimitStage_FewerItemsThanLimit(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3},
		func(pctx pipeline.Context, in <-chan int) <-chan int {
			return stages.LimitStage[int]{
				Name:   "test-limit",
				Buffer: 10,
				Limit:  10,
			}.Create(pctx, in)
		},
	)

	expected := []int{1, 2, 3}
	assert.Equal(t, expected, results)
}
