package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestAggregateStage_Success(t *testing.T) {
	results := runStageTest(t, []int{1, 2, 3, 4, 5},
		func(pctx pipeline.Context, in <-chan int) <-chan []int {
			return stages.AggregateStage[int]{
				Name:   "test-aggregate",
				Buffer: 1,
			}.Create(pctx, in)
		},
	)

	expected := [][]int{{1, 2, 3, 4, 5}}
	assert.Equal(t, expected, results)
}

func TestAggregateStage_Empty(t *testing.T) {
	results := runStageTest(t, []int{},
		func(pctx pipeline.Context, in <-chan int) <-chan []int {
			return stages.AggregateStage[int]{
				Name:   "test-aggregate",
				Buffer: 1,
			}.Create(pctx, in)
		},
	)

	assert.Len(t, results, 1)
	assert.Empty(t, results[0])
}
