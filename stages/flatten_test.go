package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestFlattenStage_Success(t *testing.T) {
	results := runStageTest(t, [][]int{{1, 2}, {3, 4, 5}, {6}},
		func(pctx pipeline.Context, in <-chan []int) <-chan int {
			return stages.FlattenStage[int]{
				Name:   "test-flatten",
				Buffer: 10,
			}.Create(pctx, in)
		},
	)

	expected := []int{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expected, results)
}

func TestFlattenStage_EmptySlices(t *testing.T) {
	results := runStageTest(t, [][]int{{}, {1}, {}},
		func(pctx pipeline.Context, in <-chan []int) <-chan int {
			return stages.FlattenStage[int]{
				Name:   "test-flatten",
				Buffer: 10,
			}.Create(pctx, in)
		},
	)

	expected := []int{1}
	assert.Equal(t, expected, results)
}
