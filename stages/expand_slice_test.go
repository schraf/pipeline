package stages_test

import (
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
)

func TestExpandSliceStage_Success(t *testing.T) {
	data := []int{1, 2, 3, 4, 5}

	results := runStageTest(t, [][]int{data},
		func(pctx pipeline.Context, in <-chan []int) <-chan int {
			return stages.ExpandSliceStage[int]{
				Name:   "test-expand-slice",
				Buffer: 5,
			}.Create(pctx, in)
		},
	)

	assert.Equal(t, data, results)
}

func TestExpandSliceStage_MultipleSlices(t *testing.T) {
	results := runStageTest(t, [][]int{{1, 2}, {3, 4, 5}, {6}},
		func(pctx pipeline.Context, in <-chan []int) <-chan int {
			return stages.ExpandSliceStage[int]{
				Name:   "test-expand-slice",
				Buffer: 10,
			}.Create(pctx, in)
		},
	)

	expected := []int{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expected, results)
}
