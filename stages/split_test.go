package stages_test

import (
	"context"
	"testing"
	"time"

	"github.com/schraf/pipeline/v3"
	"github.com/schraf/pipeline/v3/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStage_Success(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  6,
		OutputBufferSize: 3,
	})

	stage := stages.SplitStage[int]{
		Name:        "test-split",
		OutputCount: 3,
		Buffer:      3,
		Selector: func(_ context.Context, x int) int {
			return (x - 1) % 3
		},
	}

	outputs := stage.Create(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results1, results2, results3 []int
	for i := 0; i < 2; i++ {
		select {
		case v := <-outputs.At(0):
			results1 = append(results1, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out1")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-outputs.At(1):
			results2 = append(results2, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out2")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-outputs.At(2):
			results3 = append(results3, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out3")
		}
	}

	expected1 := []int{1, 4}
	expected2 := []int{2, 5}
	expected3 := []int{3, 6}

	assert.Equal(t, expected1, results1, "out1: unexpected values")
	assert.Equal(t, expected2, results2, "out2: unexpected values")
	assert.Equal(t, expected3, results3, "out3: unexpected values")
}

// Note: TestSplitStage_PanicOnInvalidIndex is not included because the panic
// occurs in a goroutine and cannot be easily caught in a test. In production,
// an invalid selector index would cause the program to crash.
