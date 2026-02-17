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

func TestFanOutStage_Success(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  3,
		OutputBufferSize: 3,
	})

	stage := stages.FanOutStage[int]{
		Name:        "test-fan-out",
		OutputCount: 3,
		Buffer:      3,
	}

	outputs := stage.Create(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, 1, 2, 3)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	checkOutput := func(ch <-chan int, name string, count int) {
		t.Helper()
		var results []int
		for i := 0; i < count; i++ {
			select {
			case v := <-ch:
				results = append(results, v)
			case <-time.After(1 * time.Second):
				require.Failf(t, "timeout", "%s: timeout waiting for value %d", name, i+1)
				return
			}
		}

		assert.Len(t, results, count, "%s: unexpected number of results", name)
		expectedSet := map[int]bool{1: true, 2: true, 3: true}

		for _, v := range results {
			assert.Truef(t, expectedSet[v], "%s: unexpected value: %d", name, v)
			delete(expectedSet, v)
		}

		assert.Empty(t, expectedSet, "%s: missing values", name)
	}

	checkOutput(outputs.At(0), "out0", 3)
	checkOutput(outputs.At(1), "out1", 3)
	checkOutput(outputs.At(2), "out2", 3)
}

func TestFanOutStage_SingleOutput(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 5,
	})

	stage := stages.FanOutStage[int]{
		Name:        "test-fan-out",
		OutputCount: 1,
		Buffer:      5,
	}

	outputs := stage.Create(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range outputs.At(0) {
		results = append(results, v)
	}

	assert.Len(t, results, 5)
}
