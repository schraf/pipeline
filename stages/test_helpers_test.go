package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runStageTest creates a pipeline, wires a single stage via the createStage
// callback, sends the provided inputs, runs the pipeline, and returns the
// collected output values. It fails the test if the pipeline returns an error.
func runStageTest[In any, Out any](
	t *testing.T,
	inputs []In,
	createStage func(pctx pipeline.Context, in <-chan In) <-chan Out,
) []Out {
	t.Helper()

	bufSize := len(inputs)
	if bufSize < 1 {
		bufSize = 1
	}

	ctx := context.Background()
	p, _ := pipeline.NewPipeline[In, Out](ctx, pipeline.PipelineConfig[In, Out]{
		Name:             "test",
		InputBufferSize:  bufSize,
		OutputBufferSize: bufSize,
	})

	out := createStage(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, inputs...)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []Out
	for v := range out {
		results = append(results, v)
	}
	return results
}

// runStageErrorTest creates a pipeline, wires a single stage that is expected
// to produce an error, sends input, runs the pipeline, and asserts the
// pipeline error matches expectedErr using errors.Is.
func runStageErrorTest[In any, Out any](
	t *testing.T,
	inputs []In,
	expectedErr error,
	createStage func(pctx pipeline.Context, in <-chan In) <-chan Out,
) {
	t.Helper()

	bufSize := len(inputs)
	if bufSize < 1 {
		bufSize = 1
	}

	ctx := context.Background()
	p, _ := pipeline.NewPipeline[In, Out](ctx, pipeline.PipelineConfig[In, Out]{
		Name:             "test",
		InputBufferSize:  bufSize,
		OutputBufferSize: bufSize,
	})

	_ = createStage(p.Context(), p.Inputs().Receiver(0))

	p.Inputs().Send(ctx, 0, inputs...)
	p.CloseAllInputs()
	p.Start()

	err := p.Wait()
	require.ErrorIs(t, err, expectedErr)
}

// assertUnorderedEqual checks that actual contains exactly the same elements
// as expected, regardless of order. T must be comparable.
func assertUnorderedEqual[T comparable](t *testing.T, expected []T, actual []T) {
	t.Helper()

	assert.Len(t, actual, len(expected))

	remaining := make(map[T]int, len(expected))
	for _, v := range expected {
		remaining[v]++
	}

	for _, v := range actual {
		count, ok := remaining[v]
		if !ok || count == 0 {
			assert.Failf(t, "unexpected value", "got %v which is not in expected set", v)
			continue
		}
		remaining[v] = count - 1
		if remaining[v] == 0 {
			delete(remaining, v)
		}
	}

	assert.Empty(t, remaining, "missing values from results")
}
