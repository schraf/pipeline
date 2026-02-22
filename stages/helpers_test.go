package stages_test

import (
	"context"
	"testing"
	"time"

	"github.com/schraf/pipeline/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 10 * time.Second

func runStageTest[In any, Out any](
	t *testing.T,
	inputs []In,
	composer func(composer pipeline.Composer[In, Out]) error,
) []Out {
	t.Helper()

	bufSize := len(inputs)
	if bufSize < 1 {
		bufSize = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	p, _, err := pipeline.NewPipeline[In, Out](ctx, pipeline.Config[In, Out]{
		Name:             "test",
		InputBufferSize:  uint(bufSize),
		OutputBufferSize: uint(bufSize),
		Composer:         composer,
	})
	require.NoError(t, err)

	require.NoError(t, p.Inputs().Send(ctx, 0, inputs...))
	p.CloseAllInputs()

	results := p.Outputs().SinkAt(ctx, 0)

	require.NoError(t, p.Wait())

	return results
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
