package pipeline

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpand_Success(t *testing.T) {
	cfg := PipelineConfig[[]int, int]{
		Name:             "test",
		InputBufferSize:  1,
		OutputBufferSize: 5,
		Executor: func(params *PipelineParameters[[]int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			ExpandSlice("test-source", pipe, in.At(0), out.At(0))
		},
	}

	data := []int{1, 2, 3, 4, 5}

	p, _ := NewPipeline(context.Background(), cfg)
	p.Inputs().At(0) <- data
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.Equal(t, data, results)
}

func TestTransform(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 5,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Transform("test-transform", pipe, func(ctx context.Context, x int) (*int, error) {
				result := x * 2
				return &result, nil
			}, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 5; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()

	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestFilter(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Filter("test-filter", pipe, func(ctx context.Context, x int) (bool, error) {
				return x%2 == 0, nil
			}, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 10; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()

	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestBatch(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Batch("test-batch", pipe, func(ctx context.Context, batch []int) (*int, error) {
				sum := 0
				for _, v := range batch {
					sum += v
				}

				return &sum, nil
			}, 3, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 7; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()

	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	// First batch: 1+2+3=6, second batch: 4+5+6=15, remaining: 7
	expected := []int{6, 15, 7}
	assert.Equal(t, expected, results)
}

func TestFanIn(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    3,
		InputBufferSize:  3,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			FanIn("test-fan-in", pipe, out.At(0), in.At(0), in.At(1), in.At(2))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, 1, 2, 3)
	p.Inputs().Send(ctx, 1, 4, 5, 6)
	p.Inputs().Send(ctx, 2, 7, 8, 9)
	p.CloseAllInputs()

	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.Len(t, results, 9)

	expected := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true}
	for _, v := range results {
		assert.Truef(t, expected[v], "unexpected value: %d", v)
		delete(expected, v)
	}

	assert.Empty(t, expected, "missing values")
}

func TestFanOut(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    1,
		InputBufferSize:  3,
		OutputChannels:   3,
		OutputBufferSize: 3,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			FanOut("test-fan-out", pipe, in.At(0), out.At(0), out.At(1), out.At(2))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, 1, 2, 3)

	p.CloseAllInputs()

	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

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

	checkOutput(p.Outputs().At(0), "out0", 3)
	checkOutput(p.Outputs().At(1), "out1", 3)
	checkOutput(p.Outputs().At(2), "out2", 3)
}

func TestFanOutRoundRobin(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    1,
		InputBufferSize:  6,
		OutputChannels:   3,
		OutputBufferSize: 3,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			FanOutRoundRobin("test-fan-out-round-robin", pipe, in.At(0), out.At(0), out.At(1), out.At(2))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 6; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results1, results2, results3 []int
	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(0):
			results1 = append(results1, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out1")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(1):
			results2 = append(results2, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out2")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(2):
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

func TestParallelTransform(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			ParallelTransform("test-parallel-transform", pipe, 3, func(_ context.Context, x int) (*int, error) {
				time.Sleep(10 * time.Millisecond)
				result := x * 2
				return &result, nil
			}, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 10; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.Len(t, results, 10)

	expected := map[int]bool{}
	for i := 1; i <= 10; i++ {
		expected[i*2] = true
	}

	for _, v := range results {
		assert.Truef(t, expected[v], "unexpected value: %d", v)
		delete(expected, v)
	}

	assert.Empty(t, expected, "missing values")
}

func TestLimit(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  10,
		OutputBufferSize: 5,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Limit("test-limit", pipe, 5, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 10; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, results)
}

func TestSplit(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  6,
		OutputChannels:   3,
		OutputBufferSize: 3,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Split("test-split", pipe, func(_ context.Context, x int) int {
				return (x - 1) % 3
			}, in.At(0), out.At(0), out.At(1), out.At(2))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 6; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results1, results2, results3 []int
	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(0):
			results1 = append(results1, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out1")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(1):
			results2 = append(results2, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out2")
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case v := <-p.Outputs().At(2):
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

func TestAggregate(t *testing.T) {
	cfg := PipelineConfig[int, []int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 1,
		Executor: func(params *PipelineParameters[int, []int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Aggregate("test-aggregate", pipe, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 5; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	result := <-p.Outputs().At(0)
	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, result)
}

func TestReduce_Sum(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  5,
		OutputBufferSize: 1,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Reduce("test-reduce-sum", pipe, 0, func(_ context.Context, acc int, val int) (int, error) {
				return acc + val, nil
			}, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 5; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	result := <-p.Outputs().At(0)
	expected := 15 // 1+2+3+4+5
	assert.Equal(t, expected, result)
}

func TestReduce_StringConcatenation(t *testing.T) {
	cfg := PipelineConfig[string, string]{
		Name:             "test",
		InputBufferSize:  4,
		OutputBufferSize: 1,
		Executor: func(params *PipelineParameters[string, string]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Reduce("test-reduce-string", pipe, "", func(_ context.Context, acc string, val string) (string, error) {
				return acc + val, nil
			}, in.At(0), out.At(0))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, "hello", " ", "world", "!")
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	result := <-p.Outputs().At(0)
	expected := "hello world!"
	assert.Equal(t, expected, result)
}

func TestReduce_MaxValue(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  6,
		OutputBufferSize: 1,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Reduce("test-reduce-max", pipe, 0, func(_ context.Context, acc int, val int) (int, error) {
				if val > acc {
					return val, nil
				}
				return acc, nil
			}, in.At(0), out.At(0))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, 3, 7, 2, 9, 1, 5)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	result := <-p.Outputs().At(0)
	expected := 9
	assert.Equal(t, expected, result)
}

func TestFlatten(t *testing.T) {
	cfg := PipelineConfig[[]int, int]{
		Name:             "test",
		InputBufferSize:  3,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[[]int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Flatten("test-flatten", pipe, in.At(0), out.At(0))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, []int{1, 2}, []int{3, 4, 5}, []int{6})
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expected, results)
}

func TestExpand(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  3,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Expand("test-expand", pipe, func(_ context.Context, x int) iter.Seq2[int, error] {
				// For each input, output x, x*2, x*3
				return func(yield func(int, error) bool) {
					yield(x, nil)
					yield(x*2, nil)
					yield(x*3, nil)
				}
			}, in.At(0), out.At(0))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, 1, 2, 3)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 2, 4, 6, 3, 6, 9}
	assert.Equal(t, expected, results)
}

// End-to-end tests

func TestPipeline_TransformFilterLimit(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  20,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			// Pipeline: Transform -> Filter -> Limit
			transformed := make(chan int, 20)
			filtered := make(chan int, 20)

			Transform("test-transform-filter-limit-transform", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 2
				return &result, nil
			}, in.At(0), transformed)

			Filter("test-transform-filter-limit-filter", pipe, func(_ context.Context, x int) (bool, error) {
				return x > 20, nil
			}, transformed, filtered)

			Limit("test-transform-filter-limit-limit", pipe, 5, filtered, out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 20; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 20, "expected all results > 20")
		assert.Equalf(t, 0, v%2, "expected all results to be even, got %d", v)
	}
}

func TestPipeline_ParallelTransformBatch(t *testing.T) {
	cfg := PipelineConfig[int, []int]{
		Name:             "test",
		InputBufferSize:  30,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, []int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			// Pipeline: ParallelTransform -> Batch
			transformed := make(chan int, 30)

			ParallelTransform("test-parallel-transform-batch-transform", pipe, 5, func(_ context.Context, x int) (*int, error) {
				time.Sleep(5 * time.Millisecond)
				result := x * 3
				return &result, nil
			}, in.At(0), transformed)

			Batch("test-parallel-transform-batch-batch", pipe, func(_ context.Context, batch []int) (*[]int, error) {
				return &batch, nil
			}, 5, transformed, out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 30; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var batches [][]int
	for batch := range p.Outputs().At(0) {
		batches = append(batches, batch)
	}

	// Should have 6 batches (30 items / 5 per batch)
	assert.Len(t, batches, 6, "expected 6 batches")

	// Each batch should have 5 items
	for i, batch := range batches {
		assert.Lenf(t, batch, 5, "batch %d: expected 5 items", i)

		// All values should be multiples of 3
		for _, v := range batch {
			assert.Equalf(t, 0, v%3, "batch %d: expected all values to be multiples of 3, got %d", i, v)
		}
	}
}

func TestPipeline_FanInTransformFanOut(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    3,
		InputBufferSize:  5,
		OutputChannels:   2,
		OutputBufferSize: 15,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			merged := make(chan int, 15)
			transformed := make(chan int, 15)

			FanIn("test-fan-in-transform-fan-out-fan-in", pipe, merged, in.At(0), in.At(1), in.At(2))

			Transform("test-fan-in-transform-fan-out-transform", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 2
				return &result, nil
			}, merged, transformed)

			FanOut("test-fan-in-transform-fan-out-fan-out", pipe, transformed, out.At(0), out.At(1))
		},
	}

	ctx := context.Background()

	p, _ := NewPipeline(ctx, cfg)

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5)
	p.Inputs().Send(ctx, 1, 6, 7, 8, 9, 10)
	p.Inputs().Send(ctx, 2, 11, 12, 13, 14, 15)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

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

		assert.Lenf(t, results, count, "%s: unexpected number of results", name)

		// All values should be even and in range [2, 30]
		for _, v := range results {
			assert.Equalf(t, 0, v%2, "%s: expected all values to be even, got %d", name, v)
			assert.GreaterOrEqualf(t, v, 2, "%s: expected values >= 2", name)
			assert.LessOrEqualf(t, v, 30, "%s: expected values <= 30", name)
		}
	}

	checkOutput(p.Outputs().At(0), "out1", 15)
	checkOutput(p.Outputs().At(1), "out2", 15)
}

func TestPipeline_SplitTransformFanIn(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  12,
		OutputBufferSize: 18,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			out1 := make(chan int, 6)
			out2 := make(chan int, 6)
			out3 := make(chan int, 6)
			transformed1 := make(chan int, 6)
			transformed2 := make(chan int, 6)
			transformed3 := make(chan int, 6)

			Split("test-split-transform-fan-in-split", pipe, func(_ context.Context, x int) int {
				return (x - 1) % 3
			}, in.At(0), out1, out2, out3)

			Transform("test-split-transform-fan-in-transform1", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 10
				return &result, nil
			}, out1, transformed1)

			Transform("test-split-transform-fan-in-transform2", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 20
				return &result, nil
			}, out2, transformed2)

			Transform("test-split-transform-fan-in-transform3", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 30
				return &result, nil
			}, out3, transformed3)

			FanIn("test-split-transform-fan-in-fan-in", pipe, out.At(0), transformed1, transformed2, transformed3)
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 12; i++ {
		p.Inputs().At(0) <- i
	}

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.Len(t, results, 12, "expected 12 results")

	// Verify routing: values 1,4,7,10 -> *10; 2,5,8,11 -> *20; 3,6,9,12 -> *30
	// Note: Some values may appear multiple times from different branches
	expectedCounts := map[int]int{
		10: 1, 40: 2, 70: 1, 100: 2, // 40 and 100 appear in both out1 and out2
		160: 1, 220: 1, // from out2
		90: 1, 180: 1, 270: 1, 360: 1, // from out3
	}

	actualCounts := make(map[int]int)
	for _, v := range results {
		actualCounts[v]++
	}

	for val, expectedCount := range expectedCounts {
		assert.Equalf(t, expectedCount, actualCounts[val], "value %d: unexpected count", val)
	}
}

func TestPipeline_ComplexMultiStage(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  50,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			// Complex pipeline: Filter -> Transform -> Batch -> Transform -> Filter -> Limit
			stage2 := make(chan int, 50)
			stage3 := make(chan int, 50)
			stage4 := make(chan []int, 20)
			stage5 := make(chan int, 20)
			stage6 := make(chan int, 20)

			// Stage 1: Filter evens
			Filter("test-complex-filter1", pipe, func(_ context.Context, x int) (bool, error) {
				return x%2 == 0, nil
			}, in.At(0), stage2)

			// Stage 2: Transform (multiply by 3)
			Transform("test-complex-transform1", pipe, func(_ context.Context, x int) (*int, error) {
				result := x * 3
				return &result, nil
			}, stage2, stage3)

			// Stage 3: Batch into groups of 4
			Batch("test-complex-batch", pipe, func(_ context.Context, batch []int) (*[]int, error) {
				return &batch, nil
			}, 4, stage3, stage4)

			// Stage 4: Transform batches (sum)
			Transform("test-complex-transform2", pipe, func(_ context.Context, batch []int) (*int, error) {
				sum := 0
				for _, v := range batch {
					sum += v
				}

				return &sum, nil
			}, stage4, stage5)

			// Stage 5: Filter sums > 100
			Filter("test-complex-filter2", pipe, func(_ context.Context, x int) (bool, error) {
				return x > 100, nil
			}, stage5, stage6)

			// Stage 6: Limit to 5 results
			Limit("test-complex-limit", pipe, 5, stage6, out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 50; i++ {
		p.Inputs().At(0) <- i
	}
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 100, "expected all results > 100")
	}
}

func TestPipeline_RoundRobinParallelProcessing(t *testing.T) {
	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  15,
		OutputBufferSize: 15,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			// Pipeline: FanOutRoundRobin -> ParallelTransform (on each branch) -> FanIn
			out1 := make(chan int, 5)
			out2 := make(chan int, 5)
			out3 := make(chan int, 5)
			processed1 := make(chan int, 5)
			processed2 := make(chan int, 5)
			processed3 := make(chan int, 5)

			FanOutRoundRobin("test-round-robin-fan-out", pipe, in.At(0), out1, out2, out3)

			ParallelTransform("test-round-robin-transform1", pipe, 2, func(_ context.Context, x int) (*int, error) {
				time.Sleep(5 * time.Millisecond)
				result := x * 100
				return &result, nil
			}, out1, processed1)

			ParallelTransform("test-round-robin-transform2", pipe, 2, func(_ context.Context, x int) (*int, error) {
				time.Sleep(5 * time.Millisecond)
				result := x * 200
				return &result, nil
			}, out2, processed2)

			ParallelTransform("test-round-robin-transform3", pipe, 2, func(_ context.Context, x int) (*int, error) {
				time.Sleep(5 * time.Millisecond)
				result := x * 300
				return &result, nil
			}, out3, processed3)

			FanIn("test-round-robin-fan-in", pipe, out.At(0), processed1, processed2, processed3)
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	for i := 1; i <= 15; i++ {
		p.Inputs().At(0) <- i
	}
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range p.Outputs().At(0) {
		results = append(results, v)
	}

	assert.Len(t, results, 15, "expected 15 results")

	// Verify routing: 1,4,7,10,13 -> *100; 2,5,8,11,14 -> *200; 3,6,9,12,15 -> *300
	// Note: Some values may appear multiple times from different branches
	expectedCounts := map[int]int{
		100: 1, 400: 2, 700: 1, 1000: 2, 1300: 1, // 400 and 1000 appear in both out1 and out2
		1600: 1, 2200: 1, 2800: 1, // from out2
		900: 1, 1800: 1, 2700: 1, 3600: 1, 4500: 1, // from out3
	}

	actualCounts := make(map[int]int)
	for _, v := range results {
		actualCounts[v]++
	}

	for val, expectedCount := range expectedCounts {
		assert.Equalf(t, expectedCount, actualCounts[val], "value %d: unexpected count", val)
	}
}

func TestPipeline_WithError(t *testing.T) {
	expectedError := errors.New("test error")

	cfg := PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		Executor: func(params *PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			Transform("test-with-error-transform", pipe, func(_ context.Context, x int) (*int, error) {
				time.Sleep(10 * time.Millisecond)

				if x == 5 {
					return nil, expectedError
				}

				result := x * 2
				return &result, nil
			}, in.At(0), out.At(0))
		},
	}

	p, _ := NewPipeline(context.Background(), cfg)

	go func() {
		for i := 1; i <= 100; i++ {
			// Sending might block/fail if pipeline is cancelled, so we ignore errors here
			// or use select with default/ctx check if we had access to ctx inside loop easily.
			// But for test, it's fine if it blocks or panics (it won't panic on send to closed,
			// but we need to be careful).
			// p.Inputs().At(0) <- i
			// Actually, if we send to channel and pipeline cancels, channel isn't closed immediately by pipeline cancellation,
			// but context is cancelled.
			// However, p.CloseAllInputs() closes inputs.
			// Let's rely on standard send.
			select {
			case p.Inputs().At(0) <- i:
			case <-time.After(1 * time.Second): // timeout if blocked too long
				return
			}
		}
		p.CloseAllInputs()
	}()

	p.Start()

	err := p.Wait()
	require.ErrorIs(t, err, expectedError)

	var results []int
	for {
		select {
		case v, ok := <-p.Outputs().At(0):
			if !ok {
				goto done
			}
			results = append(results, v)
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:

	assert.NotEmpty(t, results, "expected some results before cancellation")
}
