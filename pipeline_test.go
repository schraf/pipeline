package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransform(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 5)
	out := make(chan int, 5)

	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 2
		return &result, nil
	}, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
		results = append(results, v)
	}

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestFilter(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 10)
	out := make(chan int, 10)

	for i := 1; i <= 10; i++ {
		in <- i
	}
	close(in)

	Filter(p, func(_ context.Context, x int) (bool, error) {
		return x%2 == 0, nil
	}, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
		results = append(results, v)
	}

	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, results)
}

func TestBatch(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 10)
	out := make(chan int, 10)

	for i := 1; i <= 7; i++ {
		in <- i
	}
	close(in)

	Batch(p, func(_ context.Context, batch []int) (*int, error) {
		sum := 0
		for _, v := range batch {
			sum += v
		}
		return &sum, nil
	}, 3, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
		results = append(results, v)
	}

	// First batch: 1+2+3=6, second batch: 4+5+6=15, remaining: 7
	expected := []int{6, 15, 7}
	assert.Equal(t, expected, results)
}

func TestFanIn(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in1 := make(chan int, 3)
	in2 := make(chan int, 3)
	in3 := make(chan int, 3)
	out := make(chan int, 10)

	in1 <- 1
	in1 <- 2
	in1 <- 3
	close(in1)

	in2 <- 4
	in2 <- 5
	in2 <- 6
	close(in2)

	in3 <- 7
	in3 <- 8
	in3 <- 9
	close(in3)

	FanIn(p, out, in1, in2, in3)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
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
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 3)
	out1 := make(chan int, 3)
	out2 := make(chan int, 3)
	out3 := make(chan int, 3)

	in <- 1
	in <- 2
	in <- 3
	close(in)

	FanOut(p, in, out1, out2, out3)

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

	checkOutput(out1, "out1", 3)
	checkOutput(out2, "out2", 3)
	checkOutput(out3, "out3", 3)
}

func TestFanOutRoundRobin(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 6)
	out1 := make(chan int, 3)
	out2 := make(chan int, 3)
	out3 := make(chan int, 3)

	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	FanOutRoundRobin(p, in, out1, out2, out3)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results1, results2, results3 []int
	for i := 0; i < 2; i++ {
		select {
		case v := <-out1:
			results1 = append(results1, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out1")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-out2:
			results2 = append(results2, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out2")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-out3:
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
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 10)
	out := make(chan int, 10)

	for i := 1; i <= 10; i++ {
		in <- i
	}
	close(in)

	ParallelTransform(p, 3, func(_ context.Context, x int) (*int, error) {
		time.Sleep(10 * time.Millisecond)
		result := x * 2
		return &result, nil
	}, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
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
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 10)
	out := make(chan int, 5)

	for i := 1; i <= 10; i++ {
		in <- i
	}
	close(in)

	Limit(p, 5, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, results)
}

func TestSplit(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 6)
	out1 := make(chan int, 3)
	out2 := make(chan int, 3)
	out3 := make(chan int, 3)

	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	Split(p, func(_ context.Context, x int) int {
		return (x - 1) % 3
	}, in, out1, out2, out3)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results1, results2, results3 []int
	for i := 0; i < 2; i++ {
		select {
		case v := <-out1:
			results1 = append(results1, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out1")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-out2:
			results2 = append(results2, v)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timeout waiting for out2")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-out3:
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
	p, _ := WithPipeline(context.Background())

	in := make(chan int, 5)
	out := make(chan []int, 1)

	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	Aggregate(p, in, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	result := <-out
	expected := []int{1, 2, 3, 4, 5}
	assert.Equal(t, expected, result)
}

// End-to-end tests

func TestPipeline_TransformFilterLimit(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	// Pipeline: Transform -> Filter -> Limit
	in := make(chan int, 20)
	transformed := make(chan int, 20)
	filtered := make(chan int, 20)
	out := make(chan int, 10)

	go func() {
		for i := 1; i <= 20; i++ {
			in <- i
		}
		close(in)
	}()

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 2
		return &result, nil
	}, in, transformed)

	Filter(p, func(_ context.Context, x int) (bool, error) {
		return x > 20, nil
	}, transformed, filtered)

	Limit(p, 5, filtered, out)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range out {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 20, "expected all results > 20")
		assert.Equalf(t, 0, v%2, "expected all results to be even, got %d", v)
	}
}

func TestPipeline_ParallelTransformBatch(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	// Pipeline: ParallelTransform -> Batch
	in := make(chan int, 30)
	transformed := make(chan int, 30)
	batched := make(chan []int, 10)

	go func() {
		for i := 1; i <= 30; i++ {
			in <- i
		}
		close(in)
	}()

	ParallelTransform(p, 5, func(_ context.Context, x int) (*int, error) {
		time.Sleep(5 * time.Millisecond)
		result := x * 3
		return &result, nil
	}, in, transformed)

	Batch(p, func(_ context.Context, batch []int) (*[]int, error) {
		return &batch, nil
	}, 5, transformed, batched)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var batches [][]int
	for batch := range batched {
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
	p, _ := WithPipeline(context.Background())

	// Pipeline: FanIn -> Transform -> FanOut
	in1 := make(chan int, 5)
	in2 := make(chan int, 5)
	in3 := make(chan int, 5)
	merged := make(chan int, 15)
	transformed := make(chan int, 15)
	out1 := make(chan int, 15)
	out2 := make(chan int, 15)

	go func() {
		for i := 1; i <= 5; i++ {
			in1 <- i
		}
		close(in1)
	}()

	go func() {
		for i := 6; i <= 10; i++ {
			in2 <- i
		}
		close(in2)
	}()

	go func() {
		for i := 11; i <= 15; i++ {
			in3 <- i
		}
		close(in3)
	}()

	FanIn(p, merged, in1, in2, in3)

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 2
		return &result, nil
	}, merged, transformed)

	FanOut(p, transformed, out1, out2)

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

	checkOutput(out1, "out1", 15)
	checkOutput(out2, "out2", 15)
}

func TestPipeline_SplitTransformFanIn(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	// Pipeline: Split -> Transform (on each branch) -> FanIn
	in := make(chan int, 12)
	out1 := make(chan int, 6)
	out2 := make(chan int, 6)
	out3 := make(chan int, 6)
	transformed1 := make(chan int, 6)
	transformed2 := make(chan int, 6)
	transformed3 := make(chan int, 6)
	merged := make(chan int, 18)

	go func() {
		for i := 1; i <= 12; i++ {
			in <- i
		}
		close(in)
	}()

	Split(p, func(_ context.Context, x int) int {
		return (x - 1) % 3
	}, in, out1, out2, out3)

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 10
		return &result, nil
	}, out1, transformed1)

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 20
		return &result, nil
	}, out2, transformed2)

	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 30
		return &result, nil
	}, out3, transformed3)

	FanIn(p, merged, transformed1, transformed2, transformed3)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range merged {
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
	p, _ := WithPipeline(context.Background())

	// Complex pipeline: Filter -> Transform -> Batch -> Transform -> Filter -> Limit
	stage1 := make(chan int, 50)
	stage2 := make(chan int, 50)
	stage3 := make(chan int, 50)
	stage4 := make(chan []int, 20)
	stage5 := make(chan int, 20)
	stage6 := make(chan int, 20)
	stage7 := make(chan int, 10)

	go func() {
		for i := 1; i <= 50; i++ {
			stage1 <- i
		}
		close(stage1)
	}()

	// Stage 1: Filter evens
	Filter(p, func(_ context.Context, x int) (bool, error) {
		return x%2 == 0, nil
	}, stage1, stage2)

	// Stage 2: Transform (multiply by 3)
	Transform(p, func(_ context.Context, x int) (*int, error) {
		result := x * 3
		return &result, nil
	}, stage2, stage3)

	// Stage 3: Batch into groups of 4
	Batch(p, func(_ context.Context, batch []int) (*[]int, error) {
		return &batch, nil
	}, 4, stage3, stage4)

	// Stage 4: Transform batches (sum)
	Transform(p, func(_ context.Context, batch []int) (*int, error) {
		sum := 0
		for _, v := range batch {
			sum += v
		}
		return &sum, nil
	}, stage4, stage5)

	// Stage 5: Filter sums > 100
	Filter(p, func(_ context.Context, x int) (bool, error) {
		return x > 100, nil
	}, stage5, stage6)

	// Stage 6: Limit to 5 results
	Limit(p, 5, stage6, stage7)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range stage7 {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 100, "expected all results > 100")
	}
}

func TestPipeline_RoundRobinParallelProcessing(t *testing.T) {
	p, _ := WithPipeline(context.Background())

	// Pipeline: FanOutRoundRobin -> ParallelTransform (on each branch) -> FanIn
	in := make(chan int, 15)
	out1 := make(chan int, 5)
	out2 := make(chan int, 5)
	out3 := make(chan int, 5)
	processed1 := make(chan int, 5)
	processed2 := make(chan int, 5)
	processed3 := make(chan int, 5)
	merged := make(chan int, 15)

	go func() {
		for i := 1; i <= 15; i++ {
			in <- i
		}
		close(in)
	}()

	FanOutRoundRobin(p, in, out1, out2, out3)

	ParallelTransform(p, 2, func(_ context.Context, x int) (*int, error) {
		time.Sleep(5 * time.Millisecond)
		result := x * 100
		return &result, nil
	}, out1, processed1)

	ParallelTransform(p, 2, func(_ context.Context, x int) (*int, error) {
		time.Sleep(5 * time.Millisecond)
		result := x * 200
		return &result, nil
	}, out2, processed2)

	ParallelTransform(p, 2, func(_ context.Context, x int) (*int, error) {
		time.Sleep(5 * time.Millisecond)
		result := x * 300
		return &result, nil
	}, out3, processed3)

	FanIn(p, merged, processed1, processed2, processed3)

	require.NoError(t, p.Wait(), "unexpected error from pipeline wait")

	var results []int
	for v := range merged {
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
	p, _ := WithPipeline(context.Background())
	expectedError := errors.New("test error")

	in := make(chan int, 10)
	out := make(chan int, 10)

	go func() {
		for i := 1; i <= 100; i++ {
			in <- i
		}
		close(in)
	}()

	Transform(p, func(_ context.Context, x int) (*int, error) {
		time.Sleep(10 * time.Millisecond)

		if x == 5 {
			return nil, expectedError
		}

		result := x * 2
		return &result, nil
	}, in, out)

	err := p.Wait()
	require.ErrorIs(t, expectedError, err)

	var results []int
	for {
		select {
		case v, ok := <-out:
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
