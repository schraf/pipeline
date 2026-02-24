package stages_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║ TransformStage                                                                ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

func TestTransformStage_SkipError(t *testing.T) {
	// Skip odd numbers, transform even numbers to x*2
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.TransformStage[int, int]{
				Name:   "test-transform-skip",
				Buffer: 10,
				Transformer: func(_ context.Context, x int) (int, error) {
					if x%2 != 0 {
						return 0, pipeline.Skip(errTest)
					}
					return x * 2, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	expected := []int{4, 8, 12}
	assert.Equal(t, expected, results)
}

func TestTransformStage_DrainError(t *testing.T) {
	// Process items until we hit 4, then drain
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.TransformStage[int, int]{
				Name:   "test-transform-drain",
				Buffer: 10,
				Transformer: func(_ context.Context, x int) (int, error) {
					if x == 4 {
						return 0, pipeline.Drain(errTest)
					}
					return x * 2, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	// Should only have results for 1, 2, 3
	expected := []int{2, 4, 6}
	assert.Equal(t, expected, results)
}

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║ ParallelTransformStage                                                        ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

func TestParallelTransformStage_SkipError(t *testing.T) {
	// Skip odd numbers, transform even numbers to x*2
	results := runStageTest(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform-skip",
				Buffer:  10,
				Workers: 3,
				Transformer: func(_ context.Context, x int) (int, error) {
					if x%2 != 0 {
						return 0, pipeline.Skip(errTest)
					}
					return x * 2, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	// Results may be out of order due to parallel processing
	expected := []int{4, 8, 12, 16, 20}
	assertUnorderedEqual(t, expected, results)
}

func TestParallelTransformStage_DrainError(t *testing.T) {
	// Use 1 worker to make behavior deterministic for testing.
	// When item 5 is hit, drain should stop all processing.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	p, _, err := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputBufferSize:  20,
		OutputBufferSize: 20,
		Composer: func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform-drain",
				Buffer:  20,
				Workers: 1,
				Transformer: func(_ context.Context, x int) (int, error) {
					if x == 5 {
						return 0, pipeline.Drain(errTest)
					}
					return x * 2, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	})
	require.NoError(t, err)

	require.NoError(t, p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	p.CloseAllInputs()

	results := p.Outputs().SinkAt(ctx, 0)

	require.NoError(t, p.Wait())

	// With 1 worker processing sequentially, items 1-4 should be processed
	// before item 5 triggers the drain.
	assert.Equal(t, []int{2, 4, 6, 8}, results)
}

func TestParallelTransformStage_DrainError_MultipleWorkers(t *testing.T) {
	// With multiple workers, we verify the pipeline completes without
	// deadlock and no error is returned, even though ordering is
	// non-deterministic.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	p, _, err := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputBufferSize:  20,
		OutputBufferSize: 20,
		Composer: func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ParallelTransformStage[int, int]{
				Name:    "test-parallel-transform-drain-multi",
				Buffer:  20,
				Workers: 3,
				Transformer: func(_ context.Context, x int) (int, error) {
					time.Sleep(5 * time.Millisecond)
					if x == 5 {
						return 0, pipeline.Drain(errTest)
					}
					return x * 2, nil
				},
			}.Create(ctx, inputs.At(0)))
		},
	})
	require.NoError(t, err)

	require.NoError(t, p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	p.CloseAllInputs()

	results := p.Outputs().SinkAt(ctx, 0)

	// Pipeline should complete without error (drain is non-fatal)
	require.NoError(t, p.Wait())

	// We should have some results (at least the items processed before
	// the drain kicked in), and no items after the drain should appear.
	// The exact count is non-deterministic with multiple workers.
	assert.NotEmpty(t, results)
	for _, v := range results {
		assert.NotEqual(t, 10, v, "item 5 should not produce a result (it was 5*2=10)")
	}
}

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║ ExpandStage                                                                   ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

func TestExpandStage_SkipError(t *testing.T) {
	// For each input, expand to x, x*2, x*3.
	// But if the input is 2, yield a SkipError mid-expansion.
	results := runStageTest(t, []int{1, 2, 3},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ExpandStage[int, int]{
				Name:   "test-expand-skip",
				Buffer: 20,
				Expander: func(_ context.Context, x int) iter.Seq2[int, error] {
					return func(yield func(int, error) bool) {
						if x == 2 {
							// Yield first item, then skip
							yield(x, nil)
							yield(0, pipeline.Skip(errTest))
							return
						}
						yield(x, nil)
						yield(x*2, nil)
						yield(x*3, nil)
					}
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	// Input 1 → 1, 2, 3
	// Input 2 → 2 (then skip, so x*2 and x*3 are not yielded)
	// Input 3 → 3, 6, 9
	expected := []int{1, 2, 3, 2, 3, 6, 9}
	assert.Equal(t, expected, results)
}

func TestExpandStage_DrainError(t *testing.T) {
	// For each input, expand to x, x*2, x*3.
	// But if the input is 2, yield a DrainError — this should drain
	// remaining input (input 3) without processing it.
	results := runStageTest(t, []int{1, 2, 3},
		func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			return outputs.Link(ctx, 0, stages.ExpandStage[int, int]{
				Name:   "test-expand-drain",
				Buffer: 20,
				Expander: func(_ context.Context, x int) iter.Seq2[int, error] {
					return func(yield func(int, error) bool) {
						if x == 2 {
							// Yield first item, then drain
							yield(x, nil)
							yield(0, pipeline.Drain(errTest))
							return
						}
						yield(x, nil)
						yield(x*2, nil)
						yield(x*3, nil)
					}
				},
			}.Create(ctx, inputs.At(0)))
		},
	)

	// Input 1 → 1, 2, 3
	// Input 2 → 2 (then drain, so no more expansion and input 3 is drained)
	expected := []int{1, 2, 3, 2}
	assert.Equal(t, expected, results)
}

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║ Integration: Skip/Drain don't kill sibling pipelines                          ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

func TestDrainError_DoesNotAffectSiblingPipelines(t *testing.T) {
	// Simulates the attribution pipeline pattern: split into sub-pipelines,
	// one sub-pipeline hits a drain error, the other should complete normally.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	splitStage := stages.SplitStage[int]{
		Name:        "split",
		OutputCount: 2,
		Buffer:      10,
		Selector: func(_ context.Context, x int) int {
			if x%2 == 0 {
				return 0
			}
			return 1
		},
	}

	// Even numbers: drain when we see 6
	transformEven := stages.TransformStage[int, int]{
		Name:   "transform-even",
		Buffer: 10,
		Transformer: func(_ context.Context, x int) (int, error) {
			if x == 6 {
				return 0, pipeline.Drain(errTest)
			}
			return x * 10, nil
		},
	}

	// Odd numbers: process all normally
	transformOdd := stages.TransformStage[int, int]{
		Name:   "transform-odd",
		Buffer: 10,
		Transformer: func(_ context.Context, x int) (int, error) {
			return x * 100, nil
		},
	}

	fanIn := stages.FanInStage[int]{
		Name:   "fan-in",
		Buffer: 20,
	}

	p, _, err := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
		Name:             "test",
		InputBufferSize:  20,
		OutputBufferSize: 20,
		Composer: func(composer pipeline.Composer[int, int]) error {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			split := splitStage.Create(ctx, inputs.At(0))
			even := transformEven.Create(ctx, split.At(0))
			odd := transformOdd.Create(ctx, split.At(1))

			merged := fanIn.Create(ctx, pipeline.NewMultiChannelReceiver(even, odd))
			return outputs.Link(ctx, 0, merged)
		},
	})
	require.NoError(t, err)

	// Evens: 2, 4, 6, 8, 10 — drain at 6, so only 2, 4 processed → 20, 40
	// Odds: 1, 3, 5, 7, 9 — all processed → 100, 300, 500, 700, 900
	require.NoError(t, p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	p.CloseAllInputs()

	results := p.Outputs().SinkAt(ctx, 0)

	require.NoError(t, p.Wait())

	// The odd pipeline should have all its results
	oddResults := []int{}
	evenResults := []int{}
	for _, v := range results {
		if v >= 100 {
			oddResults = append(oddResults, v)
		} else {
			evenResults = append(evenResults, v)
		}
	}

	assertUnorderedEqual(t, []int{100, 300, 500, 700, 900}, oddResults)

	// The even pipeline should have results only for items before the drain.
	// The split stage sends evens and odds to different channels. Due to
	// buffering, items 2 and 4 should be processed before 6 triggers drain.
	assertUnorderedEqual(t, []int{20, 40}, evenResults)
}
