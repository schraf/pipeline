package stages_test

import (
	"context"
	"testing"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
	"github.com/stretchr/testify/assert"
)

func TestWindowedReduceStage(t *testing.T) {
	type State struct {
		Sum   int
		Count int
	}

	t.Run("sum every 2 items", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		expected := []State{
			{Sum: 3, Count: 2}, // 1+2
			{Sum: 7, Count: 2}, // 3+4
			{Sum: 5, Count: 1}, // 5 (remaining)
		}

		results := runStageTest(t, inputs,
			func(composer pipeline.Composer[int, State]) error {
				ctx := composer.Context()
				inputs := composer.Inputs()
				outputs := composer.Outputs()

				return outputs.Link(ctx, 0, stages.WindowedReduceStage[int, State]{
					Name:    "test-windowed-reduce",
					Buffer:  1,
					Initial: State{},
					Reducer: func(_ context.Context, acc State, val int) (State, State, bool, error) {
						acc.Sum += val
						acc.Count++

						if acc.Count == 2 {
							// Window full:
							// 1. Prepare output (current state)
							output := acc
							// 2. Prepare next state (reset)
							nextAcc := State{}
							// 3. Signal emit
							return nextAcc, output, true, nil
						}

						// Window not full:
						// 1. Next state is current state
						// 2. Output is ignored (zero value)
						// 3. Signal no emit
						return acc, State{}, false, nil
					},
				}.Create(ctx, inputs.At(0)))
			},
		)

		assert.Equal(t, expected, results)
	})

	t.Run("emit on close with empty state", func(t *testing.T) {
		// Test that it emits the final state even if empty, consistent with ReduceStage
		inputs := []int{1, 2}
		// 1+2 -> emits {3, 2}, resets to {0, 0}.
		// Channel closes.
		// Should emit {0, 0} as the final artifact.
		expected := []State{
			{Sum: 3, Count: 2},
			{Sum: 0, Count: 0},
		}

		results := runStageTest(t, inputs,
			func(composer pipeline.Composer[int, State]) error {
				ctx := composer.Context()
				inputs := composer.Inputs()
				outputs := composer.Outputs()

				return outputs.Link(ctx, 0, stages.WindowedReduceStage[int, State]{
					Name:    "test-windowed-reduce-exact",
					Buffer:  1,
					Initial: State{},
					Reducer: func(_ context.Context, acc State, val int) (State, State, bool, error) {
						acc.Sum += val
						acc.Count++

						if acc.Count == 2 {
							return State{}, acc, true, nil
						}
						return acc, State{}, false, nil
					},
				}.Create(ctx, inputs.At(0)))
			},
		)

		assert.Equal(t, expected, results)
	})
}
