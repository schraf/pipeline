package stages_test

import (
	"context"
	"testing"
	"time"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimelimiterStage(t *testing.T) {
	t.Run("cuts off after duration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, _, err := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
			Name:             "test-timelimit",
			InputBufferSize:  10,
			OutputBufferSize: 10,
			Composer: func(composer pipeline.Composer[int, int]) error {
				cCtx := composer.Context()
				inputs := composer.Inputs()
				outputs := composer.Outputs()

				delayed := stages.TransformStage[int, int]{
					Name: "delay",
					Transformer: func(ctx context.Context, val int) (int, error) {
						time.Sleep(50 * time.Millisecond)
						return val, nil
					},
				}.Create(cCtx, inputs.At(0))

				limited := stages.TimelimiterStage[int]{
					Name:     "timelimit",
					Buffer:   10,
					Duration: 120 * time.Millisecond,
				}.Create(cCtx, delayed)

				return outputs.Link(cCtx, 0, limited)
			},
		})
		require.NoError(t, err)

		inputs := []int{1, 2, 3, 4, 5}
		require.NoError(t, p.Inputs().Send(ctx, 0, inputs...))
		p.CloseAllInputs()

		results := p.Outputs().SinkAt(ctx, 0)
		require.NoError(t, p.Wait())

		// We expect only the first 2 values to make it through before 120ms
		assert.Equal(t, []int{1, 2}, results)
	})

	t.Run("processes all if fast enough", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, _, err := pipeline.NewPipeline[int, int](ctx, pipeline.Config[int, int]{
			Name:             "test-timelimit-fast",
			InputBufferSize:  10,
			OutputBufferSize: 10,
			Composer: func(composer pipeline.Composer[int, int]) error {
				cCtx := composer.Context()
				inputs := composer.Inputs()
				outputs := composer.Outputs()

				limited := stages.TimelimiterStage[int]{
					Name:     "timelimit",
					Buffer:   10,
					Duration: 500 * time.Millisecond,
				}.Create(cCtx, inputs.At(0))

				return outputs.Link(cCtx, 0, limited)
			},
		})
		require.NoError(t, err)

		inputs := []int{1, 2, 3, 4, 5}
		require.NoError(t, p.Inputs().Send(ctx, 0, inputs...))
		p.CloseAllInputs()

		results := p.Outputs().SinkAt(ctx, 0)
		require.NoError(t, p.Wait())

		assert.Equal(t, []int{1, 2, 3, 4, 5}, results)
	})
}
