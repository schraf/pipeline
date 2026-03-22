package stages

import (
	"context"
	"time"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║Timelimiter passes values through until a set duration has been reached.   ║
// ║Once the time limit has passed, the channel will close.                    ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type TimelimiterStage[T any] struct {
	Name     string
	Buffer   uint
	Duration time.Duration
}

func (s TimelimiterStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan T {
	out := make(chan T, s.Buffer)
	pipeline.RegisterChannel(ctx.Telemetry(), s.Name, out)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)
		defer pipeline.DrainChannel(in)

		timer := time.NewTimer(s.Duration)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			case input, ok := <-in:
				if !ok {
					return nil
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- input:
				}
			}
		}
	})

	return out
}
