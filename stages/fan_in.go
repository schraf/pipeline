package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
	"golang.org/x/sync/errgroup"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║FanIn merges multiple input channels into a single output channel,         ║
// ║forwarding all values from each input until the context is done or all     ║
// ║inputs are closed.                                                         ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type FanInStage[T any] struct {
	Name   string
	Buffer uint
}

func (s FanInStage[T]) Create(ctx pipeline.Context, in pipeline.MultiChannelReceiver[T]) <-chan T {
	out := make(chan T, s.Buffer)

	ctx.Go(s.Name, func(pctx context.Context) error {
		defer close(out)

		group, gctx := errgroup.WithContext(pctx)

		for inputChannel := range in.Iter() {
			capturedInputChannel := inputChannel

			group.Go(func() error {
				for input := range capturedInputChannel {
					select {
					case <-gctx.Done():
						return gctx.Err()
					case out <- input:
					}
				}

				return nil
			})
		}

		return group.Wait()
	})

	return out
}
