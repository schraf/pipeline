package stages

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync"

	"github.com/schraf/pipeline/v4"
	"golang.org/x/sync/errgroup"
)

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║ParallelTransform applies the transformer function to values read from the      ║
// ║input channel using a fixed number of concurrent workers, forwarding successful ║
// ║results to the output channel until the context is done or the input channel is ║
// ║closed. The transformer must return a non-nil pointer when err is nil, otherwise║
// ║an error will be returned.                                                      ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

type ParallelTransformStage[In any, Out any] struct {
	Name        string
	Buffer      uint
	Workers     uint
	Transformer Transformer[In, Out]
}

func (s ParallelTransformStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	if s.Transformer == nil {
		panic("ParallelTransformStage: Transformer must not be nil")
	}

	out := make(chan Out, s.Buffer)

	ctx.Go(s.Name, func(pctx context.Context) error {
		defer close(out)

		group, gctx := errgroup.WithContext(pctx)

		// draining is closed when any worker receives a DrainError,
		// signaling all workers to stop processing and drain the input.
		draining := make(chan struct{})
		drainOnce := sync.Once{}

		for i := 0; i < int(s.Workers); i++ {
			workerIndex := i
			group.Go(func() error {
				defer trace.StartRegion(pctx, fmt.Sprintf("%s_%d", s.Name, workerIndex)).End()

				for {
					var input In
					var ok bool

					select {
					case <-gctx.Done():
						return gctx.Err()
					case <-draining:
						pipeline.DrainChannel(in)
						return nil
					case input, ok = <-in:
						if !ok {
							return nil
						}
					}

					output, err := s.Transformer(gctx, input)
					if err != nil {
						if pipeline.IsDrainError(err) {
							drainOnce.Do(func() { close(draining) })
							pipeline.DrainChannel(in)
							return nil
						}

						if pipeline.IsSkipError(err) {
							continue
						}

						return pipeline.ErrorInStage(s.Name, err)
					}

					select {
					case <-gctx.Done():
						return gctx.Err()
					case <-draining:
						pipeline.DrainChannel(in)
						return nil
					case out <- output:
					}
				}
			})
		}

		return group.Wait()
	})

	return out
}
