package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime/trace"

	"github.com/schraf/pipeline/v3"
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
	Buffer      int
	Workers     int
	Transformer Transformer[In, Out]
}

func (s ParallelTransformStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	out := make(chan Out, s.Buffer)

	ctx.Go(s.Name, func(pctx context.Context) error {
		defer close(out)

		group, gctx := errgroup.WithContext(pctx)

		for i := 0; i < s.Workers; i++ {
			workerIndex := i
			group.Go(func() error {
				defer trace.StartRegion(pctx, fmt.Sprintf("%s_%d", s.Name, workerIndex)).End()

				for {
					var input In
					var ok bool

					select {
					case <-gctx.Done():
						return gctx.Err()
					case input, ok = <-in:
						if !ok {
							return nil
						}
					}

					output, err := s.Transformer(gctx, input)
					if err != nil {
						return err
					}
					if output == nil {
						return errors.New("transformer returned nil output without error")
					}

					select {
					case <-gctx.Done():
						return gctx.Err()
					case out <- *output:
					}
				}
			})
		}

		return group.Wait()
	})

	return out
}
