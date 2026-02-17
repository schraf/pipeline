package stages

import (
	"context"
	"errors"

	"github.com/schraf/pipeline/v3"
)

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║Batch groups incoming values into fixed-size batches, passes each batch to      ║
// ║the batcher function, and forwards the resulting value to the output channel.   ║
// ║Any remaining items after the input channel closes are processed as a final     ║
// ║batch. The batcher must return a non-nil pointer when err is nil, otherwise an  ║
// ║error will be returned.                                                         ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

type Batcher[In any, Out any] func(context.Context, []In) (*Out, error)

type BatchStage[In any, Out any] struct {
	Name      string
	Buffer    int
	BatchSize int
	Batcher   Batcher[In, Out]
}

func (s BatchStage[In, Out]) Create(ctx pipeline.Context, in <-chan In) <-chan Out {
	out := make(chan Out, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		batch := make([]In, 0, s.BatchSize)

		for input := range in {
			batch = append(batch, input)

			if len(batch) >= s.BatchSize {
				localBatch := append([]In(nil), batch...)

				output, err := s.Batcher(ctx, localBatch)
				if err != nil {
					return err
				}
				if output == nil {
					return errors.New("batcher returned nil output without error")
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- *output:
				}

				batch = batch[:0]
			}
		}

		// Process remaining items if any
		if len(batch) > 0 {
			batchCopy := append([]In(nil), batch...)

			output, err := s.Batcher(ctx, batchCopy)
			if err != nil {
				return err
			}
			if output == nil {
				return errors.New("batcher returned nil output without error")
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- *output:
			}
		}

		return nil
	})

	return out
}
