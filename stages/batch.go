package stages

import (
	"context"

	"github.com/schraf/pipeline/v3"
)

// ╔════════════════════════════════════════════════════════════════════════════════╗
// ║Batch groups incoming values into fixed-size batches, and forwards the          ║
// ║resulting value to the output channel. Any remaining items after the input      ║
// ║channel closes are processed as a final batch.                                  ║
// ╚════════════════════════════════════════════════════════════════════════════════╝

type BatchStage[T any] struct {
	Name      string
	Buffer    int
	BatchSize int
}

func (s BatchStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan []T {
	out := make(chan []T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		batch := make([]T, 0, s.BatchSize)

		for input := range in {
			batch = append(batch, input)

			if len(batch) >= s.BatchSize {
				output := append([]T(nil), batch...)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- output:
				}

				batch = batch[:0]
			}
		}

		// Process remaining items if any
		if len(batch) > 0 {
			output := append([]T(nil), batch...)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- output:
			}
		}

		return nil
	})

	return out
}
