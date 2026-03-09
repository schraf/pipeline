package stages

import (
	"context"

	"github.com/schraf/pipeline/v4"
)

// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║UniqueValues passes values through that it has not seen before.            ║
// ║It uses a map to keep track of values and ensure only unique values pass.  ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

type UniqueValuesStage[T comparable] struct {
	Name   string
	Buffer uint
}

func (s UniqueValuesStage[T]) Create(ctx pipeline.Context, in <-chan T) <-chan T {
	out := make(chan T, s.Buffer)

	ctx.Go(s.Name, func(ctx context.Context) error {
		defer close(out)

		set := make(map[T]struct{})

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-in:
				if !ok {
					return nil
				}

				if _, exists := set[input]; exists {
					continue
				}

				set[input] = struct{}{}

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
