package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime/trace"

	"golang.org/x/sync/errgroup"
)

// Transform reads values from the input channel, applies the transformer
// function, and forwards successful results to the output channel until the
// context is done or the input channel is closed. The transformer must return
// a non-nil pointer when err is nil, otherwise a panic will occur.
func Transform[In any, Out any](name string, pipe *Pipe, transformer func(context.Context, In) (*Out, error), in <-chan In, out chan<- Out) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for input := range in {
			output, err := transformer(pipe.ctx, input)
			if err != nil {
				pipe.err(err)
				return
			}
			if output == nil {
				pipe.err(errors.New("transformer returned nil output without error"))
				return
			}

			select {
			case <-pipe.ctx.Done():
				return
			case out <- *output:
			}
		}
	}()
}

// Expand reads values from the input channel, applies the expander function to
// each value, and forwards all items from the returned iterator to the output
// channel. For each input item, the expander returns an iterator of output items,
// which are all sent to the output channel. Processing continues until the
// context is done or the input channel is closed. This allows for lazy evaluation
// and avoids loading all expanded items into memory at once.
func Expand[In any, Out any](name string, pipe *Pipe, expander func(context.Context, In) iter.Seq2[Out, error], in <-chan In, out chan<- Out) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for input := range in {
			seq := expander(pipe.ctx, input)

			for output, err := range seq {
				if err != nil {
					pipe.err(err)
					return
				}

				select {
				case <-pipe.ctx.Done():
					return
				case out <- output:
				}
			}
		}
	}()
}

// ExpandSlice reads slices from the input channel and forwards all items from the
// returned iterator to the output channel.
func ExpandSlice[T any](name string, pipe *Pipe, in <-chan []T, out chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for input := range in {
			for _, output := range input {
				select {
				case <-pipe.ctx.Done():
					return
				case out <- output:
				}
			}
		}
	}()
}

// Filter reads values from the input channel, applies the filter predicate,
// and forwards only values that satisfy the predicate to the output channel.
// It respects context cancellation and stops processing on error.
func Filter[T any](name string, pipe *Pipe, filter func(context.Context, T) (bool, error), in <-chan T, out chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for input := range in {
			shouldForward, err := filter(pipe.ctx, input)
			if err != nil {
				pipe.err(err)
				return
			}

			if !shouldForward {
				continue
			}

			select {
			case <-pipe.ctx.Done():
				return
			case out <- input:
			}
		}
	}()
}

// Batch groups incoming values into fixed-size batches, passes each batch to
// the batcher function, and forwards the resulting value to the output
// channel. Any remaining items after the input channel closes are processed
// as a final batch. The batcher must return a non-nil pointer when err is nil,
// otherwise a panic will occur.
func Batch[In any, Out any](name string, pipe *Pipe, batcher func(context.Context, []In) (*Out, error), batchSize int, in <-chan In, out chan<- Out) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		batch := make([]In, 0, batchSize)

		for input := range in {
			batch = append(batch, input)

			if len(batch) >= batchSize {
				localBatch := append([]In(nil), batch...)

				output, err := batcher(pipe.ctx, localBatch)
				if err != nil {
					pipe.err(err)
					return
				}
				if output == nil {
					pipe.err(errors.New("batcher returned nil output without error"))
					return
				}

				select {
				case <-pipe.ctx.Done():
					return
				case out <- *output:
				}

				batch = batch[:0]
			}
		}

		// Process remaining items if any
		if len(batch) > 0 {
			// As above, work on a copy to avoid races with any retained
			// references to previous batches.
			batchCopy := append([]In(nil), batch...)

			output, err := batcher(pipe.ctx, batchCopy)
			if err != nil {
				pipe.err(err)
				return
			}
			if output == nil {
				pipe.err(errors.New("batcher returned nil output without error"))
				return
			}

			select {
			case <-pipe.ctx.Done():
				return
			case out <- *output:
			}
		}
	}()
}

// FanIn merges multiple input channels into a single output channel,
// forwarding all values from each input until the context is done or all
// inputs are closed.
func FanIn[T any](name string, pipe *Pipe, out chan<- T, in ...<-chan T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		group, ctx := errgroup.WithContext(pipe.ctx)

		for _, inputChannel := range in {
			capturedInputChannel := inputChannel

			group.Go(func() error {
				for input := range capturedInputChannel {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- input:
					}
				}

				return nil
			})
		}

		if err := group.Wait(); err != nil {
			pipe.err(err)
		}
	}()
}

// FanOut distributes items from a single input channel to multiple output
// channels, sending each item to all output channels.
func FanOut[T any](name string, pipe *Pipe, in <-chan T, out ...chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		group, ctx := errgroup.WithContext(pipe.ctx)

		for input := range in {
			capturedInput := input

			for _, outputChannel := range out {
				capturedOutputChannel := outputChannel

				group.Go(func() error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case capturedOutputChannel <- capturedInput:
					}

					return nil
				})
			}
		}

		if err := group.Wait(); err != nil {
			pipe.err(err)
		}
	}()
}

// FanOutRoundRobin distributes items from a single input channel to multiple
// output channels using round-robin distribution, sending each item to only
// one output channel. Panics if no output channels are provided.
func FanOutRoundRobin[T any](name string, pipe *Pipe, in <-chan T, out ...chan<- T) {
	if len(out) == 0 {
		panic("FanOutRoundRobin: at least one output channel required")
	}

	pipe.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		index := 0

		for input := range in {
			outputChannel := out[index%len(out)]

			select {
			case <-pipe.ctx.Done():
				return
			case outputChannel <- input:
			}

			index++
		}
	}()
}

// ParallelTransform applies the transformer function to values read from the
// input channel using a fixed number of concurrent workers, forwarding
// successful results to the output channel until the context is done or the
// input channel is closed. The transformer must return a non-nil pointer when
// err is nil, otherwise a panic will occur.
func ParallelTransform[In any, Out any](name string, pipe *Pipe, workers int, transformer func(context.Context, In) (*Out, error), in <-chan In, out chan<- Out) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		group, ctx := errgroup.WithContext(pipe.ctx)

		for i := 0; i < workers; i++ {
			group.Go(func() error {
				defer trace.StartRegion(pipe.ctx, fmt.Sprintf("%s_%d", name, i)).End()

				for {
					var input In
					var ok bool

					select {
					case <-ctx.Done():
						return ctx.Err()
					case input, ok = <-in:
						if !ok {
							return nil
						}
					}

					output, err := transformer(ctx, input)
					if err != nil {
						return err
					}
					if output == nil {
						return errors.New("transformer returned nil output without error")
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- *output:
					}
				}
			})
		}

		if err := group.Wait(); err != nil {
			pipe.err(err)
		}
	}()
}

// Limit reads values from the input channel, forwards at most n values to the
// output channel, and then returns. It respects context cancellation while
// reading and forwarding values.
func Limit[T any](name string, pipe *Pipe, n int, in <-chan T, out chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		if n <= 0 {
			return
		}

		count := 0

	InputRecv:
		for {
			if count >= n {
				break
			}

			select {
			case <-pipe.ctx.Done():
				return
			case input, ok := <-in:
				if !ok {
					break InputRecv
				}

				select {
				case <-pipe.ctx.Done():
					return
				case out <- input:
				}

				count++
			}
		}
	}()
}

// Split routes each value read from the input channel to exactly one of the
// provided output channels, as determined by the selector function. The
// selector must return a valid index into the out slice. Panics if the
// selector returns an invalid index.
func Split[T any](name string, pipe *Pipe, selector func(context.Context, T) int, in <-chan T, out ...chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for input := range in {
			index := selector(pipe.ctx, input)
			if index < 0 || index >= len(out) {
				panic("Split: selector returned invalid index")
			}
			outputChannel := out[index]

			select {
			case <-pipe.ctx.Done():
				return
			case outputChannel <- input:
			}
		}
	}()
}

// Aggregate consumes all values from the input channel and sends the
// collected slice of values as a single item on the output channel.
func Aggregate[T any](name string, pipe *Pipe, in <-chan T, out chan<- []T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		inputs := []T{}

	InputRecv:
		for {
			select {
			case <-pipe.ctx.Done():
				return
			case input, ok := <-in:
				if !ok {
					break InputRecv
				}

				inputs = append(inputs, input)
			}
		}

		select {
		case <-pipe.ctx.Done():
			return
		case out <- inputs:
		}
	}()
}

// Reduce processes values from the input channel incrementally using a reducer
// function, combining them with an accumulator. This allows aggregating results
// as they come in without keeping all values in memory. The reducer function
// takes the current accumulator and the next value, and returns the updated
// accumulator. The final accumulated result is sent to the output channel.
func Reduce[T any, Acc any](name string, pipe *Pipe, initial Acc, reducer func(context.Context, Acc, T) (Acc, error), in <-chan T, out chan<- Acc) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		accumulator := initial

		for input := range in {
			var err error
			accumulator, err = reducer(pipe.ctx, accumulator, input)
			if err != nil {
				pipe.err(err)
				return
			}

			select {
			case <-pipe.ctx.Done():
				return
			default:
				// Continue processing
			}
		}

		select {
		case <-pipe.ctx.Done():
			return
		case out <- accumulator:
		}
	}()
}

// Flatten takes an input channel of slices and emits each element of each
// slice as an individual item on the output channel. It continues until the
// input channel is closed or the context is cancelled.
func Flatten[T any](name string, pipe *Pipe, in <-chan []T, out chan<- T) {
	pipe.group.Add(1)

	go func() {
		defer close(out)
		defer pipe.group.Done()
		defer trace.StartRegion(pipe.ctx, name).End()

		for slice := range in {
			for _, item := range slice {
				select {
				case <-pipe.ctx.Done():
					return
				case out <- item:
				}
			}
		}
	}()
}
