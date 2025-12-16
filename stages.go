package pipeline

import (
	"errors"

	"golang.org/x/sync/errgroup"
)

// Transform reads values from the input channel, applies the transformer
// function, and forwards successful results to the output channel until the
// context is done or the input channel is closed. The transformer must return
// a non-nil pointer when err is nil, otherwise a panic will occur.
func Transform[In any, Out any](p *Pipeline, transformer func(In) (*Out, error), in <-chan In, out chan<- Out) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		for input := range in {
			output, err := transformer(input)
			if err != nil {
				p.setError(err)
				return
			}
			if output == nil {
				p.setError(errors.New("transformer returned nil output without error"))
				return
			}

			select {
			case <-p.ctx.Done():
				return
			case out <- *output:
			}
		}
	}()
}

// Filter reads values from the input channel, applies the filter predicate,
// and forwards only values that satisfy the predicate to the output channel.
// It respects context cancellation and stops processing on error.
func Filter[T any](p *Pipeline, filter func(T) (bool, error), in <-chan T, out chan<- T) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		for input := range in {
			shouldForward, err := filter(input)
			if err != nil {
				p.setError(err)
				return
			}

			if !shouldForward {
				continue
			}

			select {
			case <-p.ctx.Done():
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
func Batch[In any, Out any](p *Pipeline, batcher func([]In) (*Out, error), batchSize int, in <-chan In, out chan<- Out) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		batch := make([]In, 0, batchSize)

		for input := range in {
			batch = append(batch, input)

			if len(batch) >= batchSize {
				localBatch := append([]In(nil), batch...)

				output, err := batcher(localBatch)
				if err != nil {
					p.setError(err)
					return
				}
				if output == nil {
					p.setError(errors.New("batcher returned nil output without error"))
					return
				}

				select {
				case <-p.ctx.Done():
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

			output, err := batcher(batchCopy)
			if err != nil {
				p.setError(err)
				return
			}
			if output == nil {
				p.setError(errors.New("batcher returned nil output without error"))
				return
			}

			select {
			case <-p.ctx.Done():
				return
			case out <- *output:
			}
		}
	}()
}

// FanIn merges multiple input channels into a single output channel,
// forwarding all values from each input until the context is done or all
// inputs are closed.
func FanIn[T any](p *Pipeline, out chan<- T, in ...<-chan T) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		group, ctx := errgroup.WithContext(p.ctx)

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
			p.setError(err)
		}
	}()
}

// FanOut distributes items from a single input channel to multiple output
// channels, sending each item to all output channels.
func FanOut[T any](p *Pipeline, in <-chan T, out ...chan<- T) {
	p.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer p.group.Done()

		group, ctx := errgroup.WithContext(p.ctx)

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
			p.setError(err)
		}
	}()
}

// FanOutRoundRobin distributes items from a single input channel to multiple
// output channels using round-robin distribution, sending each item to only
// one output channel. Panics if no output channels are provided.
func FanOutRoundRobin[T any](p *Pipeline, in <-chan T, out ...chan<- T) {
	if len(out) == 0 {
		panic("FanOutRoundRobin: at least one output channel required")
	}

	p.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer p.group.Done()
		index := 0

		for input := range in {
			outputChannel := out[index%len(out)]

			select {
			case <-p.ctx.Done():
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
func ParallelTransform[In any, Out any](p *Pipeline, workers int, transformer func(In) (*Out, error), in <-chan In, out chan<- Out) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		group, ctx := errgroup.WithContext(p.ctx)

		for i := 0; i < workers; i++ {
			group.Go(func() error {
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

					output, err := transformer(input)
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
			p.setError(err)
		}
	}()
}

// Limit reads values from the input channel, forwards at most n values to the
// output channel, and then returns. It respects context cancellation while
// reading and forwarding values.
func Limit[T any](p *Pipeline, n int, in <-chan T, out chan<- T) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

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
			case <-p.ctx.Done():
				return
			case input, ok := <-in:
				if !ok {
					break InputRecv
				}

				select {
				case <-p.ctx.Done():
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
func Split[T any](p *Pipeline, selector func(T) int, in <-chan T, out ...chan<- T) {
	p.group.Add(1)

	go func() {
		defer func() {
			for _, outputChannel := range out {
				close(outputChannel)
			}
		}()
		defer p.group.Done()

		for input := range in {
			index := selector(input)
			if index < 0 || index >= len(out) {
				panic("Split: selector returned invalid index")
			}
			outputChannel := out[index]

			select {
			case <-p.ctx.Done():
				return
			case outputChannel <- input:
			}
		}
	}()
}

// Aggregate consumes all values from the input channel and sends the
// collected slice of values as a single item on the output channel.
func Aggregate[T any](p *Pipeline, in <-chan T, out chan<- []T) {
	p.group.Add(1)

	go func() {
		defer close(out)
		defer p.group.Done()

		inputs := []T{}

	InputRecv:
		for {
			select {
			case <-p.ctx.Done():
				return
			case input, ok := <-in:
				if !ok {
					break InputRecv
				}

				inputs = append(inputs, input)
			}
		}

		select {
		case <-p.ctx.Done():
			return
		case out <- inputs:
		}
	}()
}
