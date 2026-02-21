package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
)

type OutputChannel[T any] interface {
	~chan T | ~chan<- T
}

type MultiChannelSender[T any] []chan<- T

func NewMultiChannelSender[T any, C OutputChannel[T]](out ...C) MultiChannelSender[T] {
	outputs := make([]chan<- T, len(out))

	for index := range out {
		outputs[index] = out[index]
	}

	return MultiChannelSender[T](outputs)
}

// At returns the channel at the given index. It panics if the index is out of
// range, following the same convention as Go slice indexing.
func (m MultiChannelSender[T]) At(index int) (chan<- T, bool) {
	if index < 0 || index >= len(m) {
		return nil, false
	}

	return m[index], true
}

func (m MultiChannelSender[T]) Len() int {
	return len(m)
}

func (m MultiChannelSender[T]) Iter() iter.Seq[chan<- T] {
	return func(yield func(chan<- T) bool) {
		for i := 0; i < len(m); i++ {
			if !yield(m[i]) {
				return
			}
		}
	}
}

func (m MultiChannelSender[T]) Link(ctx Context, index int, in <-chan T) error {
	if index < 0 || index >= len(m) {
		return fmt.Errorf("runtime error: channel index %d out of range", index)
	}

	ctx.Go("link", func(ctx context.Context) error {
		defer close(m[index])

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case value, ok := <-in:
				if !ok {
					return nil
				}

				select {
				case m[index] <- value:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})

	return nil
}

func (m MultiChannelSender[T]) LinkAll(ctx Context, in MultiChannelReceiver[T]) error {
	if len(m) != in.Len() {
		return errors.New("runtime error: channel link size mismatch")
	}

	for index := 0; index < len(m); index++ {
		idx := index
		ctx.Go("link_all", func(ctx context.Context) error {
			defer close(m[idx])

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case value, ok := <-in.At(idx):
					if !ok {
						return nil
					}

					select {
					case m[idx] <- value:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		})
	}

	return nil
}

func (m MultiChannelSender[T]) Send(ctx context.Context, index int, values ...T) error {
	if index < 0 || index >= len(m) {
		return fmt.Errorf("runtime error: channel index %d out of range", index)
	}

	for _, value := range values {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m[index] <- value:
		}
	}

	return nil
}

func (m MultiChannelSender[T]) SendToAll(ctx context.Context, values ...T) error {
	for _, value := range values {
		for _, ch := range m {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- value:
			}
		}
	}

	return nil
}

func (m MultiChannelSender[T]) SendRoundRobin(ctx context.Context, values ...T) error {
	if len(m) == 0 {
		return nil
	}

	for i, value := range values {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m[i%len(m)] <- value:
		}
	}

	return nil
}
