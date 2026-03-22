package pipeline

import (
	"context"
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

func (m MultiChannelSender[T]) At(index uint) chan<- T {
	if int(index) >= len(m) {
		return nil
	}

	return m[index]
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

func (m MultiChannelSender[T]) Link(ctx Context, index uint, in <-chan T) error {
	if int(index) >= len(m) {
		return ErrInvalidChannel
	}

	sends, recvs := RegisterCounter(ctx.Telemetry(), fmt.Sprintf("link[%d]", index))

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

				if recvs != nil {
					recvs.Add(1)
				}

				select {
				case m[index] <- value:
					if sends != nil {
						sends.Add(1)
					}
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
		return ErrInvalidChannel
	}

	for index := 0; index < len(m); index++ {
		idx := index
		sends, recvs := RegisterCounter(ctx.Telemetry(), fmt.Sprintf("link_all[%d]", idx))

		ctx.Go("link_all", func(ctx context.Context) error {
			defer close(m[idx])

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case value, ok := <-in[idx]:
					if !ok {
						return nil
					}

					if recvs != nil {
						recvs.Add(1)
					}

					select {
					case m[idx] <- value:
						if sends != nil {
							sends.Add(1)
						}
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		})
	}

	return nil
}

func (m MultiChannelSender[T]) Send(ctx context.Context, index uint, values ...T) error {
	if int(index) >= len(m) {
		return ErrInvalidChannel
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
