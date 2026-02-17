package pipeline

import (
	"context"
	"fmt"
	"iter"
)

type MultiChannelSender[T any] []chan T

func (m MultiChannelSender[T]) At(index int) chan<- T {
	if index < 0 || index >= len(m) {
		panic("runtime error: channel index out of range")
	}

	return m[index]
}

func (m MultiChannelSender[T]) Receiver(index int) <-chan T {
	if index < 0 || index >= len(m) {
		panic("runtime error: channel index out of range")
	}

	return m[index]
}

func (m MultiChannelSender[T]) Receivers() MultiChannelReceiver[T] {
	return MultiChannelReceiver[T](m)
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
