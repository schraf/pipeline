package pipeline

import (
	"context"
	"iter"
	"slices"
)

type InputChannel[T any] interface {
	~chan T | ~<-chan T
}

type MultiChannelReceiver[T any] []<-chan T

func NewMultiChannelReceiver[T any, C InputChannel[T]](in ...C) MultiChannelReceiver[T] {
	inputs := make([]<-chan T, len(in))

	for index := range in {
		inputs[index] = in[index]
	}

	return MultiChannelReceiver[T](inputs)
}

func (m MultiChannelReceiver[T]) At(index int) <-chan T {
	if index < 0 || index >= len(m) {
		panic("runtime error: channel index out of range")
	}

	return m[index]
}

func (m MultiChannelReceiver[T]) Len() int {
	return len(m)
}

func (m MultiChannelReceiver[T]) Iter() iter.Seq[<-chan T] {
	return func(yield func(<-chan T) bool) {
		for i := 0; i < len(m); i++ {
			if !yield(m[i]) {
				return
			}
		}
	}
}

func (m MultiChannelReceiver[T]) SinkAtIter(ctx context.Context, index int) iter.Seq[T] {
	if index < 0 || index >= len(m) {
		panic("runtime error: channel index out of range")
	}

	return func(yield func(T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-m[index]:
				if !ok {
					return
				}

				if !yield(value) {
					return
				}
			}
		}
	}
}

func (m MultiChannelReceiver[T]) SinkAt(ctx context.Context, index int) []T {
	return slices.Collect(m.SinkAtIter(ctx, index))
}
