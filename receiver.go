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

func (m MultiChannelReceiver[T]) At(index uint) <-chan T {
	if int(index) >= len(m) {
		return nil
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

func (m MultiChannelReceiver[T]) SinkAtIter(ctx context.Context, index uint) iter.Seq[T] {
	return func(yield func(T) bool) {
		if int(index) >= len(m) {
			return
		}

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

func (m MultiChannelReceiver[T]) SinkAt(ctx context.Context, index uint) []T {
	return slices.Collect(m.SinkAtIter(ctx, index))
}
