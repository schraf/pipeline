package pipeline

import "iter"

type MultiChannelReceiver[T any] []chan T

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
