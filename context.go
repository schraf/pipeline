package pipeline

import (
	"context"
	"runtime/trace"
	"sync"
)

type Context struct {
	context.Context
	group *sync.WaitGroup
	err   func(error)
}

func (c Context) Go(name string, fn func(ctx context.Context) error) {
	c.group.Add(1)

	go func() {
		defer c.group.Done()
		defer trace.StartRegion(c, name).End()

		if err := fn(c); err != nil {
			c.err(err)
		}
	}()
}
