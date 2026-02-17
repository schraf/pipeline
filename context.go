package pipeline

import (
	"context"
	"runtime/trace"
	"sync"
)

type Context struct {
	parent context.Context
	group  *sync.WaitGroup
	err    func(error)
}

func (c Context) Go(name string, fn func(ctx context.Context) error) {
	c.group.Add(1)

	go func() {
		defer c.group.Done()
		defer trace.StartRegion(c.parent, name).End()

		if err := fn(c.parent); err != nil {
			c.err(err)
		}
	}()
}

// Context returns the underlying context.Context
func (c Context) Context() context.Context {
	return c.parent
}
