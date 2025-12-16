package pipeline

import (
	"context"
	"sync"
)

type Pipeline struct {
	cancel  context.CancelCauseFunc
	ctx     context.Context
	group   sync.WaitGroup
	errOnce sync.Once
	err     error
}

func WithPipeline(ctx context.Context) (*Pipeline, context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)

	return &Pipeline{
		cancel: cancel,
		ctx:    ctx,
	}, ctx
}

func (p *Pipeline) Wait() error {
	p.group.Wait()
	return p.err
}

func (p *Pipeline) setError(err error) {
	p.errOnce.Do(func() {
		p.err = err
		if p.cancel != nil {
			p.cancel(err)
		}
	})
}
