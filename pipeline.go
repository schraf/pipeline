// Package pipeline provides composable stages for building concurrent data
// processing pipelines using channels. It handles context cancellation, error
// propagation, and goroutine lifecycle management automatically.
package pipeline

import (
	"context"
	"runtime/trace"
	"sync"
)

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline struct {
	cancel  context.CancelCauseFunc
	ctx     context.Context
	group   sync.WaitGroup
	errOnce sync.Once
	task    *trace.Task
	err     error
}

// WithPipeline creates a new Pipeline and a derived context for coordinating
// pipeline stages. The returned context is cancelled when any stage encounters
// an error. Use the returned Pipeline to register stages and wait for completion.
func WithPipeline(ctx context.Context, name string) (*Pipeline, context.Context) {
	ctx, task := trace.NewTask(ctx, name)
	ctx, cancel := context.WithCancelCause(ctx)

	return &Pipeline{
		cancel: cancel,
		ctx:    ctx,
		task:   task,
	}, ctx
}

// Wait blocks until all registered stages complete and returns the first error
// encountered by any stage, or nil if all stages completed successfully.
func (p *Pipeline) Wait() error {
	defer p.task.End()
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
