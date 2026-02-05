// Package pipeline provides composable stages for building concurrent data
// processing pipelines using channels. It handles context cancellation, error
// propagation, and goroutine lifecycle management automatically.
package pipeline

import (
	"context"
	"runtime/trace"
	"sync"
)

// Pipe is used in defining stages in a PipelineExecutor function.
type Pipe struct {
	ctx   context.Context
	group *sync.WaitGroup
	err   func(error)
}

// PipeExecutor defines the body of the pipeline. The function should connect
// the input channel to the output channel using stages on the provided pipe.
type PipelineExecutor[In any, Out any] func(*Pipe, <-chan In, chan<- Out)

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline[In any, Out any] struct {
	cancel   context.CancelCauseFunc
	ctx      context.Context
	group    sync.WaitGroup
	errOnce  sync.Once
	task     *trace.Task
	err      error
	input    chan In
	output   chan Out
	executor PipelineExecutor[In, Out]
}

type PipelineConfig[In any, Out any] struct {
	Name             string
	InputBufferSize  int
	OutputBufferSize int
	Executor         PipelineExecutor[In, Out]
}

// NewPipeline creates a new Pipeline and a derived context for coordinating
// pipeline stages. The returned context is cancelled when any stage encounters
// an error. Use the returned Pipeline to register stages and wait for completion.
func NewPipeline[In any, Out any](ctx context.Context, cfg PipelineConfig[In, Out]) (*Pipeline[In, Out], context.Context) {
	ctx, task := trace.NewTask(ctx, cfg.Name)
	ctx, cancel := context.WithCancelCause(ctx)

	return &Pipeline[In, Out]{
		cancel:   cancel,
		ctx:      ctx,
		task:     task,
		input:    make(chan In, cfg.InputBufferSize),
		output:   make(chan Out, cfg.OutputBufferSize),
		executor: cfg.Executor,
	}, ctx
}

func (p *Pipeline[In, Out]) Input() chan<- In {
	return p.input
}

func (p *Pipeline[In, Out]) Output() <-chan Out {
	return p.output
}

func (p *Pipeline[In, Out]) Start() {
	p.group.Add(1)

	go func() {
		defer p.group.Done()

		pipe := Pipe{
			ctx:   p.ctx,
			group: &p.group,
			err: func(err error) {
				p.setError(err)
			},
		}

		p.executor(&pipe, p.input, p.output)
	}()
}

// Wait blocks until all registered stages complete and returns the first error
// encountered by any stage, or nil if all stages completed successfully.
func (p *Pipeline[In, Out]) Wait() error {
	defer p.task.End()
	close(p.input)
	p.group.Wait()
	return p.err
}

func (p *Pipeline[In, Out]) setError(err error) {
	p.errOnce.Do(func() {
		p.err = err
		if p.cancel != nil {
			p.cancel(err)
		}
	})
}
