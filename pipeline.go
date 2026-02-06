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
type PipelineExecutor[In any, Out any] func(*Pipe, MultiChannelReceiver[In], MultiChannelSender[Out])

// PipeConfig defines the make up of a pipeline and is required for
// construction of it
type PipelineConfig[In any, Out any] struct {
	Name             string
	InputChannels    int
	InputBufferSize  int
	OutputChannels   int
	OutputBufferSize int
	Executor         PipelineExecutor[In, Out]
}

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline[In any, Out any] struct {
	cancel   context.CancelCauseFunc
	ctx      context.Context
	group    sync.WaitGroup
	errOnce  sync.Once
	task     *trace.Task
	err      error
	inputs   []chan In
	outputs  []chan Out
	executor PipelineExecutor[In, Out]
}

// NewPipeline creates a new Pipeline and a derived context for coordinating
// pipeline stages. The returned context is cancelled when any stage encounters
// an error. Use the returned Pipeline to register stages and wait for completion.
func NewPipeline[In any, Out any](ctx context.Context, cfg PipelineConfig[In, Out]) (*Pipeline[In, Out], context.Context) {
	ctx, task := trace.NewTask(ctx, cfg.Name)
	ctx, cancel := context.WithCancelCause(ctx)

	if cfg.InputChannels == 0 {
		cfg.InputChannels = 1
	}

	if cfg.OutputChannels == 0 {
		cfg.OutputChannels = 1
	}

	inputs := make([]chan In, cfg.InputChannels)

	for index := range inputs {
		inputs[index] = make(chan In, cfg.InputBufferSize)
	}

	outputs := make([]chan Out, cfg.OutputChannels)

	for index := range outputs {
		outputs[index] = make(chan Out, cfg.OutputBufferSize)
	}

	return &Pipeline[In, Out]{
		cancel:   cancel,
		ctx:      ctx,
		task:     task,
		inputs:   inputs,
		outputs:  outputs,
		executor: cfg.Executor,
	}, ctx
}

func (p *Pipeline[In, Out]) Inputs() MultiChannelSender[In] {
	return MultiChannelSender[In](p.inputs)
}

func (p *Pipeline[In, Out]) Outputs() MultiChannelReceiver[Out] {
	return MultiChannelReceiver[Out](p.outputs)
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

		inputs := MultiChannelReceiver[In](p.inputs)
		outputs := MultiChannelSender[Out](p.outputs)

		p.executor(&pipe, inputs, outputs)
	}()
}

// Wait blocks until all registered stages complete and returns the first error
// encountered by any stage, or nil if all stages completed successfully.
func (p *Pipeline[In, Out]) Wait() error {
	defer p.task.End()

	for _, input := range p.inputs {
		close(input)
	}

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
