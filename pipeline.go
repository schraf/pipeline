// Package pipeline provides composable stages for building concurrent data
// processing pipelines using channels. It handles context cancellation, error
// propagation, and goroutine lifecycle management automatically.
package pipeline

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync"
)

// Pipe is used in defining stages in a PipelineExecutor function.
type Pipe struct {
	ctx   context.Context
	group *sync.WaitGroup
	err   func(error)
}

// Context returns the context of the pipeline
func (p Pipe) Context() context.Context {
	return p.ctx
}

// PipelineParameters defines types needed for the body of the pipeline
// execution.
type PipelineParameters[In any, Out any] struct {
	Pipe *Pipe
	In   MultiChannelReceiver[In]
	Out  MultiChannelSender[Out]
}

// PipelineExecutor defines the execution body of the pipeline. It will
// need to connect the input channels to the output channels.
type PipelineExecutor[In any, Out any] func(*PipelineParameters[In, Out])

// PipeConfig defines the make up of a pipeline and is required for
// construction of it
type PipelineConfig[In any, Out any] struct {
	Name             string
	InputChannels    int
	InputBufferSize  int
	OutputChannels   int
	OutputBufferSize int
	StartImmediately bool
	Executor         PipelineExecutor[In, Out]
}

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline[In any, Out any] struct {
	state    *PipelineState
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

	pipeline := &Pipeline[In, Out]{
		state:    newPipelineState(),
		cancel:   cancel,
		ctx:      ctx,
		task:     task,
		inputs:   inputs,
		outputs:  outputs,
		executor: cfg.Executor,
	}

	if cfg.StartImmediately {
		if err := pipeline.Start(); err != nil {
			// we should always be able to start on creation of a new pipeline
			panic("pipeline failed to start immediately")
		}
	}

	return pipeline, ctx
}

func (p *Pipeline[In, Out]) State() *PipelineState {
	return p.state
}

func (p *Pipeline[In, Out]) Context() context.Context {
	return p.ctx
}

func (p *Pipeline[In, Out]) Inputs() MultiChannelSender[In] {
	return MultiChannelSender[In](p.inputs)
}

func (p *Pipeline[In, Out]) Outputs() MultiChannelReceiver[Out] {
	return MultiChannelReceiver[Out](p.outputs)
}

func (p *Pipeline[In, Out]) Start() error {
	if !p.state.set(StateCreated, StateStarted) {
		return fmt.Errorf("unable to start pipeline, unexpected state: %s", p.state.String())
	}

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

		params := PipelineParameters[In, Out]{
			Pipe: &pipe,
			In:   inputs,
			Out:  outputs,
		}

		p.executor(&params)
	}()

	return nil
}

// CloseAllInputs will close all of the input channels
func (p *Pipeline[In, Out]) CloseAllInputs() {
	for _, input := range p.inputs {
		close(input)
	}
}

// Wait blocks until all registered stages complete and returns the first error
// encountered by any stage, or nil if all stages completed successfully.
func (p *Pipeline[In, Out]) Wait() error {
	if !p.state.set(StateStarted, StateWaiting) {
		return fmt.Errorf("unable to wait on pipeline, unexpected state: %s", p.state.String())
	}

	defer p.task.End()
	defer p.state.value.Store(StateDone)
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
