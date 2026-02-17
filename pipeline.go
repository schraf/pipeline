// Package pipeline provides composable stages for building concurrent data
// processing pipelines using channels. It handles context cancellation, error
// propagation, and goroutine lifecycle management automatically.
package pipeline

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"
)

const (
	stateInvalid int32 = iota
	stateCreated
	stateStarted
	stateWaiting
	stateDone
)

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline[In any, Out any] struct {
	state   atomic.Int32
	cancel  context.CancelCauseFunc
	ctx     context.Context
	group   sync.WaitGroup
	errOnce sync.Once
	task    *trace.Task
	err     error
	inputs  []chan In
	outputs []chan Out
}

// NewPipeline creates a new Pipeline and a derived context for coordinating
// pipeline stages. The returned context is cancelled when any stage encounters
// an error. Use the returned Pipeline to register stages and wait for completion.
func NewPipeline[In any, Out any](ctx context.Context, cfg Config[In, Out]) (*Pipeline[In, Out], context.Context) {
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
		cancel:  cancel,
		ctx:     ctx,
		task:    task,
		inputs:  inputs,
		outputs: outputs,
	}

	pipeline.state.Store(stateCreated)

	cfg.Composer(Composer[In, Out]{
		ctx:     pipeline.context(),
		inputs:  pipeline.inputs,
		outputs: pipeline.outputs,
	})

	return pipeline, ctx
}

func (p *Pipeline[In, Out]) context() Context {
	return Context{
		Context: p.ctx,
		group:   &p.group,
		err:     p.setError,
	}
}

func (p *Pipeline[In, Out]) Inputs() MultiChannelSender[In] {
	return MultiChannelSender[In](p.inputs)
}

func (p *Pipeline[In, Out]) Outputs() MultiChannelReceiver[Out] {
	return MultiChannelReceiver[Out](p.outputs)
}

func (p *Pipeline[In, Out]) Start() error {
	if !p.state.CompareAndSwap(stateCreated, stateStarted) {
		return fmt.Errorf("unable to start pipeline, unexpected state: %d", p.state.Load())
	}

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
	if !p.state.CompareAndSwap(stateStarted, stateWaiting) {
		return fmt.Errorf("unable to wait on pipeline, unexpected state: %d", p.state.Load())
	}

	defer p.task.End()
	defer p.state.Store(stateDone)
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
