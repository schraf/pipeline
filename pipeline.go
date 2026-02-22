// Package pipeline provides composable stages for building concurrent data
// processing pipelines using channels. It handles context cancellation, error
// propagation, and goroutine lifecycle management automatically.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"
)

const (
	stateInvalid int32 = iota
	stateStarted
	stateWaiting
	stateDone
)

var stateNames = map[int32]string{
	stateInvalid: "Invalid",
	stateStarted: "Started",
	stateWaiting: "Waiting",
	stateDone:    "Done",
}

func stateName(s int32) string {
	if name, ok := stateNames[s]; ok {
		return name
	}

	return fmt.Sprintf("Unknown(%d)", s)
}

// Pipeline coordinates concurrent processing stages, managing their lifecycle
// and propagating errors and cancellation signals across all stages.
type Pipeline[In any, Out any] struct {
	state      atomic.Int32
	cancel     context.CancelCauseFunc
	ctx        context.Context
	group      sync.WaitGroup
	cancelOnce sync.Once
	closeOnce  sync.Once
	task       *trace.Task
	errLock    sync.Mutex
	err        error
	inputs     []chan In
	outputs    []chan Out
}

// NewPipeline creates a new Pipeline and a derived context for coordinating
// pipeline stages. The returned context is cancelled when any stage encounters
// an error. Use the returned Pipeline to register stages and wait for completion.
func NewPipeline[In any, Out any](ctx context.Context, cfg Config[In, Out]) (*Pipeline[In, Out], context.Context, error) {
	if cfg.Composer == nil {
		return nil, ctx, fmt.Errorf("pipeline: Config.Composer must not be nil")
	}

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

	pipeline.state.Store(stateStarted)

	if err := cfg.Composer(Composer[In, Out]{
		ctx:     pipeline.context(),
		inputs:  NewMultiChannelReceiver(pipeline.inputs...),
		outputs: NewMultiChannelSender(pipeline.outputs...),
	}); err != nil {
		cancel(err)
		task.End()
		return nil, ctx, fmt.Errorf("pipeline: composer failed: %w", err)
	}

	return pipeline, ctx, nil
}

func (p *Pipeline[In, Out]) context() Context {
	return Context{
		Context: p.ctx,
		group:   &p.group,
		err:     p.setError,
	}
}

func (p *Pipeline[In, Out]) Inputs() MultiChannelSender[In] {
	return NewMultiChannelSender(p.inputs...)
}

func (p *Pipeline[In, Out]) Outputs() MultiChannelReceiver[Out] {
	return NewMultiChannelReceiver(p.outputs...)
}

// CloseAllInputs will close all of the input channels. It is safe to call
// multiple times; only the first call will close the channels.
func (p *Pipeline[In, Out]) CloseAllInputs() {
	p.closeOnce.Do(func() {
		for _, input := range p.inputs {
			close(input)
		}
	})
}

// Wait blocks until all registered stages complete and returns all errors
// encountered by any stage (joined via errors.Join), or nil if all stages
// completed successfully.
func (p *Pipeline[In, Out]) Wait() error {
	if !p.state.CompareAndSwap(stateStarted, stateWaiting) {
		return fmt.Errorf("unable to wait on pipeline, unexpected state: %s", stateName(p.state.Load()))
	}

	defer p.cancel(nil)
	defer p.task.End()
	defer p.state.Store(stateDone)
	p.group.Wait()
	return p.err
}

func (p *Pipeline[In, Out]) setError(err error) {
	// Cancel the context on the first error to signal all stages to stop.
	p.cancelOnce.Do(func() {
		if p.cancel != nil {
			p.cancel(err)
		}
	})

	// Aggregate all errors so the caller gets full visibility.
	p.errLock.Lock()
	p.err = errors.Join(p.err, err)
	p.errLock.Unlock()
}
