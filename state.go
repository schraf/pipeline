package pipeline

import "sync/atomic"

type PipelineState struct {
	value atomic.Int32
}

const (
	StateInvalid int32 = iota
	StateCreated
	StateStarted
	StateWaiting
	StateDone
)

func newPipelineState() *PipelineState {
	state := &PipelineState{}
	state.value.Store(StateCreated)
	return state
}

func (s *PipelineState) String() string {
	switch s.value.Load() {
	case StateCreated:
		return "created"
	case StateStarted:
		return "started"
	case StateWaiting:
		return "waiting"
	case StateDone:
		return "done"
	default:
		return "invalid"
	}
}

func (s *PipelineState) Is(state int32) bool {
	return s.value.Load() == state
}

func (s *PipelineState) set(currentState, newState int32) bool {
	return s.value.CompareAndSwap(currentState, newState)
}
