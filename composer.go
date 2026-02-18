package pipeline

type Composer[In any, Out any] struct {
	ctx     Context
	inputs  MultiChannelReceiver[In]
	outputs MultiChannelSender[Out]
}

func (c Composer[In, Out]) Context() Context {
	return c.ctx
}

func (c Composer[In, Out]) Inputs() MultiChannelReceiver[In] {
	return c.inputs
}

func (c Composer[In, Out]) Outputs() MultiChannelSender[Out] {
	return c.outputs
}
