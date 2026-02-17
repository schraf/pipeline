package pipeline

type Composer[In any, Out any] struct {
	ctx     Context
	inputs  []chan In
	outputs []chan Out
}

func (c Composer[In, Out]) Context() Context {
	return c.ctx
}

func (c Composer[In, Out]) Inputs() MultiChannelReceiver[In] {
	return MultiChannelReceiver[In](c.inputs)
}

func (c Composer[In, Out]) Outputs() MultiChannelSender[Out] {
	return MultiChannelSender[Out](c.outputs)
}
