package pipeline

// Config defines the make up of a pipeline and is required for
// construction of it
type Config[In any, Out any] struct {
	Name             string
	InputChannels    uint
	InputBufferSize  uint
	OutputChannels   uint
	OutputBufferSize uint
	Composer         func(Composer[In, Out]) error
}
