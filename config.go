package pipeline

// Config defines the make up of a pipeline and is required for
// construction of it
type Config[In any, Out any] struct {
	Name             string
	InputChannels    int
	InputBufferSize  int
	OutputChannels   int
	OutputBufferSize int
	Composer         func(Composer[In, Out])
}
