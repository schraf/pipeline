package pipeline

import "time"

// Config defines the make up of a pipeline and is required for
// construction of it
type Config[In any, Out any] struct {
	Name             string
	InputChannels    uint
	InputBufferSize  uint
	OutputChannels   uint
	OutputBufferSize uint
	Composer         func(Composer[In, Out]) error

	// MetricsCollector, when non-nil, enables telemetry for the pipeline.
	// All pipeline boundary channels (inputs and outputs) are automatically
	// registered. Built-in stages register their internal channels as well.
	// The collector receives periodic snapshots of channel buffer utilization
	// and throughput data suitable for bottleneck detection.
	MetricsCollector MetricsCollector

	// MetricsInterval controls how often telemetry snapshots are collected
	// and delivered to the MetricsCollector. It is ignored when
	// MetricsCollector is nil. The default is 1 second.
	MetricsInterval time.Duration
}
