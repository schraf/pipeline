package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ChannelSnapshot is a point-in-time view of a single registered channel's state.
//
// When a channel is unbuffered (Cap == 0), Len and Utilization are always 0.
// Buffer utilization is not meaningful for unbuffered channels because items
// are transferred directly between sender and receiver with no intermediate
// buffering. For unbuffered channels, use TotalSent/TotalRecv throughput
// counters (available on Link paths and Meter'd channels) to compare
// processing rates between stages and identify bottlenecks.
type ChannelSnapshot struct {
	// Name identifies the channel (e.g. "transform", "input[0]").
	Name string

	// Len is the number of items currently buffered in the channel.
	// Always 0 for unbuffered channels.
	Len int

	// Cap is the total buffer capacity of the channel.
	// 0 indicates an unbuffered channel.
	Cap int

	// Utilization is Len/Cap as a value between 0.0 and 1.0.
	// It is 0.0 when the channel is unbuffered (Cap == 0).
	// Not meaningful for unbuffered channels; use throughput counters instead.
	Utilization float64

	// TotalSent is the cumulative number of items sent into the channel.
	// It is -1 when throughput counting is not enabled for this channel.
	TotalSent int64

	// TotalRecv is the cumulative number of items received from the channel.
	// It is -1 when throughput counting is not enabled for this channel.
	TotalRecv int64
}

// PipelineSnapshot is a point-in-time view of all registered channels in a pipeline.
type PipelineSnapshot struct {
	// PipelineName is the name of the pipeline as provided in Config.Name.
	PipelineName string

	// Timestamp is the time at which the snapshot was taken.
	Timestamp time.Time

	// Channels contains one entry per registered channel.
	Channels []ChannelSnapshot
}

// MetricsCollector receives periodic telemetry snapshots from a pipeline.
// Implementations must be safe for concurrent use.
type MetricsCollector interface {
	// OnSnapshot is called periodically with a snapshot of all registered channels.
	OnSnapshot(PipelineSnapshot)
}

// channelProbe is a type-erased handle to a registered channel.
// Closures are used to avoid reflection while retaining type safety at
// the registration site.
type channelProbe struct {
	name  string
	lenFn func() int
	capFn func() int
	sends *atomic.Int64 // nil when throughput counting is not enabled
	recvs *atomic.Int64 // nil when throughput counting is not enabled
}

// Telemetry is a registry of channels and a periodic sampler that reports
// snapshots through a MetricsCollector. A nil *Telemetry is safe to use;
// all methods and the package-level registration functions treat nil as a
// no-op.
type Telemetry struct {
	pipelineName string
	collector    MetricsCollector
	interval     time.Duration

	lock   sync.RWMutex
	probes []channelProbe

	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// NewTelemetry creates a new telemetry registry. The sampler is not started
// until Start is called.
func NewTelemetry(pipelineName string, collector MetricsCollector, interval time.Duration) *Telemetry {
	if collector == nil {
		return nil
	}

	if interval <= 0 {
		interval = time.Second
	}

	return &Telemetry{
		pipelineName: pipelineName,
		collector:    collector,
		interval:     interval,
		done:         make(chan struct{}),
	}
}

// RegisterChannel registers a channel for periodic len/cap sampling.
// If t is nil the call is a no-op.
func RegisterChannel[T any](t *Telemetry, name string, ch chan T) {
	if t == nil {
		return
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	t.probes = append(t.probes, channelProbe{
		name:  name,
		lenFn: func() int { return len(ch) },
		capFn: func() int { return cap(ch) },
	})
}

// RegisterChannelWithCounters registers a channel for periodic len/cap
// sampling and returns atomic counters that the caller should increment on
// each send and receive. If t is nil the call returns (nil, nil) and the
// caller should skip incrementing.
func RegisterChannelWithCounters[T any](t *Telemetry, name string, ch chan T) (sends *atomic.Int64, recvs *atomic.Int64) {
	if t == nil {
		return nil, nil
	}

	sends = &atomic.Int64{}
	recvs = &atomic.Int64{}

	t.lock.Lock()
	defer t.lock.Unlock()

	t.probes = append(t.probes, channelProbe{
		name:  name,
		lenFn: func() int { return len(ch) },
		capFn: func() int { return cap(ch) },
		sends: sends,
		recvs: recvs,
	})

	return sends, recvs
}

// Start launches the periodic sampler goroutine. It is bound to the provided
// context and will also stop when Stop is called.
func (t *Telemetry) Start(ctx context.Context) {
	if t == nil {
		return
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	// Check if already stopped or already started
	select {
	case <-t.done:
		return
	default:
	}

	if t.cancel != nil {
		return
	}

	ctx, t.cancel = context.WithCancel(ctx)

	go func() {
		defer close(t.done)

		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap := t.Snapshot()
				t.collector.OnSnapshot(snap)
			}
		}
	}()
}

// Stop stops the periodic sampler and waits for it to exit. It is safe to
// call multiple times.
func (t *Telemetry) Stop() {
	if t == nil {
		return
	}

	t.stopOnce.Do(func() {
		t.lock.Lock()
		if t.cancel != nil {
			t.cancel()
		} else {
			// Signal completion if never started to avoid hanging
			select {
			case <-t.done:
			default:
				close(t.done)
			}
		}
		t.lock.Unlock()
	})

	<-t.done
}

// RegisterCounter registers a named throughput counter pair without an
// associated channel. This is useful for tracking items flowing through a
// link or other pathway where a bidirectional channel reference is not
// available. If t is nil the call returns (nil, nil).
func RegisterCounter(t *Telemetry, name string) (sends *atomic.Int64, recvs *atomic.Int64) {
	if t == nil {
		return nil, nil
	}

	sends = &atomic.Int64{}
	recvs = &atomic.Int64{}

	t.lock.Lock()
	defer t.lock.Unlock()

	t.probes = append(t.probes, channelProbe{
		name:  name,
		lenFn: func() int { return 0 },
		capFn: func() int { return 0 },
		sends: sends,
		recvs: recvs,
	})

	return sends, recvs
}

// Snapshot takes an immediate point-in-time snapshot of all registered
// channels.
func (t *Telemetry) Snapshot() PipelineSnapshot {
	if t == nil {
		return PipelineSnapshot{}
	}

	t.lock.RLock()
	defer t.lock.RUnlock()

	snap := PipelineSnapshot{
		PipelineName: t.pipelineName,
		Timestamp:    time.Now(),
		Channels:     make([]ChannelSnapshot, len(t.probes)),
	}

	for i, p := range t.probes {
		l := p.lenFn()
		c := p.capFn()

		var utilization float64
		if c > 0 {
			utilization = float64(l) / float64(c)
		}

		var totalSent, totalRecv int64 = -1, -1
		if p.sends != nil {
			totalSent = p.sends.Load()
		}
		if p.recvs != nil {
			totalRecv = p.recvs.Load()
		}

		snap.Channels[i] = ChannelSnapshot{
			Name:        p.name,
			Len:         l,
			Cap:         c,
			Utilization: utilization,
			TotalSent:   totalSent,
			TotalRecv:   totalRecv,
		}
	}

	return snap
}
