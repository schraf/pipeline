package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCollector records all snapshots delivered to it.
type mockCollector struct {
	mu        sync.Mutex
	snapshots []PipelineSnapshot
}

func (m *mockCollector) OnSnapshot(snap PipelineSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots = append(m.snapshots, snap)
}

func (m *mockCollector) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.snapshots)
}

func (m *mockCollector) latest() PipelineSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshots[len(m.snapshots)-1]
}

// ─── Nil safety ──────────────────────────────────────────────────────────

func TestNilTelemetry_RegisterChannel(t *testing.T) {
	ch := make(chan int, 5)
	// Must not panic.
	RegisterChannel[int](nil, "test", ch)
}

func TestNilTelemetry_RegisterChannelWithCounters(t *testing.T) {
	ch := make(chan int, 5)
	sends, recvs := RegisterChannelWithCounters[int](nil, "test", ch)
	assert.Nil(t, sends)
	assert.Nil(t, recvs)
}

func TestNilTelemetry_RegisterCounter(t *testing.T) {
	sends, recvs := RegisterCounter(nil, "test")
	assert.Nil(t, sends)
	assert.Nil(t, recvs)
}

func TestNilTelemetry_Snapshot(t *testing.T) {
	var tel *Telemetry
	snap := tel.Snapshot()
	assert.Empty(t, snap.Channels)
}

func TestNilTelemetry_StartStop(t *testing.T) {
	var tel *Telemetry
	// Must not panic.
	tel.Start(context.Background())
	tel.Stop()
}

// ─── NewTelemetry ────────────────────────────────────────────────────────

func TestNewTelemetry_NilCollector(t *testing.T) {
	tel := NewTelemetry("test", nil, time.Second)
	assert.Nil(t, tel)
}

func TestNewTelemetry_DefaultInterval(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, 0)
	require.NotNil(t, tel)
	assert.Equal(t, time.Second, tel.interval)
}

func TestNewTelemetry_NegativeInterval(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, -5*time.Second)
	require.NotNil(t, tel)
	assert.Equal(t, time.Second, tel.interval)
}

// ─── Channel registration and snapshots ──────────────────────────────────

func TestRegisterChannel_Snapshot(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test-pipeline", col, time.Second)

	ch := make(chan int, 10)
	RegisterChannel(tel, "my-channel", ch)

	// Put 3 items in the buffer.
	ch <- 1
	ch <- 2
	ch <- 3

	snap := tel.Snapshot()
	assert.Equal(t, "test-pipeline", snap.PipelineName)
	require.Len(t, snap.Channels, 1)

	cs := snap.Channels[0]
	assert.Equal(t, "my-channel", cs.Name)
	assert.Equal(t, 3, cs.Len)
	assert.Equal(t, 10, cs.Cap)
	assert.InDelta(t, 0.3, cs.Utilization, 0.01)
	assert.Equal(t, int64(-1), cs.TotalSent)
	assert.Equal(t, int64(-1), cs.TotalRecv)
}

func TestRegisterChannel_Unbuffered(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	ch := make(chan string)
	RegisterChannel(tel, "unbuf", ch)

	snap := tel.Snapshot()
	require.Len(t, snap.Channels, 1)

	cs := snap.Channels[0]
	assert.Equal(t, 0, cs.Len)
	assert.Equal(t, 0, cs.Cap)
	assert.Equal(t, 0.0, cs.Utilization)
}

func TestRegisterChannel_FullBuffer(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	ch := make(chan int, 5)
	RegisterChannel(tel, "full", ch)

	for i := 0; i < 5; i++ {
		ch <- i
	}

	snap := tel.Snapshot()
	require.Len(t, snap.Channels, 1)

	cs := snap.Channels[0]
	assert.Equal(t, 5, cs.Len)
	assert.Equal(t, 5, cs.Cap)
	assert.InDelta(t, 1.0, cs.Utilization, 0.01)
}

func TestRegisterChannelWithCounters(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	ch := make(chan int, 10)
	sends, recvs := RegisterChannelWithCounters(tel, "counted", ch)
	require.NotNil(t, sends)
	require.NotNil(t, recvs)

	sends.Add(42)
	recvs.Add(37)

	snap := tel.Snapshot()
	require.Len(t, snap.Channels, 1)

	cs := snap.Channels[0]
	assert.Equal(t, int64(42), cs.TotalSent)
	assert.Equal(t, int64(37), cs.TotalRecv)
}

func TestRegisterCounter(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	sends, recvs := RegisterCounter(tel, "link[0]")
	require.NotNil(t, sends)
	require.NotNil(t, recvs)

	sends.Add(100)
	recvs.Add(99)

	snap := tel.Snapshot()
	require.Len(t, snap.Channels, 1)

	cs := snap.Channels[0]
	assert.Equal(t, "link[0]", cs.Name)
	assert.Equal(t, 0, cs.Len)
	assert.Equal(t, 0, cs.Cap)
	assert.Equal(t, int64(100), cs.TotalSent)
	assert.Equal(t, int64(99), cs.TotalRecv)
}

func TestMultipleRegistrations(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	ch1 := make(chan int, 5)
	ch2 := make(chan int, 10)
	ch3 := make(chan int, 20)

	RegisterChannel(tel, "first", ch1)
	RegisterChannel(tel, "second", ch2)
	RegisterChannel(tel, "third", ch3)

	snap := tel.Snapshot()
	require.Len(t, snap.Channels, 3)
	assert.Equal(t, "first", snap.Channels[0].Name)
	assert.Equal(t, "second", snap.Channels[1].Name)
	assert.Equal(t, "third", snap.Channels[2].Name)
	assert.Equal(t, 5, snap.Channels[0].Cap)
	assert.Equal(t, 10, snap.Channels[1].Cap)
	assert.Equal(t, 20, snap.Channels[2].Cap)
}

// ─── Concurrent registration ─────────────────────────────────────────────

func TestConcurrentRegistration(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, time.Second)

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ch := make(chan int, 1)
			RegisterChannel(tel, "concurrent", ch)
		}()
	}

	wg.Wait()

	snap := tel.Snapshot()
	assert.Len(t, snap.Channels, n)
}

// ─── Periodic sampling ───────────────────────────────────────────────────

func TestPeriodicSampling(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("sampled", col, 50*time.Millisecond)

	ch := make(chan int, 10)
	RegisterChannel(tel, "ch", ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tel.Start(ctx)

	// Wait for at least 2 snapshots.
	require.Eventually(t, func() bool {
		return col.count() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	tel.Stop()

	snap := col.latest()
	assert.Equal(t, "sampled", snap.PipelineName)
	assert.False(t, snap.Timestamp.IsZero())
	require.Len(t, snap.Channels, 1)
	assert.Equal(t, "ch", snap.Channels[0].Name)
}

func TestStopIdempotent(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tel.Start(ctx)

	// Wait for at least one snapshot.
	require.Eventually(t, func() bool {
		return col.count() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Stop multiple times; must not panic or deadlock.
	tel.Stop()
	tel.Stop()
	tel.Stop()
}

func TestStartContextCancellation(t *testing.T) {
	col := &mockCollector{}
	tel := NewTelemetry("test", col, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	tel.Start(ctx)

	// Wait for at least one snapshot.
	require.Eventually(t, func() bool {
		return col.count() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Cancel the context; the sampler should stop.
	cancel()

	// Stop should return promptly (sampler already exited).
	done := make(chan struct{})
	go func() {
		tel.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after context cancellation")
	}
}

// ─── LogCollector ────────────────────────────────────────────────────────

// testLogHandler records log records for assertion.
type testLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *testLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *testLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *testLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *testLogHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *testLogHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func TestLogCollector_EmitsOneMessagePerChannel(t *testing.T) {
	handler := &testLogHandler{}
	logger := slog.New(handler)

	collector := &LogCollector{
		Logger: logger,
		Level:  slog.LevelInfo,
	}

	snapshot := PipelineSnapshot{
		PipelineName: "test-pipeline",
		Timestamp:    time.Now(),
		Channels: []ChannelSnapshot{
			{Name: "input[0]", Len: 5, Cap: 10, Utilization: 0.5, TotalSent: 100, TotalRecv: 95},
			{Name: "transform", Len: 8, Cap: 10, Utilization: 0.8, TotalSent: -1, TotalRecv: -1},
			{Name: "output[0]", Len: 0, Cap: 5, Utilization: 0.0, TotalSent: 50, TotalRecv: 50},
		},
	}

	collector.OnSnapshot(snapshot)

	assert.Equal(t, 3, handler.count())

	handler.mu.Lock()
	defer handler.mu.Unlock()

	// Verify first record has expected message and level.
	assert.Equal(t, "pipeline_channel_snapshot", handler.records[0].Message)
	assert.Equal(t, slog.LevelInfo, handler.records[0].Level)
}

func TestLogCollector_DefaultLogger(t *testing.T) {
	// Should not panic when Logger is nil (falls back to slog.Default).
	collector := &LogCollector{}
	snapshot := PipelineSnapshot{
		PipelineName: "test",
		Timestamp:    time.Now(),
		Channels:     []ChannelSnapshot{{Name: "ch", Len: 0, Cap: 1}},
	}

	assert.NotPanics(t, func() {
		collector.OnSnapshot(snapshot)
	})
}

func TestLogCollector_OmitsCountersWhenNegative(t *testing.T) {
	handler := &testLogHandler{}
	logger := slog.New(handler)

	collector := &LogCollector{
		Logger: logger,
		Level:  slog.LevelDebug,
	}

	snapshot := PipelineSnapshot{
		PipelineName: "test",
		Timestamp:    time.Now(),
		Channels: []ChannelSnapshot{
			{Name: "ch", Len: 0, Cap: 10, Utilization: 0.0, TotalSent: -1, TotalRecv: -1},
		},
	}

	collector.OnSnapshot(snapshot)

	require.Equal(t, 1, handler.count())

	handler.mu.Lock()
	defer handler.mu.Unlock()

	// Count attributes — total_sent and total_recv should be absent.
	var attrNames []string
	handler.records[0].Attrs(func(a slog.Attr) bool {
		attrNames = append(attrNames, a.Key)
		return true
	})

	assert.NotContains(t, attrNames, "total_sent")
	assert.NotContains(t, attrNames, "total_recv")
}

func TestLogCollector_IncludesCountersWhenPresent(t *testing.T) {
	handler := &testLogHandler{}
	logger := slog.New(handler)

	collector := &LogCollector{
		Logger: logger,
		Level:  slog.LevelInfo,
	}

	snapshot := PipelineSnapshot{
		PipelineName: "test",
		Timestamp:    time.Now(),
		Channels: []ChannelSnapshot{
			{Name: "ch", Len: 0, Cap: 10, Utilization: 0.0, TotalSent: 500, TotalRecv: 499},
		},
	}

	collector.OnSnapshot(snapshot)

	require.Equal(t, 1, handler.count())

	handler.mu.Lock()
	defer handler.mu.Unlock()

	var attrNames []string
	handler.records[0].Attrs(func(a slog.Attr) bool {
		attrNames = append(attrNames, a.Key)
		return true
	})

	assert.Contains(t, attrNames, "total_sent")
	assert.Contains(t, attrNames, "total_recv")
}

// ─── NoopCollector ───────────────────────────────────────────────────────

func TestNoopCollector(t *testing.T) {
	collector := NoopCollector{}
	assert.NotPanics(t, func() {
		collector.OnSnapshot(PipelineSnapshot{
			PipelineName: "test",
			Channels:     []ChannelSnapshot{{Name: "ch"}},
		})
	})
}

// ─── Integration: Pipeline with telemetry ────────────────────────────────

func TestPipelineTelemetry_EndToEnd(t *testing.T) {
	col := &mockCollector{}

	p, _, err := NewPipeline(context.Background(), Config[int, int]{
		Name:             "e2e-test",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		MetricsCollector: col,
		MetricsInterval:  50 * time.Millisecond,
		Composer: func(c Composer[int, int]) error {
			ctx := c.Context()
			return c.Outputs().Link(ctx, 0, c.Inputs().At(0))
		},
	})
	require.NoError(t, err)

	// Verify telemetry is non-nil.
	require.NotNil(t, p.Telemetry())

	// Send some data.
	inputs := p.Inputs()
	for i := 0; i < 5; i++ {
		inputs.Send(context.Background(), 0, i)
	}
	p.CloseAllInputs()

	// Consume output.
	outputs := p.Outputs()
	var results []int
	for v := range outputs.SinkAtIter(context.Background(), 0) {
		results = append(results, v)
	}

	err = p.Wait()
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, results)

	// Verify that at least one snapshot was taken with the pipeline channels.
	// The pipeline registers input[0], output[0], and link[0].
	snap := p.Telemetry().Snapshot()
	assert.Equal(t, "e2e-test", snap.PipelineName)

	// At minimum we should see input[0], output[0], and link[0].
	channelNames := make([]string, len(snap.Channels))
	for i, ch := range snap.Channels {
		channelNames[i] = ch.Name
	}
	assert.Contains(t, channelNames, "input[0]")
	assert.Contains(t, channelNames, "output[0]")
	assert.Contains(t, channelNames, "link[0]")
}

func TestPipelineTelemetry_Disabled(t *testing.T) {
	p, _, err := NewPipeline(context.Background(), Config[int, int]{
		Name:            "no-telemetry",
		InputBufferSize: 5,
		Composer: func(c Composer[int, int]) error {
			ctx := c.Context()
			return c.Outputs().Link(ctx, 0, c.Inputs().At(0))
		},
	})
	require.NoError(t, err)
	assert.Nil(t, p.Telemetry())

	p.CloseAllInputs()
	err = p.Wait()
	require.NoError(t, err)
}
