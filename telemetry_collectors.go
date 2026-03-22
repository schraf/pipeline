package pipeline

import (
	"context"
	"log/slog"
)

// LogCollector writes structured telemetry snapshots using slog. Each
// snapshot produces one log message per registered channel, making it
// straightforward to filter and alert on individual channels in log
// aggregation systems.
type LogCollector struct {
	// Logger is the slog.Logger used to emit telemetry messages.
	// When nil, slog.Default() is used.
	Logger *slog.Logger

	// Level is the log level for telemetry messages. The zero value
	// corresponds to slog.LevelInfo.
	Level slog.Level
}

// OnSnapshot logs one structured message per channel in the snapshot.
func (c *LogCollector) OnSnapshot(snapshot PipelineSnapshot) {
	logger := c.Logger
	if logger == nil {
		logger = slog.Default()
	}

	for _, ch := range snapshot.Channels {
		attrs := []slog.Attr{
			slog.String("pipeline", snapshot.PipelineName),
			slog.Time("sample_timestamp", snapshot.Timestamp),
			slog.String("channel", ch.Name),
			slog.Bool("unbuffered", ch.Cap == 0),
			slog.Int("len", ch.Len),
			slog.Int("cap", ch.Cap),
			slog.Float64("utilization", ch.Utilization),
		}

		if ch.TotalSent >= 0 {
			attrs = append(attrs, slog.Int64("total_sent", ch.TotalSent))
		}

		if ch.TotalRecv >= 0 {
			attrs = append(attrs, slog.Int64("total_recv", ch.TotalRecv))
		}

		logger.LogAttrs(context.Background(), c.Level, "pipeline_channel_snapshot", attrs...)
	}
}

// NoopCollector silently discards all telemetry snapshots. It is useful as a
// placeholder or for benchmarking the overhead of the telemetry registry
// without any I/O.
type NoopCollector struct{}

// OnSnapshot discards the snapshot.
func (NoopCollector) OnSnapshot(PipelineSnapshot) {}
