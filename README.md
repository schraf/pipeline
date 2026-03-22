# pipeline

A Go package for building concurrent data processing pipelines using channels.

## Overview

`pipeline` provides a set of composable stages for processing data streams
concurrently. It handles context cancellation, error propagation, and goroutine
lifecycle management automatically.

Stages are defined as structs in the `stages` package. Each stage has a
`Create` method that integrates it into a pipeline by connecting input and
output channels.

The package uses Go's `runtime/trace` to create trace regions for each stage,
allowing for detailed performance analysis.

## Installation

```bash
go get github.com/schraf/pipeline/v4
```

## Usage

### Basic Example

To create a pipeline, you define a `Config` and use `NewPipeline`. You then
connect stages by calling their `Create` methods inside the `Composer` function.

```go
package main

import (
	"context"
	"fmt"

	"github.com/schraf/pipeline/v4"
	"github.com/schraf/pipeline/v4/stages"
)

func main() {
	ctx := context.Background()

	// Transform stage: multiply by 2
	transformStage := stages.TransformStage[int, int]{
		Name:   "multiply",
		Buffer: 10,
		Transformer: func(ctx context.Context, x int) (*int, error) {
			result := x * 2
			return &result, nil
		},
	}

    // Configure the pipeline
	cfg := pipeline.Config[int, int]{
		Name:             "example",
		InputBufferSize:  10,
		OutputBufferSize: 10,
		Composer: func(composer pipeline.Composer[int, int]) {
			ctx := composer.Context()
			inputs := composer.Inputs()
			outputs := composer.Outputs()

			out := transformStage.Create(ctx, inputs.At(0))

			outputs.Link(ctx, 0, out)
		},
	}

	pipe, _ := pipeline.NewPipeline(ctx, cfg)

	pipe.Start()

	// Feed data into the pipeline
	go func() {
		defer pipe.CloseAllInputs()
		for i := 1; i <= 5; i++ {
			pipe.Inputs().Send(ctx, 0, i)
		}
	}()

	// Consume results from the last stage's output channel
	for v := range pipe.Outputs().SinkAtIter(ctx, 0) {
		fmt.Println(v)
	}

	// Wait for completion and check for errors
	if err := pipe.Wait(); err != nil {
		panic(err)
	}
}
```

## Pipeline Configuration

The `Config` struct specifies the pipeline's basic parameters:

- `Name`: Used for tracing and identification.
- `InputChannels`: Number of input channels to create (defaults to 1).
- `InputBufferSize`: Buffer size for each input channel.
- `OutputChannels`: Number of output channels to create (defaults to 1).
- `OutputBufferSize`: Buffer size for each output channel.
- `Composer`: Function used to connect the pipeline inputs to outputs.

## Pipeline Stages

Stages are located in the `github.com/schraf/pipeline/v4/stages` package.

### Transform

Applies a transformation function to each value:

```go
stage := stages.TransformStage[int, int]{
    Name:   "transform",
    Buffer: 10,
    Transformer: func(ctx context.Context, x int) (*int, error) {
        result := x * 2
        return &result, nil
    },
}
out := stage.Create(ctx, in)
```

### Filter

Filters values based on a predicate:

```go
stage := stages.FilterStage[int]{
    Name:   "filter",
    Buffer: 10,
    Filter: func(ctx context.Context, x int) (bool, error) {
        return x%2 == 0, nil
    },
}
out := stage.Create(ctx, in)
```

### ParallelTransform

Applies transformation with multiple concurrent workers:

```go
stage := stages.ParallelTransformStage[int, int]{
    Name:    "parallel",
    Buffer:  10,
    Workers: 5,
    Transformer: func(ctx context.Context, x int) (*int, error) {
        return &x, nil
    },
}
out := stage.Create(ctx, in)
```

### Batch

Groups values into fixed-size batches:

```go
stage := stages.BatchStage[int, []int]{
    Name:      "batch",
    Buffer:    10,
    BatchSize: 5,
}
out := stage.Create(ctx, in)
```

### FanIn

Merges multiple input channels into one:

```go
stage := stages.FanInStage[int]{
    Name:   "fan-in",
    Buffer: 10,
}
out := stage.Create(ctx, multiIn) // multiIn is a MultiChannelReceiver
```

### FanOut

Distributes values to multiple output channels (broadcast):

```go
stage := stages.FanOutStage[int]{
    Name:        "fan-out",
    OutputCount: 3,
    Buffer:      10,
}
outputs := stage.Create(ctx, in) // Returns MultiChannelReceiver
```

### FanOutRoundRobin

Distributes values to multiple output channels in a round-robin fashion:

```go
stage := stages.FanOutRoundRobinStage[int]{
    Name:        "fan-out-rr",
    OutputCount: 3,
    Buffer:      10,
}
outputs := stage.Create(ctx, in)
```

### Split

Routes values to different channels based on a selector:

```go
stage := stages.SplitStage[int]{
    Name:        "split",
    OutputCount: 3,
    Buffer:      10,
    Selector: func(ctx context.Context, x int) int {
        return x % 3
    },
}
outputs := stage.Create(ctx, in)
```

### Reduce

Processes values incrementally using a reducer function:

```go
stage := stages.ReduceStage[int, int]{
    Name:         "reduce",
    Buffer:       1,
    InitialValue: 0,
    Reducer: func(ctx context.Context, acc int, x int) (int, error) {
        return acc + x, nil
    },
}
out := stage.Create(ctx, in)
```

### WindowedReduce

Processes values incrementally allowing intermediate results and state resets:

```go
stage := stages.WindowedReduceStage[int, int]{
    Name:    "windowed-reduce",
    Buffer:  1,
    Initial: 0,
    Reducer: func(ctx context.Context, acc int, val int) (int, int, bool, error) {
        acc += val
        // Emit and reset when sum >= 10
        if acc >= 10 {
            return 0, acc, true, nil
        }
        return acc, 0, false, nil
    },
}
out := stage.Create(ctx, in)
```

### Flatten

Takes an input channel of slices and emits each element individually:

```go
stage := stages.FlattenStage[int]{
    Name:   "flatten",
    Buffer: 10,
}
out := stage.Create(ctx, inSlices)
```

### Limit

Limits the number of values passed through the stage:

```go
stage := stages.LimitStage[int]{
    Name:   "limit",
    Buffer: 10,
    Limit:  5,
}
out := stage.Create(ctx, in)
```

### Expand

Lazily expands single input items into multiple items using an iterator:

```go
stage := stages.ExpandStage[int, int]{
    Name:   "expand",
    Buffer: 10,
    Expander: func(ctx context.Context, x int) iter.Seq2[int, error] {
        return func(yield func(int, error) bool) {
            yield(x, nil)
            yield(x*10, nil)
        }
    },
}
out := stage.Create(ctx, in)
```

### Aggregate

Collects all values into a single slice:

```go
stage := stages.AggregateStage[int]{
    Name:   "aggregate",
    Buffer: 1,
}
out := stage.Create(ctx, in) // out is <-chan []int
```

## Telemetry

Pipelines support opt-in telemetry for monitoring channel throughput and buffer utilization, useful for identifying bottlenecks. Enable it by setting `MetricsCollector` in the pipeline config:

```go
cfg := pipeline.Config[int, int]{
    Name:            "example",
    InputBufferSize: 50,
    MetricsCollector: &pipeline.LogCollector{
        Logger: slog.New(slog.NewJSONHandler(os.Stderr, nil)),
        Level:  slog.LevelInfo,
    },
    MetricsInterval: 500 * time.Millisecond,
    Composer: func(c pipeline.Composer[int, int]) error {
        // stages automatically register their channels
        out := transformStage.Create(c.Context(), c.Inputs().At(0))
        return c.Outputs().Link(c.Context(), 0, out)
    },
}
```

The `LogCollector` emits one structured `slog` message per channel per tick with `len`, `cap`, `utilization`, and throughput counters. A consistently full channel upstream of a stage with an empty channel downstream indicates a bottleneck.

Implement the `MetricsCollector` interface to send telemetry to a custom backend:

```go
type MetricsCollector interface {
    OnSnapshot(PipelineSnapshot)
}
```

When `MetricsCollector` is nil (the default), no telemetry code runs and there is zero overhead.

## Error Handling

The pipeline automatically cancels all stages when an error occurs. The first error encountered is captured and returned by `Wait()`:

```go
if err := p.Wait(); err != nil {
    log.Fatal(err)
}
```

## Requirements

- Go 1.24.0 or later

## License

See LICENSE file for details.
