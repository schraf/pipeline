# pipeline

A Go package for building concurrent data processing pipelines using channels.

## Overview

`pipeline` provides a set of composable stages for processing data streams
concurrently. It handles context cancellation, error propagation, and goroutine
lifecycle management automatically.

All pipeline stages require a name parameter as their first argument. These
names are used to create trace regions for performance analysis using Go's
`runtime/trace` package.

## Installation

```bash
go get github.com/schraf/pipeline
```

## Usage

### Basic Example

To create a pipeline, you define a `PipelineConfig` which specifies the
pipeline's name, buffer sizes, and an `Executor` function. The `Executor` is
where you connect your pipeline stages.

```go
package main

import (
	"context"
	"fmt"

	"github.com/schraf/pipeline"
)

func main() {
	// Define the pipeline configuration
	cfg := pipeline.PipelineConfig[int, int]{
		Name:            "example",
		InputChannels:   1,
		OutputChannels:  1,
		Executor: func(params *pipeline.PipelineParameters[int, int]) {
			pipe, in, out := params.Pipe, params.In, params.Out
			// Connect input to output through a Transform stage
			pipeline.Transform("multiply", pipe, func(ctx context.Context, x int) (*int, error) {
				result := x * 2
				return &result, nil
			}, in.At(0), out.At(0))
		},
	}

	// Create the pipeline
	p, _ := pipeline.NewPipeline(context.Background(), cfg)

	// Start processing
	p.Start()

	// Feed data into the pipeline
	go func() {
		defer p.CloseAllInputs()
		for i := 1; i <= 10; i++ {
			p.Inputs().At(0) <- i
		}
	}()

	// Consume results
	for v := range p.Outputs().At(0) {
		fmt.Println(v)
	}

	// Wait for completion and check for errors
	if err := p.Wait(); err != nil {
		panic(err)
	}
}
```

### Chaining Pipelines

You can connect a pipeline to one or more downstream pipelines using the `Chain`
function. This connects the outputs of the previous pipeline to the inputs of
the new pipelines automatically. If multiple configurations are provided, the
data is fanned out to all downstream pipelines.

```go
// Create first pipeline (e.g., generates or transforms data)
p1, _ := pipeline.NewPipeline(ctx, cfg1)

// Chain two pipelines to the first one.
// The output of p1 will be broadcast to both p2 and p3.
// Note: Chain uses p1's context for the new pipelines.
downstreamPipelines, err := pipeline.Chain(p1, cfg2, cfg3)
if err != nil {
    panic(err)
}

// Start all pipelines
p1.Start()
for _, p := range downstreamPipelines {
    p.Start()
}

// Feed p1 and consume from p2/p3 (via downstreamPipelines)
// ...
```

### Pipeline Groups

The `PipelineGroup` struct allows you to manage a collection of pipelines with
the same input and output types. It provides methods to add, start, and wait for
multiple pipelines as a single unit.

```go
// Create a group for the downstream pipelines
group := pipeline.NewPipelineGroup[int, int]()

// Add pipelines to the group
group.Add(downstreamPipelines...)

// Start all pipelines in the group
if err := group.Start(); err != nil {
    panic(err)
}

// Wait for all pipelines in the group to complete
if err := group.Wait(ctx); err != nil {
    panic(err)
}
```

## Pipeline Stages

Stages are designed to be used inside the `Executor` function of your
`PipelineConfig`. The `pipe` argument provided to the executor is passed to
each stage.

### Transform

Applies a transformation function to each value:

```go
pipeline.Transform("transform", pipe, func(ctx context.Context, x int) (*int, error) {
    result := x * 2
    return &result, nil
}, in, out)
```

### Filter

Filters values based on a predicate:

```go
pipeline.Filter("filter", pipe, func(ctx context.Context, x int) (bool, error) {
    return x%2 == 0, nil
}, in, out)
```

### Batch

Groups values into fixed-size batches:

```go
pipeline.Batch("batch", pipe, func(ctx context.Context, batch []int) (*int, error) {
    sum := 0
    for _, v := range batch {
        sum += v
    }
    return &sum, nil
}, 3, in, out)
```

### ParallelTransform

Applies transformation with concurrent workers:

```go
pipeline.ParallelTransform("parallel-transform", pipe, 5, func(ctx context.Context, x int) (*int, error) {
    result := x * 2
    return &result, nil
}, in, out)
```

### FanIn

Merges multiple input channels into one:

```go
pipeline.FanIn("fan-in", pipe, out, in1, in2, in3)
```

### FanOut

Distributes values to multiple output channels (broadcast):

```go
pipeline.FanOut("fan-out", pipe, in, out1, out2, out3)
```

### FanOutRoundRobin

Distributes values round-robin style:

```go
pipeline.FanOutRoundRobin("fan-out-round-robin", pipe, in, out1, out2, out3)
```

### Limit

Limits the number of values passed through:

```go
pipeline.Limit("limit", pipe, 10, in, out)
```

### Split

Routes values to different channels based on a selector:

```go
pipeline.Split("split", pipe, func(ctx context.Context, x int) int {
    return (x - 1) % 3
}, in, out1, out2, out3)
```

### Aggregate

Collects all values into a single slice:

```go
pipeline.Aggregate("aggregate", pipe, in, out)
```

### Reduce

Processes values incrementally using a reducer function, combining them with an
accumulator. This allows aggregating results as they come in without keeping
all values in memory:

```go
pipeline.Reduce("reduce", pipe, 0, func(ctx context.Context, acc int, x int) (int, error) {
    return acc + x, nil
}, in, out)
```

### Flatten

Takes an input channel of slices and emits each element of each slice as an
individual item on the output channel:

```go
pipeline.Flatten("flatten", pipe, in, out)
```

### Expand

Takes single input items from a channel and for each input, outputs multiple
items of another type. The expander function returns an iterator
(`iter.Seq2[Out, error]`) of output items for each input, allowing for lazy
evaluation and avoiding loading all expanded items into memory at once:

```go
pipeline.Expand("expand", pipe, func(ctx context.Context, x int) iter.Seq2[string, error] {
    return func(yield func(string, error) bool) {
        yield(fmt.Sprintf("%d", x), nil)
        yield(fmt.Sprintf("%d", x*2), nil)
    }
}, in, out)
```

## Error Handling

The pipeline automatically cancels all stages when an error occurs. The first
error encountered is returned by `Wait()`:

```go
if err := p.Wait(); err != nil {
    log.Fatal(err)
}
```

## Requirements

- Go 1.24.0 or later

## License

See LICENSE file for details.
