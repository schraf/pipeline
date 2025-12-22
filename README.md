# pipeline

A Go package for building concurrent data processing pipelines using channels.

## Overview

`pipeline` provides a set of composable stages for processing data streams concurrently. It handles context cancellation, error propagation, and goroutine lifecycle management automatically.

## Installation

```bash
go get github.com/schraf/pipeline
```

## Usage

### Basic Example

```go
package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/schraf/pipeline"
)

func main() {
	p, _ := pipeline.WithPipeline(context.Background())

	// Define some data to process
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Create channels
	in := make(chan int, len(data))
	out := make(chan int, len(data))

	// Source: feed data into the pipeline from a slice
	pipeline.SourceSlice(p, slices.Values(data), in)

	// Transform: multiply by 2
	pipeline.Transform(p, func(ctx context.Context, x int) (*int, error) {
		result := x * 2
		return &result, nil
	}, in, out)

	// Wait for pipeline to complete
	if err := p.Wait(); err != nil {
		panic(err)
	}

	// Read results
	for v := range out {
		fmt.Println(v)
	}
}
```

## Pipeline Stages

### Source

Starts a pipeline from a fallible iterator (`iter.Seq2[T, error]`). If the iterator returns an error, the pipeline is cancelled.

```go
// Example: create a custom iterator that reads from a file or database
// and can return an error.
var myIterator iter.Seq2[string, error] 

out := make(chan string, 100)
pipeline.Source(p, myIterator, out)
```

### SourceSlice

Starts a pipeline from a simple, error-free iterator (`iter.Seq[T]`), which is useful for in-memory slices.

```go
data := []int{1, 2, 3, 4, 5}
out := make(chan int, 5)

// The iterator can be created from a slice using slices.Values
pipeline.SourceSlice(p, slices.Values(data), out)
```

### Transform

Applies a transformation function to each value:

```go
pipeline.Transform(p, func(ctx context.Context, x int) (*int, error) {
    result := x * 2
    return &result, nil
}, in, out)
```

### Filter

Filters values based on a predicate:

```go
pipeline.Filter(p, func(ctx context.Context, x int) (bool, error) {
    return x%2 == 0, nil
}, in, out)
```

### Batch

Groups values into fixed-size batches:

```go
pipeline.Batch(p, func(ctx context.Context, batch []int) (*int, error) {
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
pipeline.ParallelTransform(p, 5, func(ctx context.Context, x int) (*int, error) {
    result := x * 2
    return &result, nil
}, in, out)
```

### FanIn

Merges multiple input channels into one:

```go
pipeline.FanIn(p, out, in1, in2, in3)
```

### FanOut

Distributes values to multiple output channels (broadcast):

```go
pipeline.FanOut(p, in, out1, out2, out3)
```

### FanOutRoundRobin

Distributes values round-robin style:

```go
pipeline.FanOutRoundRobin(p, in, out1, out2, out3)
```

### Limit

Limits the number of values passed through:

```go
pipeline.Limit(p, 10, in, out)
```

### Split

Routes values to different channels based on a selector:

```go
pipeline.Split(p, func(ctx context.Context, x int) int {
    return (x - 1) % 3
}, in, out1, out2, out3)
```

### Aggregate

Collects all values into a single slice:

```go
pipeline.Aggregate(p, in, out)
```

## Error Handling

The pipeline automatically cancels all stages when an error occurs. The first error encountered is returned by `Wait()`:

```go
if err := p.Wait(); err != nil {
    log.Fatal(err)
}
```

## Requirements

- Go 1.24.0 or later

## License

See LICENSE file for details.
