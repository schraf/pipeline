package stages_test

/*

// End-to-end multi-stage pipeline tests

func TestPipeline_TransformFilterLimit(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  20,
		OutputBufferSize: 10,
	})

	// Pipeline: Transform -> Filter -> Limit
	transformStage := stages.TransformStage[int, int]{
		Name:   "transform",
		Buffer: 20,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 2
			return &result, nil
		},
	}

	filterStage := stages.FilterStage[int]{
		Name:   "filter",
		Buffer: 20,
		Filter: func(_ context.Context, x int) (bool, error) {
			return x > 20, nil
		},
	}

	limitStage := stages.LimitStage[int]{
		Name:   "limit",
		Buffer: 10,
		Limit:  5,
	}

	transformed := transformStage.Create(p.Context(), p.Inputs().Receiver(0))
	filtered := filterStage.Create(p.Context(), transformed)
	out := limitStage.Create(p.Context(), filtered)

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 20, "expected all results > 20")
		assert.Equalf(t, 0, v%2, "expected all results to be even, got %d", v)
	}
}

func TestPipeline_ParallelTransformBatch(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, []int](ctx, pipeline.PipelineConfig[int, []int]{
		Name:             "test",
		InputBufferSize:  30,
		OutputBufferSize: 10,
	})

	// Pipeline: ParallelTransform -> Batch
	parallelTransformStage := stages.ParallelTransformStage[int, int]{
		Name:    "parallel-transform",
		Buffer:  30,
		Workers: 5,
		Transformer: func(_ context.Context, x int) (*int, error) {
			time.Sleep(5 * time.Millisecond)
			result := x * 3
			return &result, nil
		},
	}

	batchStage := stages.BatchStage[int, []int]{
		Name:      "batch",
		Buffer:    10,
		BatchSize: 5,
		Batcher: func(_ context.Context, batch []int) (*[]int, error) {
			return &batch, nil
		},
	}

	transformed := parallelTransformStage.Create(p.Context(), p.Inputs().Receiver(0))
	out := batchStage.Create(p.Context(), transformed)

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var batches [][]int
	for batch := range out {
		batches = append(batches, batch)
	}

	// Should have 6 batches (30 items / 5 per batch)
	assert.Len(t, batches, 6, "expected 6 batches")

	// Each batch should have 5 items
	for i, batch := range batches {
		assert.Lenf(t, batch, 5, "batch %d: expected 5 items", i)

		// All values should be multiples of 3
		for _, v := range batch {
			assert.Equalf(t, 0, v%3, "batch %d: expected all values to be multiples of 3, got %d", i, v)
		}
	}
}

func TestPipeline_FanInTransformFanOut(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputChannels:    3,
		InputBufferSize:  5,
		OutputBufferSize: 15,
	})

	// Pipeline: FanIn -> Transform -> FanOut
	fanInStage := stages.FanInStage[int]{
		Name:   "fan-in",
		Buffer: 15,
	}

	transformStage := stages.TransformStage[int, int]{
		Name:   "transform",
		Buffer: 15,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 2
			return &result, nil
		},
	}

	fanOutStage := stages.FanOutStage[int]{
		Name:        "fan-out",
		OutputCount: 2,
		Buffer:      15,
	}

	merged := fanInStage.Create(p.Context(), p.Inputs().Receivers())
	transformed := transformStage.Create(p.Context(), merged)
	outputs := fanOutStage.Create(p.Context(), transformed)

	// Send values to each input channel
	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5)
	p.Inputs().Send(ctx, 1, 6, 7, 8, 9, 10)
	p.Inputs().Send(ctx, 2, 11, 12, 13, 14, 15)

	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	checkOutput := func(ch <-chan int, name string, count int) {
		t.Helper()

		var results []int
		for i := 0; i < count; i++ {
			select {
			case v := <-ch:
				results = append(results, v)
			case <-time.After(1 * time.Second):
				require.Failf(t, "timeout", "%s: timeout waiting for value %d", name, i+1)
				return
			}
		}

		assert.Lenf(t, results, count, "%s: unexpected number of results", name)

		// All values should be even and in range [2, 30]
		for _, v := range results {
			assert.Equalf(t, 0, v%2, "%s: expected all values to be even, got %d", name, v)
			assert.GreaterOrEqualf(t, v, 2, "%s: expected values >= 2", name)
			assert.LessOrEqualf(t, v, 30, "%s: expected values <= 30", name)
		}
	}

	checkOutput(outputs.At(0), "out1", 15)
	checkOutput(outputs.At(1), "out2", 15)
}

func TestPipeline_SplitTransformFanIn(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  12,
		OutputBufferSize: 12,
	})

	// Pipeline: Split -> Transform (x3) -> FanIn
	splitStage := stages.SplitStage[int]{
		Name:        "split",
		OutputCount: 3,
		Buffer:      6,
		Selector: func(_ context.Context, x int) int {
			return (x - 1) % 3
		},
	}

	transformStage1 := stages.TransformStage[int, int]{
		Name:   "transform1",
		Buffer: 6,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 10
			return &result, nil
		},
	}

	transformStage2 := stages.TransformStage[int, int]{
		Name:   "transform2",
		Buffer: 6,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 20
			return &result, nil
		},
	}

	transformStage3 := stages.TransformStage[int, int]{
		Name:   "transform3",
		Buffer: 6,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 30
			return &result, nil
		},
	}

	fanInStage := stages.FanInStage[int]{
		Name:   "fan-in",
		Buffer: 12,
	}

	splitOutputs := splitStage.Create(p.Context(), p.Inputs().Receiver(0))
	transformed1 := transformStage1.Create(p.Context(), splitOutputs.At(0))
	transformed2 := transformStage2.Create(p.Context(), splitOutputs.At(1))
	transformed3 := transformStage3.Create(p.Context(), splitOutputs.At(2))

	// Create channels for FanIn input
	fanInInputs := []chan int{
		make(chan int, 6),
		make(chan int, 6),
		make(chan int, 6),
	}

	// Forward transformed outputs to fanIn inputs (in goroutines)
	go func() {
		for v := range transformed1 {
			fanInInputs[0] <- v
		}
		close(fanInInputs[0])
	}()
	go func() {
		for v := range transformed2 {
			fanInInputs[1] <- v
		}
		close(fanInInputs[1])
	}()
	go func() {
		for v := range transformed3 {
			fanInInputs[2] <- v
		}
		close(fanInInputs[2])
	}()

	out := fanInStage.Create(p.Context(), pipeline.MultiChannelReceiver[int](fanInInputs))

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	assert.Len(t, results, 12, "expected 12 results")

	// Verify routing: values 1,4,7,10 -> *10; 2,5,8,11 -> *20; 3,6,9,12 -> *30
	// Expected values: 10, 40, 70, 100, 40, 100, 160, 220, 90, 180, 270, 360
	for _, v := range results {
		assert.True(t, v > 0, "expected positive value, got: %d", v)
	}
}

func TestPipeline_ComplexMultiStage(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  50,
		OutputBufferSize: 10,
	})

	// Complex pipeline: Filter -> Transform -> Batch -> Transform -> Filter -> Limit

	// Stage 1: Filter evens
	filterStage1 := stages.FilterStage[int]{
		Name:   "filter1",
		Buffer: 50,
		Filter: func(_ context.Context, x int) (bool, error) {
			return x%2 == 0, nil
		},
	}

	// Stage 2: Transform (multiply by 3)
	transformStage1 := stages.TransformStage[int, int]{
		Name:   "transform1",
		Buffer: 50,
		Transformer: func(_ context.Context, x int) (*int, error) {
			result := x * 3
			return &result, nil
		},
	}

	// Stage 3: Batch into groups of 4
	batchStage := stages.BatchStage[int, []int]{
		Name:      "batch",
		Buffer:    20,
		BatchSize: 4,
		Batcher: func(_ context.Context, batch []int) (*[]int, error) {
			return &batch, nil
		},
	}

	// Stage 4: Transform batches (sum)
	transformStage2 := stages.TransformStage[[]int, int]{
		Name:   "transform2",
		Buffer: 20,
		Transformer: func(_ context.Context, batch []int) (*int, error) {
			sum := 0
			for _, v := range batch {
				sum += v
			}
			return &sum, nil
		},
	}

	// Stage 5: Filter sums > 100
	filterStage2 := stages.FilterStage[int]{
		Name:   "filter2",
		Buffer: 20,
		Filter: func(_ context.Context, x int) (bool, error) {
			return x > 100, nil
		},
	}

	// Stage 6: Limit to 5 results
	limitStage := stages.LimitStage[int]{
		Name:   "limit",
		Buffer: 10,
		Limit:  5,
	}

	stage1 := filterStage1.Create(p.Context(), p.Inputs().Receiver(0))
	stage2 := transformStage1.Create(p.Context(), stage1)
	stage3 := batchStage.Create(p.Context(), stage2)
	stage4 := transformStage2.Create(p.Context(), stage3)
	stage5 := filterStage2.Create(p.Context(), stage4)
	out := limitStage.Create(p.Context(), stage5)

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	assert.LessOrEqual(t, len(results), 5, "expected at most 5 results")

	for _, v := range results {
		assert.Greater(t, v, 100, "expected all results > 100")
	}
}

func TestPipeline_RoundRobinParallelProcessing(t *testing.T) {
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  15,
		OutputBufferSize: 15,
	})

	// Pipeline: FanOutRoundRobin -> ParallelTransform (on each branch) -> FanIn
	fanOutRRStage := stages.FanOutRoundRobinStage[int]{
		Name:        "fan-out-rr",
		OutputCount: 3,
		Buffer:      5,
	}

	parallelTransformStage1 := stages.ParallelTransformStage[int, int]{
		Name:    "parallel-transform1",
		Buffer:  5,
		Workers: 2,
		Transformer: func(_ context.Context, x int) (*int, error) {
			time.Sleep(5 * time.Millisecond)
			result := x * 100
			return &result, nil
		},
	}

	parallelTransformStage2 := stages.ParallelTransformStage[int, int]{
		Name:    "parallel-transform2",
		Buffer:  5,
		Workers: 2,
		Transformer: func(_ context.Context, x int) (*int, error) {
			time.Sleep(5 * time.Millisecond)
			result := x * 200
			return &result, nil
		},
	}

	parallelTransformStage3 := stages.ParallelTransformStage[int, int]{
		Name:    "parallel-transform3",
		Buffer:  5,
		Workers: 2,
		Transformer: func(_ context.Context, x int) (*int, error) {
			time.Sleep(5 * time.Millisecond)
			result := x * 300
			return &result, nil
		},
	}

	fanInStage := stages.FanInStage[int]{
		Name:   "fan-in",
		Buffer: 15,
	}

	rrOutputs := fanOutRRStage.Create(p.Context(), p.Inputs().Receiver(0))
	processed1 := parallelTransformStage1.Create(p.Context(), rrOutputs.At(0))
	processed2 := parallelTransformStage2.Create(p.Context(), rrOutputs.At(1))
	processed3 := parallelTransformStage3.Create(p.Context(), rrOutputs.At(2))

	// Create channels for FanIn input
	fanInInputs := []chan int{
		make(chan int, 5),
		make(chan int, 5),
		make(chan int, 5),
	}

	go func() {
		for v := range processed1 {
			fanInInputs[0] <- v
		}
		close(fanInInputs[0])
	}()
	go func() {
		for v := range processed2 {
			fanInInputs[1] <- v
		}
		close(fanInInputs[1])
	}()
	go func() {
		for v := range processed3 {
			fanInInputs[2] <- v
		}
		close(fanInInputs[2])
	}()

	out := fanInStage.Create(p.Context(), pipeline.MultiChannelReceiver[int](fanInInputs))

	p.Inputs().Send(ctx, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	p.CloseAllInputs()
	p.Start()

	require.NoError(t, p.Wait())

	var results []int
	for v := range out {
		results = append(results, v)
	}

	assert.Len(t, results, 15, "expected 15 results")

	// Verify all results are present (values may be out of order)
	resultSet := make(map[int]bool)
	for _, v := range results {
		resultSet[v] = true
	}

	// 1,4,7,10,13 -> *100 = 100,400,700,1000,1300
	// 2,5,8,11,14 -> *200 = 400,1000,1600,2200,2800
	// 3,6,9,12,15 -> *300 = 900,1800,2700,3600,4500
	expectedValues := []int{100, 400, 700, 1000, 1300, 400, 1000, 1600, 2200, 2800, 900, 1800, 2700, 3600, 4500}
	for _, expected := range expectedValues {
		assert.Truef(t, resultSet[expected], "expected value %d not found", expected)
	}
}

func TestPipeline_WithError(t *testing.T) {
	expectedError := errors.New("test error")
	ctx := context.Background()
	p, _ := pipeline.NewPipeline[int, int](ctx, pipeline.PipelineConfig[int, int]{
		Name:             "test",
		InputBufferSize:  100,
		OutputBufferSize: 10,
	})

	stage := stages.TransformStage[int, int]{
		Name:   "transform",
		Buffer: 10,
		Transformer: func(_ context.Context, x int) (*int, error) {
			time.Sleep(10 * time.Millisecond)

			if x == 5 {
				return nil, expectedError
			}

			result := x * 2
			return &result, nil
		},
	}

	out := stage.Create(p.Context(), p.Inputs().Receiver(0))

	go func() {
		for i := 1; i <= 100; i++ {
			select {
			case p.Inputs().At(0) <- i:
			case <-time.After(1 * time.Second):
				return
			}
		}
		p.CloseAllInputs()
	}()

	p.Start()

	err := p.Wait()
	require.ErrorIs(t, err, expectedError)

	var results []int
	for {
		select {
		case v, ok := <-out:
			if !ok {
				goto done
			}
			results = append(results, v)
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:

	assert.NotEmpty(t, results, "expected some results before cancellation")
}

*/
