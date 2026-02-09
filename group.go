package pipeline

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

type PipelineGroup[In any, Out any] struct {
	pipelines []*Pipeline[In, Out]
	lock      sync.Mutex
}

func NewPipelineGroup[In any, Out any]() *PipelineGroup[In, Out] {
	return &PipelineGroup[In, Out]{}
}

func (g *PipelineGroup[In, Out]) Add(pipelines ...*Pipeline[In, Out]) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.pipelines = append(g.pipelines, pipelines...)
}

func (g *PipelineGroup[In, Out]) Start() error {
	g.lock.Lock()
	defer g.lock.Unlock()

	for _, pipeline := range g.pipelines {
		if err := pipeline.Start(); err != nil {
			return fmt.Errorf("failed to start all pipelines in group: %w", err)
		}
	}

	return nil
}

func (g *PipelineGroup[In, Out]) Wait(ctx context.Context) error {
	g.lock.Lock()
	defer g.lock.Unlock()

	group, ctx := errgroup.WithContext(ctx)

	for _, pipeline := range g.pipelines {
		group.Go(func() error {
			return pipeline.Wait()
		})
	}

	return group.Wait()
}
