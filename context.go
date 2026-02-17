package pipeline

import (
	"context"
	"sync"
)

type Context struct {
	parent context.Context
	group  *sync.WaitGroup
	err    func(error)
}
