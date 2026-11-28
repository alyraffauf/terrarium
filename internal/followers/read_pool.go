package followers

import (
	"context"
	"sync"
	"time"
)

// ReadPool runs bounded public reads alongside a single writer. The writer
// calls its methods and owns each DID until Next returns that read's result.
type ReadPool[T any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	busy    map[string]bool
	results chan readResult[T]
	workers sync.WaitGroup
}

type readResult[T any] struct {
	did   string
	value T
}

func NewReadPool[T any](ctx context.Context, limit int) *ReadPool[T] {
	ctx, cancel := context.WithCancel(ctx)
	return &ReadPool[T]{ctx: ctx, cancel: cancel, busy: map[string]bool{}, results: make(chan readResult[T], limit)}
}

func (p *ReadPool[T]) Free() int            { return cap(p.results) - len(p.busy) }
func (p *ReadPool[T]) Busy(did string) bool { return p.busy[did] }

func (p *ReadPool[T]) Start(did string, read func(context.Context) T) bool {
	if p.Free() == 0 || p.busy[did] {
		return false
	}
	p.busy[did] = true
	p.workers.Go(func() { p.results <- readResult[T]{did, read(p.ctx)} })
	return true
}

// Next waits up to wait so the writer can run periodic maintenance even if
// every outstanding read is stalled.
func (p *ReadPool[T]) Next(ctx context.Context, wait time.Duration) (T, bool) {
	select {
	case result := <-p.results:
		delete(p.busy, result.did)
		return result.value, true
	case <-ctx.Done():
	case <-time.After(wait):
	}
	var zero T
	return zero, false
}

func (p *ReadPool[T]) Close() { p.cancel(); p.workers.Wait() }
