package palustre

import (
	"context"
	"sync"
)

// HostLimiter permits two concurrent public reads per normalized PDS endpoint.
// Its zero value is ready for use. Waiting for a slot respects cancellation.
type HostLimiter struct {
	mu    sync.Mutex
	hosts map[string]*hostSlots
}

type hostSlots struct {
	active chan struct{}
	users  int
}

func (l *HostLimiter) Acquire(ctx context.Context, host string) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.hosts == nil {
		l.hosts = make(map[string]*hostSlots)
	}
	slots := l.hosts[host]
	if slots == nil {
		slots = &hostSlots{active: make(chan struct{}, 2)}
		l.hosts[host] = slots
	}
	slots.users++
	l.mu.Unlock()

	done := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		slots.users--
		if slots.users == 0 {
			delete(l.hosts, host)
		}
	}
	resume := HealthWait(ctx)
	defer resume()
	select {
	case slots.active <- struct{}{}:
		return func() { <-slots.active; done() }, nil
	case <-ctx.Done():
		done()
		return nil, ctx.Err()
	}
}
