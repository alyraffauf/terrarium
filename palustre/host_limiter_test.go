package palustre

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

func TestHostLimitIsolatesSlowPDS(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var limiter HostLimiter
		acquire := func(host string) func() {
			t.Helper()
			release, err := limiter.Acquire(t.Context(), host)
			if err != nil {
				t.Fatal(err)
			}
			return release
		}
		first, second := acquire("one"), acquire("one")
		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			release, err := limiter.Acquire(ctx, "one")
			if release != nil {
				release()
			}
			done <- err
		}()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("third reader passed while both slots were held")
		default:
		}
		acquire("two")()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		first()
		acquire("one")()
		second()
		if len(limiter.hosts) != 0 {
			t.Fatal("retained idle PDS")
		}
	})
}
