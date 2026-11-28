package followers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
)

func TestStalledReadRemainsVisibleWhileWriterProgresses(t *testing.T) {
	health, err := palustre.StartHealth("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer health.Close()
	health.Ready()

	synctest.Test(t, func(t *testing.T) {
		writer, done := health.Worker(t.Context())
		defer done()
		pool := NewReadPool[int](t.Context(), 1, health)
		release := make(chan struct{})
		defer pool.Close()
		pool.Start("actor", func(ctx context.Context) int {
			select {
			case <-ctx.Done():
			case <-release:
			}
			return 1
		})
		synctest.Wait()
		probe := func(want int) {
			t.Helper()
			rec := httptest.NewRecorder()
			health.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
			if rec.Code != want {
				t.Fatalf("livez=%d, want %d", rec.Code, want)
			}
		}
		probe(http.StatusOK)
		for range 6 {
			time.Sleep(time.Minute)
			palustre.HealthProgress(writer)
		}
		probe(http.StatusServiceUnavailable)
		close(release)
		synctest.Wait()
		probe(http.StatusOK)
	})
}
