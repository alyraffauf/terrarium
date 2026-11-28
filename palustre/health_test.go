package palustre

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"
)

// Compare both probes against independent clocks for startup and two workers.
// An active publisher must not hide its stalled peer.
func FuzzHealthProbes(f *testing.F) {
	f.Add([]byte{1, 1, 2, 3, 7, 0, 1, 1, 8})
	f.Add([]byte{1, 2, 4, 0, 0, 0, 7, 1, 1, 6})
	f.Add([]byte{1, 2, 5, 4, 1, 1, 7, 3})
	f.Fuzz(func(t *testing.T, ops []byte) {
		synctest.Test(t, func(t *testing.T) {
			h := &Health{workers: map[*healthWorker]struct{}{}}
			h.startup = h.register(startupGrace)
			initial := h.StartupContext(t.Context())
			var contexts [2]context.Context
			var stops [2]func()
			var resumes [2][]func()
			minute, startupBeat := 0, 0
			var initialized bool
			var deadlines [2]int
			for step, op := range ops[:min(48, len(ops))] {
				i := int(op/9) % 2
				switch op % 9 {
				case 0:
					time.Sleep(time.Minute)
					minute++
				case 1:
					time.Sleep(16 * time.Minute)
					minute += 16
				case 2:
					if !initialized {
						h.Ready()
						initialized = true
					}
				case 3:
					HealthProgress(initial)
					startupBeat = minute
				case 4:
					if contexts[i] == nil {
						contexts[i], stops[i] = h.Worker(t.Context())
						deadlines[i] = minute + 5
					}
				case 5:
					if contexts[i] != nil {
						HealthProgress(contexts[i])
						deadlines[i] = max(deadlines[i], minute+5)
					}
				case 6:
					if contexts[i] != nil {
						resumes[i] = append(resumes[i], HealthWait(contexts[i]))
					}
				case 7:
					if n := len(resumes[i]); n > 0 {
						resumes[i][n-1]()
						resumes[i] = resumes[i][:n-1]
						deadlines[i] = max(deadlines[i], minute+5)
					}
				case 8:
					if contexts[i] != nil {
						stops[i]()
						contexts[i] = nil
						resumes[i] = nil
					}
				}
				live := initialized || minute <= startupBeat+30
				for j, ctx := range contexts {
					if ctx != nil && len(resumes[j]) == 0 && minute > deadlines[j] {
						live = false
					}
				}
				for path, want := range map[string]bool{"/livez": live, "/readyz": live && initialized} {
					w := httptest.NewRecorder()
					h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
					if (w.Code == http.StatusOK) != want {
						t.Fatalf("step %d minute %d %s: got %d, want healthy=%t", step, minute, path, w.Code, want)
					}
				}
			}
		})
	})
}

func TestHealthWhileWorkerIsPaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &Health{workers: map[*healthWorker]struct{}{}}
		ctx, done := h.Worker(t.Context())
		defer done()
		go Pause(ctx, 24*time.Hour)
		synctest.Wait()
		time.Sleep(23 * time.Hour)
		if code, _ := h.status("/livez"); code != 200 {
			t.Fatal("paced worker reported stalled")
		}
		time.Sleep(2 * time.Hour)
		if code, _ := h.status("/livez"); code != 503 {
			t.Fatal("stalled worker remained live after pacing ended")
		}
	})
}
