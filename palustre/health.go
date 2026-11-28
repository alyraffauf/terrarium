package palustre

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

const startupGrace = 30 * time.Minute
const workerGrace = 5 * time.Minute

// Health watches local worker control flow. Probes never call an upstream service.
type Health struct {
	server  *http.Server
	mu      sync.Mutex
	startup *healthWorker
	workers map[*healthWorker]struct{}
}

type healthWorker struct {
	health   *Health
	deadline time.Time
	grace    time.Duration
	waits    int
}

type healthContextKey struct{}

func StartHealth(address string) (*Health, error) {
	if address == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	h := &Health{workers: make(map[*healthWorker]struct{})}
	h.startup = h.register(startupGrace)
	h.server = &http.Server{Handler: h, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		if err := h.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server failed", "error", err)
		}
	}()
	return h, nil
}

func (h *Health) Close() {
	if h != nil {
		_ = h.server.Close()
	}
}

func (h *Health) register(grace time.Duration) *healthWorker {
	w := &healthWorker{health: h, grace: grace, deadline: time.Now().Add(grace)}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.workers[w] = struct{}{}
	return w
}

// StartupContext tracks initialization until Ready is called.
func (h *Health) StartupContext(ctx context.Context) context.Context {
	if h == nil {
		return ctx
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return context.WithValue(ctx, healthContextKey{}, h.startup)
}

func (h *Health) Ready() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.workers, h.startup)
	h.startup = nil
}

// Worker registers one independent worker. Call done when the worker exits.
func (h *Health) Worker(ctx context.Context) (context.Context, func()) {
	if h == nil {
		return ctx, func() {}
	}
	w := h.register(workerGrace)
	return context.WithValue(ctx, healthContextKey{}, w), func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.workers, w)
	}
}

// HealthProgress records a worker iteration or a remote call. A failed request is progress too.
func HealthProgress(ctx context.Context) { healthDelay(ctx, 0) }

func healthDelay(ctx context.Context, delay time.Duration) {
	w, _ := ctx.Value(healthContextKey{}).(*healthWorker)
	if w == nil {
		return
	}
	w.health.mu.Lock()
	defer w.health.mu.Unlock()
	// Another goroutine sharing the context can be in an intentional longer sleep.
	next := time.Now().Add(delay).Add(w.grace)
	if next.After(w.deadline) {
		w.deadline = next
	}
}

// HealthWait suspends the worker's clock while another watched worker owns a
// shared resource. Resume immediately after acquiring the resource.
func HealthWait(ctx context.Context) func() {
	w, _ := ctx.Value(healthContextKey{}).(*healthWorker)
	if w == nil {
		return func() {}
	}
	w.health.mu.Lock()
	w.waits++
	w.health.mu.Unlock()
	return func() {
		w.health.mu.Lock()
		defer w.health.mu.Unlock()
		w.waits--
		next := time.Now().Add(w.grace)
		if next.After(w.deadline) {
			w.deadline = next
		}
	}
}

func (h *Health) status(path string) (int, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch path {
	case "/readyz":
		if h.startup != nil {
			return http.StatusServiceUnavailable, "starting"
		}
	case "/livez":
	default:
		return http.StatusNotFound, "unknown probe"
	}
	now := time.Now()
	for worker := range h.workers {
		if worker.waits == 0 && now.After(worker.deadline) {
			return http.StatusServiceUnavailable, "worker stalled"
		}
	}
	return http.StatusOK, "ok"
}

func (h *Health) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	code, message := h.status(req.URL.Path)
	out.Header().Set("Content-Type", "text/plain; charset=utf-8")
	out.WriteHeader(code)
	_, _ = out.Write([]byte(message + "\n"))
}

type healthTransport struct{ base http.RoundTripper }

func (t healthTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	HealthProgress(r.Context())
	defer HealthProgress(r.Context())
	return t.base.RoundTrip(r)
}

// HTTPClient gives long inventories one progress signal per bounded HTTP read.
func (h *Health) HTTPClient() *http.Client {
	client := &http.Client{Timeout: 30 * time.Second}
	if h != nil {
		client.Transport = healthTransport{http.DefaultTransport}
	}
	return client
}
