package palustre

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetrics serves an isolated registry without involving the membership worker.
// An empty address disables the listener. The caller closes it before its database.
func (api *Session) StartMetrics(address string, pending func(context.Context) (int64, error)) (func(), error) {
	if address == "" {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.metricsHandler(pending))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	slog.Info("serving metrics", "address", listener.Addr().String(), "path", "/metrics")
	return func() { _ = server.Close() }, nil
}

func (api *Session) metricsHandler(pending func(context.Context) (int64, error)) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_members_added_total", Help: "Successful list-item creations since process start."}, func() float64 { return float64(api.MembersAdded.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_members_removed_total", Help: "Successful list-item deletions since process start, including duplicate cleanup."}, func() float64 { return float64(api.MembersRemoved.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_checks_completed_total", Help: "Successful membership checks since process start."}, func() float64 { return float64(api.ChecksCompleted.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_check_retries_total", Help: "Failed membership checks scheduled for retry since process start."}, func() float64 { return float64(api.CheckRetries.Load()) }),
		&pendingCollector{count: pending, desc: prometheus.NewDesc("terrarium_pending_accounts", "Queued accounts, including in-flight checks and delayed retries.", nil, nil)},
	)
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: 5 * time.Second})
}

type pendingCollector struct {
	count func(context.Context) (int64, error)
	desc  *prometheus.Desc
}

func (collector *pendingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- collector.desc }
func (collector *pendingCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	count, err := collector.count(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(collector.desc, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(collector.desc, prometheus.GaugeValue, float64(count))
}
