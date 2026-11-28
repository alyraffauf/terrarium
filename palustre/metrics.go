package palustre

import (
	"context"
	"errors"
	lex "github.com/bluesky-social/indigo/lex/util"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetrics serves an isolated registry without involving the membership worker.
// An empty address disables the listener. The caller closes it before its database.
func (api *Session) StartMetrics(address string, pending func(context.Context) (int64, error), gauges ...QueueGauge) (func(), error) {
	return api.StartMetricsWithCollectors(address, pending, nil, gauges...)
}

func (api *Session) StartMetricsWithCollectors(address string, pending func(context.Context) (int64, error), collectors []prometheus.Collector, gauges ...QueueGauge) (func(), error) {
	if address == "" {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.metricsHandlerExtra(pending, collectors, gauges...))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	slog.Info("serving metrics", "address", listener.Addr().String(), "path", "/metrics")
	return func() { _ = server.Close() }, nil
}

func (api *Session) metricsHandler(pending func(context.Context) (int64, error), gauges ...QueueGauge) http.Handler {
	return api.metricsHandlerExtra(pending, nil, gauges...)
}
func (api *Session) metricsHandlerExtra(pending func(context.Context) (int64, error), collectors []prometheus.Collector, gauges ...QueueGauge) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors...)
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "terrarium_pds_verification_read_duration_seconds",
		Help:    "Elapsed seconds per verification XRPC read, including response decoding and errors.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"method", "result"})
	api.pdsReads.CompareAndSwap(nil, latency)
	registry.MustRegister(api.pdsReads.Load(), prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_follow_inventories_completed_total", Help: "Completed candidate follow inventories, including verified empty inventories."}, func() float64 { return float64(api.FollowInventoriesCompleted.Load()) }))
	registry.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_members_added_total", Help: "Successful list-item creations since process start."}, func() float64 { return float64(api.MembersAdded.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_members_removed_total", Help: "Successful list-item deletions since process start, including duplicate cleanup."}, func() float64 { return float64(api.MembersRemoved.Load()) }),
	)
	if pending == nil {
		registry.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_verification_deferred_total", Help: "Verification attempts deferred after a failed read."}, func() float64 { return float64(api.VerificationDeferred.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_verification_recovered_total", Help: "Verification retries that completed successfully."}, func() float64 { return float64(api.VerificationRecovered.Load()) }),
		)
	} else {
		registry.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_checks_completed_total", Help: "Successful membership checks since process start."}, func() float64 { return float64(api.ChecksCompleted.Load()) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "terrarium_check_retries_total", Help: "Failed membership checks scheduled for retry since process start."}, func() float64 { return float64(api.CheckRetries.Load()) }),
		)
		gauges = append(gauges, QueueGauge{Name: "terrarium_pending_accounts", Help: "Queued accounts, including in-flight checks and delayed retries.", Read: pending})
	}
	for _, gauge := range gauges {
		registry.MustRegister(&queueCollector{read: gauge.Read, desc: prometheus.NewDesc(gauge.Name, gauge.Help, nil, nil)})
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: 5 * time.Second})
}

type QueueGauge struct {
	Name string
	Help string
	Read func(context.Context) (int64, error)
}

type queueCollector struct {
	read func(context.Context) (int64, error)
	desc *prometheus.Desc
}

func (collector *queueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- collector.desc }
func (collector *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	count, err := collector.read(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(collector.desc, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(collector.desc, prometheus.GaugeValue, float64(count))
}

// PDSVerificationClient measures only reads made to verify a candidate.
// Discovery, identity resolution and publication use their original clients.
func (api *Session) PDSVerificationClient(client lex.LexClient) lex.LexClient {
	if histogram := api.pdsReads.Load(); histogram != nil {
		return measuredVerificationClient{client, histogram}
	}
	return client
}

type verificationReads = atomic.Pointer[prometheus.HistogramVec]

type measuredVerificationClient struct {
	client  lex.LexClient
	latency *prometheus.HistogramVec
}

func (c measuredVerificationClient) LexDo(ctx context.Context, method, encoding, endpoint string, params map[string]any, body, out any) error {
	start := time.Now()
	err := c.client.LexDo(ctx, method, encoding, endpoint, params, body, out)
	result := "success"
	if err != nil {
		result = "error"
	}
	c.latency.WithLabelValues(endpoint, result).Observe(time.Since(start).Seconds())
	return err
}
