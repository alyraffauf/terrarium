package main

import (
	"context"
	"database/sql"
	"github.com/alyraffauf/terrarium/palustre"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pagesCommitted = prometheus.NewCounter(prometheus.CounterOpts{Name: "terrarium_verification_pages_total", Help: "Inventory pages committed, including final and confirmed empty inventories."})
	verifiedHints  = prometheus.NewCounter(prometheus.CounterOpts{Name: "terrarium_verification_hints_total", Help: "Hint jobs resolved successfully."})
	rateLimited    = prometheus.NewCounter(prometheus.CounterOpts{Name: "terrarium_verification_pds_429_total", Help: "PDS verification responses with status 429."})
	inflight       = prometheus.NewGauge(prometheus.GaugeOpts{Name: "terrarium_verification_inflight", Help: "Verification jobs holding a PDS read slot."})
	txSeconds      = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "terrarium_sqlite_transaction_seconds", Help: "Trifolium transaction duration including waiting for the SQLite connection.", Buckets: prometheus.ExponentialBuckets(.001, 2, 15)})
)

func verificationCollectors() []prometheus.Collector {
	return []prometheus.Collector{pagesCommitted, verifiedHints, rateLimited, inflight, txSeconds}
}
func queueAges(db *sql.DB) []palustre.QueueGauge {
	var gauges []palustre.QueueGauge
	for lane, source := range map[string]string{"fresh": "scans WHERE continuation=0", "continuation": "scans WHERE continuation=1", "hint": "hints WHERE 1=1"} {
		gauges = append(gauges, palustre.QueueGauge{Name: "terrarium_verification_" + lane + "_oldest_seconds", Help: "Age of the oldest ready " + lane + " verification job.", Read: func(ctx context.Context) (int64, error) {
			var age int64
			err := db.QueryRowContext(ctx, "SELECT coalesce(unixepoch()-min(max(queued,due)),0) FROM "+source+" AND due<=unixepoch()").Scan(&age)
			return age, err
		}})
	}
	return gauges
}
