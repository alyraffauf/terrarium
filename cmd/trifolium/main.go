package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
	"github.com/alyraffauf/terrarium/trifolium/storage"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofrs/flock"
)

const nextChange = `
 WITH eligible AS (SELECT actor AS did FROM qualifying_follows JOIN cluster_targets USING(target)
 WHERE cluster=?1 AND actor<>?3 AND actor NOT IN (SELECT target FROM edges WHERE actor=?3)
 GROUP BY actor HAVING count(DISTINCT target)>=?2),
 changes AS (
 SELECT did FROM (SELECT did FROM published WHERE cluster=?1 EXCEPT SELECT did FROM eligible)
 UNION ALL SELECT did FROM (SELECT did FROM eligible EXCEPT SELECT did FROM published WHERE cluster=?1)
 UNION ALL SELECT did FROM pending WHERE cluster=?1)
 SELECT did,did IN (SELECT did FROM eligible) AS include FROM changes
 WHERE did NOT IN (SELECT did FROM retries WHERE cluster=?1 AND due>unixepoch())
 AND (did NOT IN (SELECT did FROM eligible) OR NOT EXISTS (SELECT 1 FROM scans WHERE did=?3))
 AND NOT EXISTS (SELECT 1 FROM audit_wait w JOIN scans s ON s.did=w.did WHERE w.cluster=?1 AND w.did=changes.did)
 ORDER BY include,did LIMIT 1`

func (a *cluster) publish(ctx context.Context) error {
	var did string
	var include bool
	err := a.QueryRowContext(ctx, nextChange, a.id, a.threshold, a.owner).Scan(&did, &include)
	if err != nil {
		return err
	}
	if _, err = a.ExecContext(ctx, "INSERT OR IGNORE INTO pending VALUES(?,?)", a.id, did); err != nil {
		return err
	}
	if err = a.api.Refresh(ctx); err != nil {
		return err
	}
	check := func(ctx context.Context) error {
		var eligible, ownerScanning bool
		err := a.QueryRowContext(ctx, `SELECT ?1<>?2
		AND NOT EXISTS(SELECT 1 FROM edges WHERE actor=?2 AND target=?1)
		AND (SELECT count(DISTINCT e.target) FROM qualifying_follows e JOIN cluster_targets c USING(target)
		     WHERE c.cluster=?3 AND e.actor=?1)>=?4,
		    EXISTS(SELECT 1 FROM scans WHERE did=?2)`, did, a.owner, a.id, a.threshold).Scan(&eligible, &ownerScanning)
		if err != nil {
			return err
		}
		if eligible != include || (include && ownerScanning) {
			return sql.ErrNoRows
		}
		return nil
	}
	err = a.api.Reconcile(ctx, a.store, palustre.Membership{Target: a.id, ListURI: a.list, DID: did, Include: include, BeforeWrite: check})
	if errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err != nil {
		retryErr := a.transaction(ctx, func(tx *sql.Tx) error {
			var attempts int
			err := tx.QueryRow("SELECT attempts FROM retries WHERE cluster=? AND did=?", a.id, did).Scan(&attempts)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			_, err = tx.Exec("INSERT OR REPLACE INTO retries(cluster,did,due,attempts) VALUES(?,?,unixepoch()+?,?)", a.id, did, int64(palustre.MembershipRetryDelay(attempts).Seconds()), min(16, attempts+1))
			return err
		})
		return errors.Join(err, retryErr)
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		query := "DELETE FROM published WHERE cluster=? AND did=?"
		if include {
			query = "INSERT OR IGNORE INTO published VALUES(?,?)"
		}
		if _, err := tx.Exec(query, a.id, did); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM retries WHERE cluster=? AND did=?", a.id, did); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM audit_wait WHERE cluster=? AND did=?", a.id, did); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM pending WHERE cluster=? AND did=?", a.id, did)
		return err
	})
}
func work(ctx context.Context, name string, next func(context.Context) error) {
	for ctx.Err() == nil {
		palustre.HealthProgress(ctx)
		if err := next(ctx); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Warn(name, "error", err)
			}
			palustre.Pause(ctx, time.Second)
		}
	}
}
func run(ctx context.Context) error {
	rawClusters := flag.String("clusters", "", `clusters by ID, e.g. {"garden":{"accounts":["a.example"],"threshold":1}}`)
	accounts := flag.String("accounts", "", "watched handles or DIDs for the default cluster, separated by commas")
	threshold := flag.Int("threshold", 2, "minimum distinct watched accounts followed for the default cluster")
	interval := flag.Duration("rescan-interval", 24*time.Hour, "interval between full discovery passes")
	path := flag.String("database", "trifolium.sqlite", "SQLite state file")
	endpoint := flag.String("jetstream", "wss://jetstream.waow.tech", "Jetstream v2 endpoint")
	identifier := flag.String("identifier", os.Getenv("TRIFOLIUM_IDENTIFIER"), "list owner")
	constellation := flag.String("constellation", palustre.DefaultConstellation, "backlink index URL")
	slingshot := flag.String("slingshot", "https://slingshot.aly.town", "identity resolver")
	pace := flag.Duration("write-interval", palustre.DefaultFollowerWriteInterval, "minimum interval between list writes")
	healthAddress := flag.String("health-address", "", "HTTP listen address for /readyz and /livez (disabled by default)")
	metrics := flag.String("metrics-address", "", "Prometheus listen address, such as :9090 (disabled by default)")
	workers := flag.Int("verification-workers", 16, "concurrent PDS verification workers")
	flag.Parse()
	if *workers < 1 {
		return errors.New("verification-workers must be positive")
	}
	password := os.Getenv("TRIFOLIUM_APP_PASSWORD")
	if *threshold < 1 || *interval <= 0 || *pace <= 0 || *identifier == "" || password == "" {
		return errors.New("set identifier and TRIFOLIUM_APP_PASSWORD and use positive threshold and intervals")
	}
	health, err := palustre.StartHealth(*healthAddress)
	if err != nil {
		return fmt.Errorf("start health listener: %w", err)
	}
	defer health.Close()
	ctx = health.StartupContext(ctx)
	stream, err := palustre.NormalizeJetstream(*endpoint)
	if err != nil {
		return err
	}
	resolver, err := palustre.NormalizePDS(*slingshot)
	if err != nil {
		return err
	}
	index, err := palustre.NormalizePDS(*constellation)
	if err != nil {
		return err
	}
	client := health.HTTPClient()
	api := &palustre.Session{HTTP: client, Config: palustre.Config{Identifier: *identifier, Password: password, Slingshot: resolver, WriteInterval: *pace}}
	configs, err := parseClusters(*rawClusters, *accounts, *threshold)
	if err != nil {
		return err
	}
	lock := flock.New(*path + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("database already in use")
	}
	defer lock.Close()
	db, err := palustre.OpenSQLite(ctx, *path)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = storage.Migrate(ctx, db); err != nil {
		return err
	}
	stopMetrics, err := api.StartMetricsWithCollectors(*metrics, nil, verificationCollectors(), queueAges(db)...)
	if err != nil {
		return err
	}
	defer stopMetrics()
	if err = api.Login(ctx); err != nil {
		return err
	}
	owner := api.Client.Auth.Did
	a := service{constellation: index, wakeBackfill: make(chan struct{}, 1), store: store{db}, api: api, owner: owner, stream: stream, targets: map[string]bool{}, interval: *interval, http: client}
	lists, err := a.setupClusters(ctx, configs, *threshold)
	if err != nil {
		return err
	}
	if err = a.prepareStream(ctx); err != nil {
		return err
	}
	for _, list := range lists {
		slog.Info("maintaining moderation list", "cluster", list.id, "list", list.list, "threshold", list.threshold)
	}
	targets := slices.Sorted(maps.Keys(a.targets))
	refreshBlocks := func(ctx context.Context) {
		followers.RefreshBlocks(ctx, db, &xrpc.Client{Host: index, Client: client}, targets, nil)
	}
	refreshBlocks(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { followers.PollBlocks(ctx, refreshBlocks) })
	wg.Go(func() { a.listen(ctx) })
	wg.Go(func() { a.backfill(ctx) })
	for i := range *workers {
		wg.Go(func() {
			ctx, done := health.Worker(ctx)
			defer done()
			work(ctx, "follow verification", func(ctx context.Context) error { return a.verify(ctx, workerLane(i)) })
		})
	}
	wg.Go(func() {
		ctx, done := health.Worker(ctx)
		defer done()
		var nextAudit time.Time
		var auditDelay time.Duration
		for ctx.Err() == nil {
			palustre.HealthProgress(ctx)
			if !time.Now().Before(nextAudit) {
				if err := a.auditMembers(ctx, lists); err != nil {
					slog.Warn("list-item inventory", "error", err)
					auditDelay = palustre.ScanRetryDelay(auditDelay, a.interval)
					nextAudit = time.Now().Add(auditDelay)
				} else {
					auditDelay = 0
					nextAudit = time.Now().Add(a.interval)
				}
			}
			idle := true
			for _, list := range lists {
				err := list.publishWithDescription(ctx)
				if err == nil {
					idle = false
				} else if !errors.Is(err, sql.ErrNoRows) {
					slog.Warn("list reconciliation", "cluster", list.id, "error", err)
				}
			}
			if idle {
				palustre.Pause(ctx, time.Second)
			}
		}
	})
	health.Ready()
	wg.Wait()
	return nil
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		slog.Error("trifolium", "error", err)
		os.Exit(1)
	}
}
