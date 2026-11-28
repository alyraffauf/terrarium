package capensis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofrs/flock"

	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
)

func Run(ctx context.Context, cfg Config) (result error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	health, err := palustre.StartHealth(cfg.HealthAddress)
	if err != nil {
		return fmt.Errorf("start health listener: %w", err)
	}
	defer health.Close()
	ctx = health.StartupContext(ctx)
	lock := flock.New(cfg.Database + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("another process is using %s", cfg.Database)
	}
	defer lock.Close()
	db, err := openStore(cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	api := newSession(cfg, health.HTTPClient(), nil)
	closeMetrics, err := api.StartMetrics(cfg.MetricsAddress, db.queries.PendingCount)
	if err != nil {
		return fmt.Errorf("start metrics: %w", err)
	}
	defer closeMetrics()
	if err := api.Login(ctx); err != nil {
		return err
	}
	if err := db.bind(ctx, "owner", api.Client.Auth.Did); err != nil {
		return err
	}
	targets, err := api.prepareTargets(ctx, db)
	if err != nil {
		return err
	}
	inventory := &inventoryScan{checks: make(map[string]time.Time)}
	for _, target := range targets {
		inventory.checkTarget(ctx, target, db)
	}
	if err := db.initializeCursor(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(2)
	rescan := make(chan struct{}, 1)
	inventory.rescan = rescan
	collectorError := make(chan error, 1)
	go func() {
		defer workers.Done()
		collector := followers.Collector{Store: db, Endpoint: cfg.Jetstream, SelectCandidate: selectCandidate}
		if err := collector.Run(ctx); err != nil {
			collectorError <- err
			slog.Error("Jetstream collection failed", "error", err)
			cancel()
		}
	}()
	go func() { defer workers.Done(); rescanLoop(ctx, targets, db, rescan) }()
	defer func() {
		cancel()
		workers.Wait()
		select {
		case err := <-collectorError:
			result = errors.Join(result, err)
		default:
		}
	}()
	// Authenticated operations have one owner, keeping token refresh serialized.
	const readers = 4
	pool := followers.NewReadPool[checkedAccount](ctx, readers, health)
	defer pool.Close()
	owner := api.Client.Auth.Did
	nextFollows := time.Time{}
	writerCtx, done := health.Worker(ctx)
	defer done()
	health.Ready()
	for writerCtx.Err() == nil {
		palustre.HealthProgress(writerCtx)
		requested, err := db.takeRescan(writerCtx)
		if err != nil {
			return err
		}
		if requested {
			select {
			case rescan <- struct{}{}:
			default:
			}
		}
		if err := api.Refresh(writerCtx); err != nil {
			slog.Error("session refresh failed", "error", err)
			if !palustre.Pause(writerCtx, time.Minute) {
				break
			}
			continue
		}
		forceFollowRefresh := !time.Now().Before(nextFollows)
		if err := followers.RefreshSnapshot(writerCtx, db, api.Client, forceFollowRefresh); err != nil {
			slog.Error("follow refresh failed", "error", err)
			if !palustre.Pause(writerCtx, 10*time.Second) {
				break
			}
			continue
		}
		if forceFollowRefresh {
			nextFollows = time.Now().Add(5 * time.Minute)
		}
		if err := inventory.step(writerCtx, targets, db); err != nil {
			slog.Error("list inventory failed", "error", err)
		}

		if pool.Free() > 0 {
			rows, err := db.queries.ReadyJobs(writerCtx, readers)
			if err != nil {
				return err
			}
			for _, row := range rows {
				work := job{DID: row.Did, Generation: row.Generation, Attempts: int(row.Attempts)}
				pool.Start(work.DID, func(readCtx context.Context) checkedAccount {
					return checkAccount(readCtx, targets, owner, work, db)
				})
			}
		}
		checked, ok := pool.Next(writerCtx, time.Second)
		if !ok {
			continue
		}
		work := checked.work
		if err := checked.apply(writerCtx, db); err != nil {
			if errors.Is(err, followers.ErrSnapshotSuperseded) {
				continue
			}
			if writerCtx.Err() != nil {
				return nil
			}
			slog.Warn("membership update failed", "did", work.DID, "error", err)
			if err := db.retry(writerCtx, work); err != nil {
				return err
			}
			api.CheckRetries.Add(1)
		} else {
			api.ChecksCompleted.Add(1)
		}
	}
	return nil
}

func rescanLoop(ctx context.Context, targets []*listTarget, db *store, rescan <-chan struct{}) {
	next := make([]time.Time, len(targets))
	failures := make([]time.Duration, len(next))
	for ctx.Err() == nil {
		scanDue(ctx, targets, db, next, failures, time.Now())
		deadline := next[0]
		for _, t := range next {
			if t.Before(deadline) {
				deadline = t
			}
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-rescan:
			timer.Stop()
			for i := range next {
				next[i] = time.Time{}
			}
		case <-timer.C:
		}
	}
}

func (api *listTarget) backfill(ctx context.Context, db *store) error {
	// Recheck former members too, including accounts omitted from listRepos.
	err := db.queueMembers(ctx, api.pds)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		release, err := api.reads.Acquire(ctx, api.pds)
		if err != nil {
			return err
		}
		page, err := atproto.SyncListRepos(ctx, api.target, cursor, 1000)
		release()
		if err != nil {
			return err
		}
		if page.Repos == nil {
			return fmt.Errorf("PDS response has no repos array")
		}
		for _, repo := range page.Repos {
			if repo == nil {
				return fmt.Errorf("PDS response includes a null repository")
			}
			if _, err := syntax.ParseDID(repo.Did); err != nil {
				return err
			}
			if err := db.queueScan(ctx, repo.Did); err != nil {
				return err
			}
		}
		if page.Cursor == nil || *page.Cursor == "" {
			return nil
		}
		if seen[*page.Cursor] {
			return fmt.Errorf("PDS repeated pagination cursor")
		}
		seen[*page.Cursor] = true
		cursor = *page.Cursor
	}
}

func (api *session) prepareTargets(ctx context.Context, db *store) ([]*listTarget, error) {
	for _, target := range api.cfg.Targets {
		if target.ListURI == "" {
			continue
		}
		uri, err := syntax.ParseATURI(target.ListURI)
		if err != nil || uri.Authority().String() != api.Client.Auth.Did {
			return nil, fmt.Errorf("list %s must belong to authenticated owner %s", target.ListURI, api.Client.Auth.Did)
		}
	}
	var targets []*listTarget
	uris, err := planLists(ctx, db.queries, api.cfg.Targets, api.Client.Auth.Did)
	if err != nil {
		return nil, err
	}
	for i, target := range api.cfg.Targets {
		uri := uris[i]
		targets = append(targets, &listTarget{session: api, pds: target.PDS, rescanInterval: api.cfg.RescanInterval, target: &xrpc.Client{Host: target.PDS, Client: api.HTTP}, listURI: uri})
	}
	return targets, nil
}

// Each step reads at most one page, leaving membership work between pages.
// The owner's item collection is shared by all targets and is scanned once.
type inventoryScan struct {
	retryDelay time.Duration
	cursor     string
	next       time.Time
	lists      map[string]*listTarget
	checks     map[string]time.Time
	rescan     chan<- struct{}
	seen       map[string]bool
}

func (scan *inventoryScan) checkTarget(ctx context.Context, target *listTarget, db *store) {
	delay := target.rescanInterval
	if err := target.checkList(ctx, db); err != nil {
		slog.Error("list check failed", "pds", target.pds, "error", err)
		target.checkRetry = palustre.ScanRetryDelay(target.checkRetry, target.rescanInterval)
		delay = target.checkRetry
	} else {
		target.checkRetry = 0
		target.refreshListIcon(ctx)
		slog.Info("maintaining list", "pds", target.pds, "uri", target.listURI, "name", palustre.ListTitle(target.listURI))
	}
	scan.checks[target.listURI] = time.Now().Add(delay)
}

func (scan *inventoryScan) step(ctx context.Context, targets []*listTarget, db *store) error {
	if scan.checks == nil {
		scan.checks = map[string]time.Time{}
	}
	for _, target := range targets {
		if time.Now().Before(scan.checks[target.listURI]) {
			continue
		}
		wasPaused := target.paused.Load()
		scan.checkTarget(ctx, target, db)
		if wasPaused && !target.paused.Load() {
			select {
			case scan.rescan <- struct{}{}:
			default:
			}
			scan.cursor = ""
			scan.lists = nil
			scan.next = time.Time{}
		}
	}
	if scan.lists == nil {
		if time.Now().Before(scan.next) {
			return nil
		}
		scan.lists = map[string]*listTarget{}
		scan.seen = map[string]bool{}
		for _, api := range targets {
			if !api.paused.Load() {
				scan.lists[api.listURI] = api
			}
		}
		if len(scan.lists) == 0 {
			scan.lists = nil
			scan.seen = nil
			return nil
		}
	}

	api := targets[0]
	next, err := palustre.ListItemPage(ctx, api.Client, scan.cursor, func(list, did, key string) error {
		target := scan.lists[list]
		if target == nil || target.paused.Load() {
			return nil
		}
		if err := db.RememberMember(ctx, target.pds, did, key); err != nil {
			return err
		}
		return db.queueScan(ctx, did)
	})
	if err == nil && next != "" {
		if !scan.seen[next] {
			scan.seen[next] = true
			scan.cursor = next
			return nil
		}
		err = fmt.Errorf("list inventory repeated pagination cursor")
	}
	scan.cursor = ""
	scan.lists = nil
	scan.seen = nil
	delay := api.rescanInterval
	if err != nil {
		scan.retryDelay = palustre.ScanRetryDelay(scan.retryDelay, api.rescanInterval)
		delay = scan.retryDelay
	} else {
		scan.retryDelay = 0
	}
	scan.next = time.Now().Add(delay)
	return err
}

func scanDue(ctx context.Context, targets []*listTarget, db *store, next []time.Time, failures []time.Duration, now time.Time) {
	for i, api := range targets {
		if api.paused.Load() {
			next[i] = now.Add(time.Minute)
			continue
		}
		if now.Before(next[i]) {
			continue
		}
		delay := api.rescanInterval
		if err := api.backfill(ctx, db); err != nil {
			slog.Error("PDS backfill failed", "pds", api.pds, "error", err)
			delay = palustre.ScanRetryDelay(failures[i], api.rescanInterval)
			failures[i] = delay
		} else {
			failures[i] = 0
			slog.Info("PDS backfill queued", "pds", api.pds)
		}
		next[i] = time.Now().Add(delay)
	}
}
