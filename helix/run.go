package helix

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gofrs/flock"

	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
)

func Run(ctx context.Context, cfg Config) (result error) {
	if err := cfg.validate(); err != nil {
		return err
	}
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
	api := newSession(cfg, &http.Client{Timeout: 30 * time.Second}, nil)
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
		inventory.checkTarget(ctx, target)
	}
	if err := db.initializeCursor(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(3)
	go func() { defer workers.Done(); logProgress(ctx, api, db) }()
	rescan := make(chan struct{}, 1)
	watched := map[string]bool{}
	for _, target := range targets {
		watched[target.actor] = true
	}
	collectorError := make(chan error, 1)
	go func() {
		defer workers.Done()
		collector := followers.Collector{Store: db, Endpoint: cfg.Jetstream, SelectCandidate: db.selectCandidate(watched)}
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
	nextFollows := time.Time{}
	for ctx.Err() == nil {
		requested, err := db.takeRescan(ctx)
		if err != nil {
			return err
		}
		if requested {
			select {
			case rescan <- struct{}{}:
			default:
			}
		}
		if err := api.Refresh(ctx); err != nil {
			slog.Error("session refresh failed", "error", err)
			if !palustre.Pause(ctx, time.Minute) {
				break
			}
			continue
		}
		forceFollowRefresh := !time.Now().Before(nextFollows)
		if err := followers.RefreshSnapshot(ctx, db, api.Client, forceFollowRefresh); err != nil {
			slog.Error("follow refresh failed", "error", err)
			if !palustre.Pause(ctx, 10*time.Second) {
				break
			}
			continue
		}
		if forceFollowRefresh {
			nextFollows = time.Now().Add(5 * time.Minute)
		}
		if err := inventory.step(ctx, targets, db, rescan); err != nil {
			slog.Error("list inventory failed", "error", err)
		}

		jobs, err := db.queries.NextJobs(ctx, time.Now().Unix())
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			if !palustre.Pause(ctx, time.Second) {
				break
			}
			continue
		}
		batch := make([]job, 0, len(jobs))
		for _, row := range jobs {
			batch = append(batch, job{DID: row.Did, Generation: row.Generation, Attempts: int(row.Attempts)})
		}
		if err := processBatch(ctx, targets, batch, db); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func rescanLoop(ctx context.Context, targets []*listTarget, db *store, rescan <-chan struct{}) {
	next := make([]time.Time, 2*len(targets))
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

func (api *session) prepareTargets(ctx context.Context, db *store) ([]*listTarget, error) {
	seen := map[string]bool{}
	for i := range api.cfg.Targets {
		target := &api.cfg.Targets[i]
		if !strings.HasPrefix(target.Actor, "did:") {
			doc, err := api.Resolve(ctx, target.Actor)
			if err != nil {
				return nil, err
			}
			target.Actor = doc.DID
		}
		if seen[target.Actor] {
			return nil, fmt.Errorf("duplicate target DID %s", target.Actor)
		}
		seen[target.Actor] = true
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
		targets = append(targets, &listTarget{session: api, db: db, actor: target.Actor, rescanInterval: api.cfg.RescanInterval, listURI: uri})
	}
	return targets, nil
}

func applySubject(ctx context.Context, targets []*listTarget, work job, subject subjectState, db *store) error {
	var failures []error
	for _, target := range targets {
		if err := target.reconcileSubject(ctx, subject, db); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", target.actor, err))
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return db.finish(ctx, work)
}

// Each step reads at most one page, leaving membership work between pages.
// The owner's item collection is shared by all targets and is scanned once.
type inventoryScan struct {
	retryDelay time.Duration
	cursor     string
	next       time.Time
	lists      map[string]*listTarget
	checks     map[string]time.Time
	seen       map[string]bool
}

func (scan *inventoryScan) checkTarget(ctx context.Context, target *listTarget) {
	delay := target.rescanInterval
	if err := target.ensureList(ctx); err != nil {
		target.suspended.Store(true)
		slog.Error("list check failed", "account", target.actor, "error", err)
	}
	if target.suspended.Load() {
		target.checkRetry = palustre.ScanRetryDelay(target.checkRetry, target.rescanInterval)
		delay = target.checkRetry
	} else {
		target.checkRetry = 0
		target.refreshListIcon(ctx)
		slog.Info("maintaining list", "account", target.actor, "uri", target.listURI, "name", target.name)
	}
	scan.checks[target.listURI] = time.Now().Add(delay)
}

func (scan *inventoryScan) step(ctx context.Context, targets []*listTarget, db *store, rescan chan<- struct{}) error {
	for _, target := range targets {
		if time.Now().Before(scan.checks[target.listURI]) {
			continue
		}
		wasSuspended := target.suspended.Load()
		scan.checkTarget(ctx, target)
		if wasSuspended && !target.suspended.Load() {
			select {
			case rescan <- struct{}{}:
			default:
			}
			// Rediscover list items we may have missed while this target was suspended.
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
			if !api.suspended.Load() {
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
		if target == nil || target.suspended.Load() {
			return nil
		}
		if err := db.RememberMember(ctx, target.actor, did, key); err != nil {
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
	var scans sync.WaitGroup
	slots := make(chan struct{}, 4)
	for i := range next {
		api := targets[i/2]
		scan := api.backfill
		source := "AppView"
		if i%2 == 1 {
			scan = api.backfillLinks
			source = "Constellation"
		}
		if api.suspended.Load() {
			next[i] = now.Add(time.Minute)
			continue
		}
		if now.Before(next[i]) {
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			scans.Wait()
			return
		}
		scans.Add(1)
		go func() {
			defer scans.Done()
			defer func() { <-slots }()
			delay := api.rescanInterval
			if err := scan(ctx, db); err != nil {
				slog.Error("Follower backfill failed", "source", source, "account", api.actor, "error", err)
				delay = palustre.ScanRetryDelay(failures[i], api.rescanInterval)
				failures[i] = delay
			} else {
				failures[i] = 0
				slog.Info("Follower backfill queued", "source", source, "account", api.actor)
			}
			next[i] = time.Now().Add(delay)
		}()
	}
	scans.Wait()
}
