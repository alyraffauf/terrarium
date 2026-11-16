package capensis

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofrs/flock"

	"github.com/alyraffauf/terrarium/palustre/listlex"
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
	for _, target := range targets {
		if err := target.ensureList(ctx); err != nil {
			return err
		}
		target.refreshListIcon(ctx)
		slog.Info("maintaining list", "pds", target.pds, "uri", target.listURI, "name", listName(target.pds))
	}
	if err := db.initializeCursor(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(2)
	rescan := make(chan struct{}, 1)
	collectorError := make(chan error, 1)
	go func() {
		defer workers.Done()
		if err := collect(ctx, cfg, db, rescan); err != nil {
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
	inventory := &inventoryScan{}
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
			if !pause(ctx, time.Minute) {
				break
			}
			continue
		}
		if err := api.syncFollows(ctx, db, time.Now().After(nextFollows)); err != nil {
			slog.Error("follow refresh failed", "error", err)
			if !pause(ctx, 10*time.Second) {
				break
			}
			continue
		}
		if time.Now().After(nextFollows) {
			nextFollows = time.Now().Add(5 * time.Minute)
		}
		if err := inventory.step(ctx, targets, db); err != nil {
			slog.Error("list inventory failed", "error", err)
		}

		work, err := db.next(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			if !pause(ctx, time.Second) {
				break
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := reconcileTargets(ctx, targets, work, db); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("membership update failed", "did", work.DID, "error", err)
			if err := db.retry(ctx, work); err != nil {
				return err
			}
		}
	}
	return nil
}

func rescanLoop(ctx context.Context, targets []*listTarget, db *store, rescan <-chan struct{}) {
	next := make([]time.Time, len(targets))
	for ctx.Err() == nil {
		scanDue(ctx, targets, db, next, time.Now())
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
	cursor := ""
	for {
		page, err := atproto.SyncListRepos(ctx, api.target, cursor, 1000)
		if err != nil {
			return err
		}
		for _, repo := range page.Repos {
			if err := db.queueScan(ctx, repo.Did); err != nil {
				return err
			}
		}
		if page.Cursor == nil || *page.Cursor == "" {
			return nil
		}
		if *page.Cursor == cursor {
			return fmt.Errorf("PDS repeated pagination cursor")
		}
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
	uris, err := db.bindLists(ctx, api.cfg.Targets, api.Client.Auth.Did)
	if err != nil {
		return nil, err
	}
	for i, target := range api.cfg.Targets {
		uri := uris[i]
		targets = append(targets, &listTarget{session: api, pds: target.PDS, rescanInterval: api.cfg.RescanInterval, target: &xrpc.Client{Host: target.PDS, Client: api.HTTP}, listURI: uri})
	}
	return targets, nil
}

func reconcileTargets(ctx context.Context, targets []*listTarget, work job, db *store) error {
	if len(targets) == 0 {
		return db.finish(ctx, work)
	}
	subject, err := targets[0].subject(ctx, db, work.DID)
	if err != nil {
		return err
	}
	var failures []error
	for _, target := range targets {
		if err := target.reconcileSubject(ctx, subject, db); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", target.pds, err))
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
	cursor string
	next   time.Time
	lists  map[string]*listTarget
	checks map[string]time.Time
}

func (scan *inventoryScan) step(ctx context.Context, targets []*listTarget, db *store) error {
	if scan.checks == nil {
		scan.checks = map[string]time.Time{}
	}
	for _, api := range targets {
		if time.Now().Before(scan.checks[api.listURI]) {
			continue
		}
		delay := api.rescanInterval
		if err := api.ensureList(ctx); err != nil {
			slog.Error("list check failed", "pds", api.pds, "error", err)
			delay = time.Minute
		} else {
			api.refreshListIcon(ctx)
		}
		scan.checks[api.listURI] = time.Now().Add(delay)
	}
	if scan.lists == nil {
		if time.Now().Before(scan.next) {
			return nil
		}
		scan.lists = map[string]*listTarget{}
		for _, api := range targets {
			scan.lists[api.listURI] = api
		}
	}

	api := targets[0]
	page, err := atproto.RepoListRecords(ctx, api.Client, itemCollection, scan.cursor, 100, api.Client.Auth.Did, false)
	if err == nil {
		for _, record := range page.Records {
			if record == nil || record.Value == nil {
				continue
			}
			item, ok := record.Value.Val.(*listlex.GraphListitem)
			if !ok {
				continue
			}
			target := scan.lists[item.List]
			if target == nil {
				continue
			}
			uri, e := syntax.ParseATURI(record.Uri)
			if e != nil {
				err = e
				break
			}
			if e = db.RememberMember(ctx, target.pds, item.Subject, uri.RecordKey().String()); e != nil {
				err = e
				break
			}
			if e = db.queueScan(ctx, item.Subject); e != nil {
				err = e
				break
			}
		}
	}
	if err == nil && page.Cursor != nil && *page.Cursor != "" {
		if *page.Cursor != scan.cursor {
			scan.cursor = *page.Cursor
			return nil
		}
		err = fmt.Errorf("list inventory repeated pagination cursor")
	}
	scan.cursor = ""
	scan.lists = nil
	delay := api.rescanInterval
	if err != nil {
		delay = time.Minute
	}
	scan.next = time.Now().Add(delay)
	return err
}

func scanDue(ctx context.Context, targets []*listTarget, db *store, next []time.Time, now time.Time) {
	for i, api := range targets {
		if now.Before(next[i]) {
			continue
		}
		delay := api.rescanInterval
		if err := api.backfill(ctx, db); err != nil {
			slog.Error("PDS backfill failed", "pds", api.pds, "error", err)
			delay = time.Minute
		} else {
			slog.Info("PDS backfill queued", "pds", api.pds)
		}
		next[i] = time.Now().Add(delay)
	}
}
