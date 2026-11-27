package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/alyraffauf/terrarium/palustre/microcosm"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

type service struct {
	wakeBackfill chan struct{}

	hostMu    sync.Mutex
	hostReads map[string]int
	store
	api           *palustre.Session
	owner, stream string
	targets       map[string]bool

	interval time.Duration
	http     *http.Client
}

type backlink struct {
	DID, Collection, Rkey string
}

func (a *service) discover(ctx context.Context, target string, backlinks bool) error {
	cursor := ""
	seen := map[string]bool{}
	for {
		next, err := a.discoverPage(ctx, target, backlinks, cursor)
		if err != nil || next == "" {
			return err
		}
		if seen[next] {
			return errors.New("repeated discovery cursor")
		}
		seen[next] = true
		cursor = next
	}
}

func (a *service) discoverPage(ctx context.Context, target string, backlinks bool, cursor string) (string, error) {
	var next string
	var records []backlink
	if backlinks {
		client := &xrpc.Client{Host: "https://constellation.microcosm.blue", Client: a.http}
		page, err := microcosm.LinksGetBacklinks(ctx, client, cursor, nil, 100, followCollection+":subject", target)
		if err != nil {
			return "", err
		}
		if page.Records == nil {
			return "", errors.New("backlink response has no records array")
		}
		if page.Cursor != nil {
			next = *page.Cursor
		}
		for _, record := range page.Records {
			if record == nil {
				return "", errors.New("backlink response contains a null record")
			}
			records = append(records, backlink{DID: record.Did, Collection: record.Collection, Rkey: record.Rkey})
		}
	} else {
		client := &xrpc.Client{Host: "https://public.api.bsky.app", Client: a.http}
		page, err := bsky.GraphGetFollowers(ctx, client, target, cursor, 100, "")
		if err != nil {
			return "", err
		}
		if page.Subject == nil || page.Subject.Did != target {
			return "", errors.New("follower response has unexpected subject")
		}
		if page.Followers == nil {
			return "", errors.New("follower response has no followers array")
		}
		if page.Cursor != nil {
			next = *page.Cursor
		}
		for _, follower := range page.Followers {
			if follower == nil {
				return "", errors.New("follower response contains a null profile")
			}
			records = append(records, backlink{DID: follower.Did})
		}
	}
	err := a.transaction(ctx, func(tx *sql.Tx) error {
		for _, record := range records {
			if _, err := syntax.ParseDID(record.DID); err != nil {
				return err
			}
			if !backlinks {
				if _, err := tx.Exec("INSERT OR IGNORE INTO scans(did) VALUES(?)", record.DID); err != nil {
					return err
				}
				continue
			}
			if record.Collection != followCollection {
				return errors.New("unexpected backlink collection")
			}
			if _, err := syntax.ParseRecordKey(record.Rkey); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT OR IGNORE INTO hints(did,rkey,target) VALUES(?,?,?)", record.DID, record.Rkey, target); err != nil {
				return err
			}
		}
		return nil
	})
	return next, err
}

const dirty = `INSERT INTO scans(did) VALUES(?) ON CONFLICT(did) DO UPDATE SET version=version+1,due=0`

func (a *service) backfill(ctx context.Context) {
	for ctx.Err() == nil {
		delay, err := a.backfillStep(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("schedule follower discovery", "error", err)
			delay = time.Second
		}
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-a.wakeBackfill:
		}
		timer.Stop()
	}
}

func (a *service) backfillStep(ctx context.Context) (time.Duration, error) {
	now := time.Now()
	nextInventory := now.Add(max(a.interval, time.Second)).Unix()
	err := a.transaction(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec("UPDATE inventory_schedule SET due=? WHERE id=1 AND due<=?", nextInventory, now.Unix())
		if err != nil {
			return err
		}
		changed, err := res.RowsAffected()
		if err != nil || changed == 0 {
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO scans(did) SELECT DISTINCT actor FROM edges"); err != nil {
			return err
		}
		_, err = tx.Exec("INSERT OR IGNORE INTO scans(did) VALUES(?)", a.owner)
		return err
	})
	if err != nil {
		return 0, err
	}

	var target string
	var backlinks bool
	var due, generation int64
	err = a.QueryRowContext(ctx, `SELECT target,backlinks,due,generation FROM discovery_schedule ORDER BY due,target,backlinks LIMIT 1`).Scan(&target, &backlinks, &due, &generation)
	if err != nil {
		return 0, err
	}
	if due > now.Unix() {
		if err := a.QueryRowContext(ctx, "SELECT due FROM inventory_schedule WHERE id=1").Scan(&nextInventory); err != nil {
			return 0, err
		}
		return time.Until(time.Unix(min(due, nextInventory), 0)), nil
	}
	interval := a.interval
	if err := a.discover(ctx, target, backlinks); err != nil {
		slog.Warn("follower discovery", "target", target, "constellation", backlinks, "error", err)
		interval = time.Minute
	}
	_, err = a.ExecContext(ctx, `UPDATE discovery_schedule SET due=? WHERE target=? AND backlinks=? AND generation=?`,
		time.Now().Add(max(interval, time.Second)).Unix(), target, backlinks, generation)
	return 0, err
}
