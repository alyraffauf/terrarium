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

type discoveryJob struct {
	target, cursor      string
	backlinks, retrying bool
	due, generation     int64
}

func (a *service) fetchDiscoveryPage(ctx context.Context, job discoveryJob) (string, []backlink, error) {
	target, cursor, backlinks := job.target, job.cursor, job.backlinks
	var next string
	var records []backlink
	if backlinks {
		client := &xrpc.Client{Host: "https://constellation.microcosm.blue", Client: a.http}
		page, err := microcosm.LinksGetBacklinks(ctx, client, cursor, nil, 100, followCollection+":subject", target)
		if err != nil {
			return "", nil, err
		}
		if page.Records == nil {
			return "", nil, errors.New("backlink response has no records array")
		}
		if page.Cursor != nil {
			next = *page.Cursor
		}
		for _, record := range page.Records {
			if record == nil {
				return "", nil, errors.New("backlink response contains a null record")
			}
			records = append(records, backlink{DID: record.Did, Collection: record.Collection, Rkey: record.Rkey})
		}
	} else {
		client := &xrpc.Client{Host: "https://public.api.bsky.app", Client: a.http}
		page, err := bsky.GraphGetFollowers(ctx, client, target, cursor, 100, "")
		if err != nil {
			return "", nil, err
		}
		if page.Subject == nil || page.Subject.Did != target {
			return "", nil, errors.New("follower response has unexpected subject")
		}
		if page.Followers == nil {
			return "", nil, errors.New("follower response has no followers array")
		}
		if page.Cursor != nil {
			next = *page.Cursor
		}
		for _, follower := range page.Followers {
			if follower == nil {
				return "", nil, errors.New("follower response contains a null profile")
			}
			records = append(records, backlink{DID: follower.Did})
		}
	}
	for _, record := range records {
		if _, err := syntax.ParseDID(record.DID); err != nil {
			return "", nil, err
		}
		if !backlinks {
			continue
		}
		if record.Collection != followCollection {
			return "", nil, errors.New("unexpected backlink collection")
		}
		if _, err := syntax.ParseRecordKey(record.Rkey); err != nil {
			return "", nil, err
		}
	}
	return next, records, nil
}

func (a *service) saveDiscoveryPage(ctx context.Context, job discoveryJob, next string, records []backlink, pageErr error, now time.Time) error {
	return a.transaction(ctx, func(tx *sql.Tx) error {
		var generation int64
		err := tx.QueryRow("SELECT generation FROM discovery_schedule WHERE target=? AND backlinks=?", job.target, job.backlinks).Scan(&generation)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && generation != job.generation) {
			return nil
		}
		if err != nil {
			return err
		}
		fetchFailed := pageErr != nil
		if next != "" {
			result, err := tx.Exec("INSERT OR IGNORE INTO discovery_cursors VALUES(?,?,?)", job.target, job.backlinks, next)
			if err != nil {
				return err
			}
			inserted, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if inserted == 0 {
				pageErr = errors.New("repeated discovery cursor")
				next = ""
			}
		}
		due, retrying := now.Unix(), false
		for _, record := range records {
			var err error
			if job.backlinks {
				_, err = tx.Exec("INSERT OR IGNORE INTO hints(did,rkey,target) VALUES(?,?,?)", record.DID, record.Rkey, job.target)
			} else {
				_, err = tx.Exec("INSERT OR IGNORE INTO scans(did) VALUES(?)", record.DID)
			}
			if err != nil {
				return err
			}
		}
		if pageErr == nil {
			if next == "" {
				due = now.Add(max(a.interval, time.Second)).Unix()
			}
		} else {
			slog.Warn("follower discovery", "target", job.target, "constellation", job.backlinks, "error", pageErr)
			due = now.Add(time.Minute).Unix()
			// Retry a failed continuation once. A cursor can expire between pages.
			if fetchFailed && job.cursor != "" && !job.retrying {
				next, retrying = job.cursor, true
			}
		}
		if next == "" {
			if _, err := tx.Exec("DELETE FROM discovery_cursors WHERE target=? AND backlinks=?", job.target, job.backlinks); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`UPDATE discovery_schedule SET cursor=?,due=?,retrying=?,turn=(SELECT max(turn)+1 FROM discovery_schedule)
			WHERE target=? AND backlinks=?`, next, due, retrying, job.target, job.backlinks)
		return err
	})
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

	var job discoveryJob
	err = a.QueryRowContext(ctx, `SELECT target,backlinks,due,generation,cursor,retrying
		FROM discovery_schedule ORDER BY due,turn,target,backlinks LIMIT 1`).
		Scan(&job.target, &job.backlinks, &job.due, &job.generation, &job.cursor, &job.retrying)
	if err != nil {
		return 0, err
	}
	if job.due > now.Unix() {
		if err := a.QueryRowContext(ctx, "SELECT due FROM inventory_schedule WHERE id=1").Scan(&nextInventory); err != nil {
			return 0, err
		}
		return time.Until(time.Unix(min(job.due, nextInventory), 0)), nil
	}
	next, records, pageErr := a.fetchDiscoveryPage(ctx, job)
	return 0, a.saveDiscoveryPage(ctx, job, next, records, pageErr, time.Now())
}
