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
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

type service struct {
	wakeBackfill chan struct{}

	hostMu    sync.Mutex
	hostReads map[string]int
	store
	api                          *palustre.Session
	owner, stream, constellation string
	targets                      map[string]bool

	interval time.Duration
	http     *http.Client
}

type backlink struct {
	DID, Collection, Rkey string
}

type discoveryJob struct {
	retrySeconds        int64
	target, cursor      string
	backlinks, retrying bool
	due, generation     int64
}

func (a *service) fetchDiscoveryPage(ctx context.Context, job discoveryJob) (string, []backlink, error) {
	target, cursor, backlinks := job.target, job.cursor, job.backlinks
	var next string
	var records []backlink
	if backlinks {
		client := &xrpc.Client{Host: a.constellation, Client: a.http}
		rows, c, err := palustre.BacklinksPage(ctx, client, target, followCollection, "subject", cursor)
		if err != nil {
			return "", nil, err
		}
		if c != nil {
			next = *c
		}
		for _, r := range rows {
			records = append(records, backlink{DID: r.Did, Collection: r.Collection, Rkey: r.Rkey})
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
		var retrySeconds int64
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
			delay := palustre.ScanRetryDelay(time.Duration(job.retrySeconds)*time.Second, a.interval)
			retrySeconds = int64(delay.Seconds())
			due = now.Add(delay).Unix()
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
		_, err = tx.Exec(`UPDATE discovery_schedule SET cursor=?,due=?,retrying=?,retry_seconds=?,turn=(SELECT max(turn)+1 FROM discovery_schedule)
			WHERE target=? AND backlinks=?`, next, due, retrying, retrySeconds, job.target, job.backlinks)
		return err
	})
}

const dirty = `INSERT INTO scans(did) VALUES(?) ON CONFLICT(did) DO UPDATE SET version=version+1,attempts=0,due=0`

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
	err = a.QueryRowContext(ctx, `SELECT target,backlinks,due,generation,cursor,retrying,retry_seconds
		FROM discovery_schedule ORDER BY due,turn,target,backlinks LIMIT 1`).
		Scan(&job.target, &job.backlinks, &job.due, &job.generation, &job.cursor, &job.retrying, &job.retrySeconds)
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
