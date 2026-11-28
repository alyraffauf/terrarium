package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
)

const followCollection = palustre.FollowCollection

type verifyJob struct {
	did, key, target string
	id, version      int64
	attempts         int
	cached           bool
	pds              string
}

type verificationLane int

const (
	freshLane verificationLane = iota
	continuationLane
	hintLane
)

func workerLane(index int) verificationLane {
	return [...]verificationLane{continuationLane, freshLane, continuationLane, hintLane}[index%4]
}

func (a *service) claim(ctx context.Context, lane verificationLane) (j verifyJob, err error) {
	err = a.transaction(ctx, func(tx *sql.Tx) error {
		hint := func() error {
			return tx.QueryRow(`SELECT id,did,rkey,target,attempts FROM hints WHERE due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,id LIMIT 1`).Scan(&j.id, &j.did, &j.key, &j.target, &j.attempts)
		}
		owner := func() error {
			err := tx.QueryRow("SELECT did,version,attempts FROM scans WHERE did=? AND due<=unixepoch() AND did NOT IN (SELECT did FROM scanning)", a.owner).Scan(&j.did, &j.version, &j.attempts)
			return err
		}
		scan := func(continuation bool) error {
			return tx.QueryRow(`SELECT did,version,attempts FROM scans WHERE continuation=? AND due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,rowid LIMIT 1`, continuation).Scan(&j.did, &j.version, &j.attempts)
		}
		err := owner()
		order := [...]verificationLane{lane, (lane + 1) % 3, (lane + 2) % 3}
		for _, choice := range order {
			if !errors.Is(err, sql.ErrNoRows) {
				break
			}
			if choice == hintLane {
				err = hint()
			} else {
				err = scan(choice == continuationLane)
			}
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO scanning VALUES(?)", j.did)
		return err
	})
	return
}
func (a *service) acquireHost(host string) func() {
	a.hostMu.Lock()
	defer a.hostMu.Unlock()
	if a.hostReads == nil {
		a.hostReads = map[string]int{}
	}
	if time.Now().Before(a.cooldowns[host]) {
		return nil
	}
	delete(a.cooldowns, host)
	if a.hostReads[host] >= 2 {
		return nil
	}
	inflight.Inc()
	a.hostReads[host]++
	return func() {
		a.hostMu.Lock()
		defer a.hostMu.Unlock()
		inflight.Dec()
		a.hostReads[host]--
		if a.hostReads[host] == 0 {
			delete(a.hostReads, host)
		}
	}
}
func (a *service) verify(ctx context.Context, lane verificationLane) error {
	j, err := a.claim(ctx, lane)
	if err != nil {
		return err
	}
	defer a.ExecContext(context.WithoutCancel(ctx), "DELETE FROM scanning WHERE did=?", j.did)
	doc, cached, err := a.api.CachedDID(ctx, j.did)
	j.cached = cached
	busy := false
	delay := 60
	if err == nil {
		if release := a.acquireHost(doc.PDS); release != nil {
			defer release()
			client := &xrpc.Client{Host: doc.PDS, Client: palustre.ObserveRateLimits(a.http, func(until time.Time) { a.coolHost(doc.PDS, until) })}
			err = a.verifyOnPDS(ctx, client, j, doc.FromDIDDocument && !cached)

		} else {
			busy = true
			delay = 2
			a.hostMu.Lock()
			if wait := time.Until(a.cooldowns[doc.PDS]); wait > 2*time.Second {
				delay = int(wait/time.Second) + 1
			}
			a.hostMu.Unlock()
		}
	}
	if err == nil && !busy {
		if j.attempts > 0 {
			a.api.VerificationRecovered.Add(1)
		}
		return nil
	}
	increment := 0
	if err != nil {
		increment = 1
		if j.did != a.owner {
			delay = int(palustre.VerificationRetryDelay(j.attempts).Seconds())
		}
	}
	var retry error
	if j.id != 0 {
		_, retry = a.ExecContext(ctx, "UPDATE hints SET attempts=min(6,attempts+?),due=unixepoch()+? WHERE id=?", increment, delay, j.id)
	} else {
		_, retry = a.ExecContext(ctx, "UPDATE scans SET attempts=min(6,attempts+?),due=unixepoch()+? WHERE did=? AND version=?", increment, delay, j.did, j.version)
	}
	if retry != nil {
		return errors.Join(err, retry)
	}
	if err != nil {
		a.api.VerificationDeferred.Add(1)
		slog.Warn("follow verification deferred", "did", j.did, "retry_seconds", delay, "error", err)
	}
	return nil
}

func (a *service) coolHost(host string, until time.Time) {
	a.hostMu.Lock()
	if a.cooldowns == nil {
		a.cooldowns = map[string]time.Time{}
	}
	if until.After(a.cooldowns[host]) {
		a.cooldowns[host] = until
	}
	a.hostMu.Unlock()
	rateLimited.Inc()
	slog.Warn("PDS verification rate limited", "pds", host, "until", until)
}

func (a *service) verifyOnPDS(ctx context.Context, c *xrpc.Client, j verifyJob, checkStatus bool) error {
	if checkStatus {
		active, err := palustre.RepoActive(ctx, a.api.PDSVerificationClient(c), j.did)
		if err != nil {
			return err
		}
		if !active {
			if j.did == a.owner {
				return fmt.Errorf("owner repository is inactive")
			}
			if j.id == 0 {
				return a.savePage(ctx, j, "", "", nil)
			}
			_, err = a.ExecContext(ctx, "DELETE FROM hints WHERE id=?", j.id)
			if err == nil {
				verifiedHints.Inc()
			}
			return err
		}
	}
	if j.id == 0 {
		return a.scanPage(ctx, c, j)
	}
	err := a.checkHint(ctx, c, j)
	if err == nil {
		verifiedHints.Inc()
	}
	return err
}

func (a *service) checkHint(ctx context.Context, c *xrpc.Client, j verifyJob) error {
	reader := a.api.PDSVerificationClient(c)
	record, err := atproto.RepoGetRecord(ctx, reader, "", followCollection, j.did, j.key)
	if j.cached && palustre.MayHaveMoved(err) {
		if confirmErr := a.api.ConfirmPDS(ctx, j.did, c.Host); confirmErr != nil {
			return confirmErr
		}
	}
	if err != nil && !palustre.XRPCCode(err, "RecordNotFound") {
		if statusErr := palustre.VerifyRepoReadError(ctx, reader, j.did, err); statusErr != nil {
			return statusErr
		}
	}
	var matches bool
	if err == nil {
		key, subject, err := palustre.ParseFollowRecord(j.did, record.Uri, record.Value)
		if err != nil {
			return err
		}
		if key != j.key {
			return fmt.Errorf("unexpected follow record key %q", key)
		}
		matches = subject == j.target
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		// A Jetstream mutation deletes this exact hint, invalidating an older HTTP response.
		if matches {
			if _, err := tx.Exec("INSERT OR REPLACE INTO edges SELECT did,rkey,target FROM hints WHERE id=?", j.id); err != nil {
				return err
			}
		}
		_, err := tx.Exec("DELETE FROM hints WHERE id=?", j.id)
		return err
	})
}

func (a *service) scanPage(ctx context.Context, c *xrpc.Client, j verifyJob) error {
	j.pds = c.Host
	var cursor string
	err := a.QueryRowContext(ctx, "SELECT cursor FROM pages WHERE did=? AND version=? AND pds=?", j.did, j.version, c.Host).Scan(&cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	seen := map[string]bool{}
	records := map[string]string{}
	reader := a.api.PDSVerificationClient(c)
	next, err := palustre.ListFollowPage(ctx, reader, j.did, cursor, func(key, subject string) error {
		if seen[key] {
			return fmt.Errorf("duplicate follow record key %q", key)
		}
		seen[key] = true
		if subject != "" && (a.targets[subject] || j.did == a.owner) {
			records[key] = subject
		}
		return nil
	})
	if err != nil {
		if j.cached && palustre.MayHaveMoved(err) {
			if confirmErr := a.api.ConfirmPDS(ctx, j.did, c.Host); confirmErr != nil {
				return confirmErr
			}
		}
		if j.did == a.owner {
			return err
		}
		if statusErr := palustre.VerifyRepoReadError(ctx, reader, j.did, err); statusErr != nil {
			return statusErr
		}
		return a.savePage(ctx, j, "", "", nil)
	}
	return a.savePage(ctx, j, cursor, next, records)
}

func (a *service) savePage(ctx context.Context, j verifyJob, cursor, nextCursor string, records map[string]string) error {
	accepted := false
	completed := false
	err := a.transaction(ctx, func(tx *sql.Tx) error {
		var current int64
		if err := tx.QueryRow("SELECT version FROM scans WHERE did=?", j.did).Scan(&current); err != nil {
			return err
		}
		if current != j.version {
			return nil
		}
		if cursor == "" {
			if err := clearPages(tx, j.did); err != nil {
				return err
			}
		}
		if nextCursor != "" {
			res, err := tx.Exec("INSERT OR IGNORE INTO page_history VALUES(?,?)", j.did, nextCursor)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				if err = clearPages(tx, j.did); err != nil {
					return err
				}
				_, err = tx.Exec("UPDATE scans SET continuation=0,due=unixepoch()+60 WHERE did=?", j.did)
				return err
			}
		}
		for key, target := range records {
			if _, err := tx.Exec("INSERT OR REPLACE INTO edges VALUES(?,?,?)", j.did, key, target); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT OR REPLACE INTO staged VALUES(?,?,?)", j.did, key, target); err != nil {
				return err
			}
		}
		if nextCursor != "" {
			if _, err := tx.Exec("INSERT OR REPLACE INTO pages(did,version,cursor,pds) VALUES(?,?,?,?)", j.did, j.version, nextCursor, j.pds); err != nil {
				return err
			}
			accepted = true
			// Yield to untouched and overdue candidates, including after a restart.
			_, err := tx.Exec("UPDATE scans SET continuation=1,queued=unixepoch(),attempts=0,due=unixepoch() WHERE did=?", j.did)
			return err
		}
		if _, err := tx.Exec("DELETE FROM edges WHERE actor=? AND target NOT IN (SELECT target FROM partial_targets WHERE did=?)", j.did, j.did); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO edges SELECT * FROM staged WHERE actor=?", j.did); err != nil {
			return err
		}
		if err := clearPages(tx, j.did); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM audit_wait WHERE did=?", j.did); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM scans WHERE did=?", j.did)
		completed = err == nil
		return err
	})
	if err == nil && (accepted || completed) {
		pagesCommitted.Inc()
	}
	if err == nil && completed {
		a.api.FollowInventoriesCompleted.Add(1)
	}
	return err
}
func clearPages(tx *sql.Tx, did string) error {
	for _, query := range []string{"DELETE FROM partial_targets WHERE did=?", "DELETE FROM staged WHERE actor=?", "DELETE FROM pages WHERE did=?", "DELETE FROM page_history WHERE did=?"} {
		if _, err := tx.Exec(query, did); err != nil {
			return err
		}
	}
	return nil
}
