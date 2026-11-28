package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lex "github.com/bluesky-social/indigo/lex/util"
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

func (a *service) claim(ctx context.Context, preferHint bool) (j verifyJob, err error) {
	err = a.transaction(ctx, func(tx *sql.Tx) error {
		hint := func() error {
			return tx.QueryRow(`SELECT id,did,rkey,target,attempts FROM hints WHERE due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,id LIMIT 1`).Scan(&j.id, &j.did, &j.key, &j.target, &j.attempts)
		}
		scan := func() error {
			err := tx.QueryRow("SELECT did,version,attempts FROM scans WHERE did=? AND due<=unixepoch() AND did NOT IN (SELECT did FROM scanning)", a.owner).Scan(&j.did, &j.version, &j.attempts)
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			return tx.QueryRow(`SELECT did,version,attempts FROM scans WHERE due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,rowid LIMIT 1`).Scan(&j.did, &j.version, &j.attempts)
		}
		first, second := scan, hint
		if preferHint {
			first, second = hint, scan
		}
		err := first()
		if errors.Is(err, sql.ErrNoRows) {
			err = second()
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
	if a.hostReads[host] >= 2 {
		return nil
	}
	a.hostReads[host]++
	return func() {
		a.hostMu.Lock()
		defer a.hostMu.Unlock()
		a.hostReads[host]--
		if a.hostReads[host] == 0 {
			delete(a.hostReads, host)
		}
	}
}
func (a *service) verify(ctx context.Context, preferHint bool) error {
	j, err := a.claim(ctx, preferHint)
	if err != nil {
		return err
	}
	defer a.ExecContext(context.WithoutCancel(ctx), "DELETE FROM scanning WHERE did=?", j.did)
	doc, cached, err := a.api.CachedDID(ctx, j.did)
	j.cached = cached
	delay := 60
	if err == nil {
		if release := a.acquireHost(doc.PDS); release != nil {
			defer release()
			client := &xrpc.Client{Host: doc.PDS, Client: a.http}
			if j.id != 0 {
				err = a.checkHint(ctx, client, j)
			} else {
				err = a.scanPage(ctx, client, j)
			}
		} else {
			delay = 2
		}
	}
	if err == nil && delay != 2 {
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
		slog.Warn("follow verification deferred", "did", j.did, "retry_seconds", delay, "error", err)
	}
	return nil
}

func parseFollow(did, rawURI string, value *lex.LexiconTypeDecoder) (key, subject string, err error) {
	uri, err := syntax.ParseATURI(rawURI)
	if err != nil {
		return "", "", err
	}
	if uri.Authority().String() != did || uri.Collection().String() != followCollection || uri.RecordKey() == "" {
		return "", "", fmt.Errorf("unexpected follow record URI %q", rawURI)
	}
	if value == nil {
		return "", "", fmt.Errorf("missing follow record value for %s", uri)
	}
	record, ok := value.Val.(*bsky.GraphFollow)
	if !ok || record == nil || record.LexiconTypeID != followCollection {
		return "", "", fmt.Errorf("unexpected follow record type for %s", uri)
	}
	if _, err := syntax.ParseDID(record.Subject); err != nil {
		return "", "", fmt.Errorf("invalid follow subject for %s: %w", uri, err)
	}
	return uri.RecordKey().String(), record.Subject, nil
}

func (a *service) checkHint(ctx context.Context, c *xrpc.Client, j verifyJob) error {
	record, err := atproto.RepoGetRecord(ctx, c, "", followCollection, j.did, j.key)
	if j.cached && palustre.MayHaveMoved(err) {
		if confirmErr := a.api.ConfirmPDS(ctx, j.did, c.Host); confirmErr != nil {
			return confirmErr
		}
	}
	if err != nil && !palustre.XRPCCode(err, "RecordNotFound") {
		if statusErr := palustre.VerifyRepoReadError(ctx, c, j.did, err); statusErr != nil {
			return statusErr
		}
	}
	var matches bool
	if err == nil {
		key, subject, err := parseFollow(j.did, record.Uri, record.Value)
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
	next, err := palustre.ListFollowPage(ctx, c, j.did, cursor, func(key, subject string) error {
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
		if statusErr := palustre.VerifyRepoReadError(ctx, c, j.did, err); statusErr != nil {
			return statusErr
		}
		return a.savePage(ctx, j, "", "", nil)
	}
	return a.savePage(ctx, j, cursor, next, records)
}

func (a *service) savePage(ctx context.Context, j verifyJob, cursor, nextCursor string, records map[string]string) error {
	return a.transaction(ctx, func(tx *sql.Tx) error {
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
				_, err = tx.Exec("UPDATE scans SET due=unixepoch()+60 WHERE did=?", j.did)
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
			// Yield to untouched and overdue candidates, including after a restart.
			_, err := tx.Exec("UPDATE scans SET attempts=0,due=unixepoch() WHERE did=?", j.did)
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
		return err
	})
}
func clearPages(tx *sql.Tx, did string) error {
	for _, query := range []string{"DELETE FROM partial_targets WHERE did=?", "DELETE FROM staged WHERE actor=?", "DELETE FROM pages WHERE did=?", "DELETE FROM page_history WHERE did=?"} {
		if _, err := tx.Exec(query, did); err != nil {
			return err
		}
	}
	return nil
}
