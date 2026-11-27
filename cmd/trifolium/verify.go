package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
}

func (a *service) claim(ctx context.Context, preferHint bool) (j verifyJob, err error) {
	err = a.transaction(ctx, func(tx *sql.Tx) error {
		hint := func() error {
			return tx.QueryRow(`SELECT id,did,rkey,target FROM hints WHERE due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,id LIMIT 1`).Scan(&j.id, &j.did, &j.key, &j.target)
		}
		scan := func() error {
			err := tx.QueryRow("SELECT did,version FROM scans WHERE did=? AND due<=unixepoch() AND did NOT IN (SELECT did FROM scanning)", a.owner).Scan(&j.did, &j.version)
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			return tx.QueryRow(`SELECT did,version FROM scans WHERE due<=unixepoch() AND did NOT IN(SELECT did FROM scanning) ORDER BY due,rowid LIMIT 1`).Scan(&j.did, &j.version)
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
func (a *service) acquireHost(host string) bool {
	a.hostMu.Lock()
	defer a.hostMu.Unlock()
	if a.hostReads == nil {
		a.hostReads = map[string]int{}
	}
	if a.hostReads[host] >= 2 {
		return false
	}
	a.hostReads[host]++
	return true
}
func (a *service) verify(ctx context.Context, preferHint bool) error {
	j, err := a.claim(ctx, preferHint)
	if err != nil {
		return err
	}
	defer a.ExecContext(context.WithoutCancel(ctx), "DELETE FROM scanning WHERE did=?", j.did)
	doc, err := a.api.Resolve(ctx, j.did)
	delay := 60
	if err == nil {
		if a.acquireHost(doc.PDS) {
			defer func() { a.hostMu.Lock(); a.hostReads[doc.PDS]--; a.hostMu.Unlock() }()
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
	var retry error
	if j.id != 0 {
		_, retry = a.ExecContext(ctx, "UPDATE hints SET due=unixepoch()+? WHERE id=?", delay, j.id)
	} else {
		_, retry = a.ExecContext(ctx, "UPDATE scans SET due=unixepoch()+? WHERE did=? AND version=?", delay, j.did, j.version)
	}
	if err != nil && j.did == a.owner {
		err = fmt.Errorf("owner follow inventory unavailable; list additions waiting: %w", err)
	}
	return errors.Join(err, retry)
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
	if err != nil && !palustre.XRPCCode(err, "RecordNotFound") {
		return err
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
	var cursor string
	err := a.QueryRowContext(ctx, "SELECT cursor FROM pages WHERE did=? AND version=?", j.did, j.version).Scan(&cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	page, err := atproto.RepoListRecords(ctx, c, followCollection, cursor, 100, j.did, false)
	if err != nil {
		return err
	}
	if page.Records == nil {
		return errors.New("follow inventory has no records array")
	}
	seen := map[string]bool{}
	records := map[string]string{}
	for _, record := range page.Records {
		if record == nil {
			return errors.New("follow inventory contains a null record")
		}
		key, subject, err := parseFollow(j.did, record.Uri, record.Value)
		if err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate follow record key %q", key)
		}
		seen[key] = true
		if a.targets[subject] || j.did == a.owner {
			records[key] = subject
		}
	}
	var next string
	if page.Cursor != nil {
		next = *page.Cursor
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
			if _, err := tx.Exec("INSERT OR REPLACE INTO pages VALUES(?,?,?)", j.did, j.version, nextCursor); err != nil {
				return err
			}
			// Yield to untouched and overdue candidates, including after a restart.
			_, err := tx.Exec("UPDATE scans SET due=unixepoch() WHERE did=?", j.did)
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
