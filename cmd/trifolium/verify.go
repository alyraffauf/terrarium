package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lex "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

const followCollection = "app.bsky.graph.follow"

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
	return errors.Join(err, retry)
}

type followRecord struct {
	URI   string
	Value struct {
		Subject string
		Type    string `json:"$type"`
	}
}

func (a *service) checkHint(ctx context.Context, c *xrpc.Client, j verifyJob) error {
	var r followRecord
	err := c.LexDo(ctx, lex.Query, "", "com.atproto.repo.getRecord", map[string]any{"repo": j.did, "collection": followCollection, "rkey": j.key}, nil, &r)
	if err != nil && !palustre.XRPCCode(err, "RecordNotFound") {
		return err
	}
	if err == nil && r.URI != "at://"+j.did+"/"+followCollection+"/"+j.key {
		return errors.New("unexpected follow record URI")
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		// A Jetstream mutation deletes this exact hint, invalidating an older HTTP response.
		if err == nil && r.Value.Type == followCollection && r.Value.Subject == j.target {
			if _, e := tx.Exec("INSERT OR REPLACE INTO edges SELECT did,rkey,target FROM hints WHERE id=?", j.id); e != nil {
				return e
			}
		}
		_, e := tx.Exec("DELETE FROM hints WHERE id=?", j.id)
		return e
	})
}

func (a *service) scanPage(ctx context.Context, c *xrpc.Client, j verifyJob) error {
	var cursor string
	err := a.QueryRowContext(ctx, "SELECT cursor FROM pages WHERE did=? AND version=?", j.did, j.version).Scan(&cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	params := map[string]any{"repo": j.did, "collection": followCollection, "limit": 100}
	if cursor != "" {
		params["cursor"] = cursor
	}
	var page struct {
		Cursor  string
		Records []followRecord
	}
	if err = c.LexDo(ctx, lex.Query, "", "com.atproto.repo.listRecords", params, nil, &page); err != nil {
		return err
	}
	records := map[string]string{}
	for _, r := range page.Records {
		uri, err := syntax.ParseATURI(r.URI)
		if err != nil {
			return err
		}
		if uri.Authority().String() != j.did || uri.Collection().String() != followCollection || uri.RecordKey() == "" {
			return fmt.Errorf("unexpected inventory URI %s", r.URI)
		}
		if a.targets[r.Value.Subject] || j.did == a.owner {
			records[uri.RecordKey().String()] = r.Value.Subject
		}
	}
	return a.savePage(ctx, j, cursor, page.Cursor, records)
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
		for key, target := range records {
			if _, err := tx.Exec("INSERT OR REPLACE INTO edges VALUES(?,?,?)", j.did, key, target); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT OR REPLACE INTO staged VALUES(?,?,?)", j.did, key, target); err != nil {
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
			if _, err = tx.Exec("INSERT OR REPLACE INTO pages VALUES(?,?,?)", j.did, j.version, nextCursor); err != nil {
				return err
			}
			// Yield to untouched and overdue candidates, including after a restart.
			_, err = tx.Exec("UPDATE scans SET due=unixepoch() WHERE did=?", j.did)
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
