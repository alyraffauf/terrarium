package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lex "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type service struct {
	wakeBackfill    chan struct{}
	nextDescription time.Time
	store
	api                 *palustre.Session
	owner, list, stream string
	targets             map[string]bool
	threshold           int
	interval            time.Duration
	http                *http.Client
}

func (a *service) discover(ctx context.Context, target string, backlinks bool) error {
	host, method := "https://public.api.bsky.app", "app.bsky.graph.getFollowers"
	params := map[string]any{"actor": target, "limit": 100}
	if backlinks {
		host, method = "https://constellation.microcosm.blue", "blue.microcosm.links.getBacklinks"
		params = map[string]any{"subject": target, "source": "app.bsky.graph.follow:subject", "limit": 100}
	}
	client := &xrpc.Client{Host: host, Client: a.http}
	seen := map[string]bool{}
	for {
		type actor struct {
			DID string `json:"did"`
		}
		var page struct {
			Cursor             string
			Followers, Records []actor
		}
		if err := client.LexDo(ctx, lex.Query, "", method, params, nil, &page); err != nil {
			return err
		}
		err := a.transaction(ctx, func(tx *sql.Tx) error {
			for _, r := range append(page.Followers, page.Records...) {
				if _, err := syntax.ParseDID(r.DID); err != nil {
					return err
				}
				if _, err := tx.Exec("INSERT OR IGNORE INTO scans(did) VALUES(?)", r.DID); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if page.Cursor == "" {
			return nil
		}
		if seen[page.Cursor] {
			return errors.New("repeated backfill cursor")
		}
		seen[page.Cursor] = true
		params["cursor"] = page.Cursor
	}
}

const dirty = `INSERT INTO scans(did) VALUES(?) ON CONFLICT(did) DO UPDATE SET version=version+1,due=0`

func (a *service) backfill(ctx context.Context) {
	for ctx.Err() == nil {
		_, err := a.ExecContext(ctx, "INSERT OR IGNORE INTO scans(did) SELECT DISTINCT actor FROM edges")
		if err != nil {
			slog.Error("schedule inventory", "error", err)
		}
		for target := range a.targets {
			for _, backlinks := range []bool{false, true} {
				if err := a.discover(ctx, target, backlinks); err != nil {
					slog.Warn("follower discovery", "target", target, "constellation", backlinks, "error", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.interval):
		case <-a.wakeBackfill:
		}
	}
}
func (a *service) listen(ctx context.Context) {
	for ctx.Err() == nil {
		var seq int64
		err := a.QueryRowContext(ctx, "SELECT coalesce(max(seq),0) FROM cursor WHERE endpoint=?", a.stream).Scan(&seq)
		if err == nil {
			if seq == 0 {
				seq = time.Now().Add(-time.Minute).UnixMicro()
			}
			err = palustre.Stream(ctx, a.stream, seq, func(raw []byte) error { return a.ingest(ctx, raw) })
		}
		if errors.Is(err, palustre.ErrStreamGap) {
			_, err = a.ExecContext(ctx, "DELETE FROM cursor; UPDATE scans SET version=version+1; INSERT OR IGNORE INTO scans(did) SELECT DISTINCT actor FROM edges;")
			if err == nil {
				_, err = a.ExecContext(ctx, dirty, a.owner)
				select {
				case a.wakeBackfill <- struct{}{}:
				default:
				}
			}
		}
		slog.Warn("reconnecting Jetstream", "error", err)
		palustre.Pause(ctx, 2*time.Second)
	}
}

func (a *service) ingest(ctx context.Context, raw []byte) error {
	var f palustre.Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return palustre.ErrStreamGap
	}
	p := f.Payload
	if f.Type == "error" || p.Name == "OutdatedCursor" || p.Name == "FutureCursor" {
		return palustre.ErrStreamGap
	}
	if p.Seq <= 0 {
		return nil
	}
	kind := strings.TrimPrefix(p.Type, palustre.EventPrefix)
	var subject struct{ Subject string }
	if kind == "commit" && p.Collection == "app.bsky.graph.follow" && p.Operation != "delete" {
		if err := json.Unmarshal(p.Record, &subject); err != nil {
			return palustre.ErrStreamGap
		}
	}
	// Unrelated creates cannot overwrite a previous record. Replaying them is harmless.
	if kind == "commit" && p.Operation == "create" && !a.targets[subject.Subject] && p.DID != a.owner {
		return nil
	}
	if kind != "sync" && !(kind == "commit" && p.Collection == "app.bsky.graph.follow") {
		return nil
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		if kind == "commit" {
			key := p.Rkey
			if _, err := syntax.ParseRecordKey(key); err != nil {
				return palustre.ErrStreamGap
			}
			if _, err := tx.Exec("DELETE FROM edges WHERE actor=? AND rkey=?", p.DID, key); err != nil {
				return err
			}
			if p.Operation != "delete" && (a.targets[subject.Subject] || p.DID == a.owner) {
				if _, err := syntax.ParseDID(subject.Subject); err != nil {
					return palustre.ErrStreamGap
				}
				if _, err := tx.Exec("INSERT INTO edges VALUES(?,?,?)", p.DID, key, subject.Subject); err != nil {
					return err
				}
			}
			// Invalidate any inventory started before this record changed.
			if _, err := tx.Exec("UPDATE scans SET version=version+1,due=0 WHERE did=?", p.DID); err != nil {
				return err
			}
		} else if _, err := tx.Exec(dirty, p.DID); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT OR REPLACE INTO cursor VALUES(?,?)", a.stream, p.Seq)
		return err
	})
}
func (a *service) scan(ctx context.Context) error {
	var actor string
	var version int64
	err := a.QueryRowContext(ctx, "SELECT did,version FROM scans WHERE due<=unixepoch() ORDER BY (did=?) DESC,rowid LIMIT 1", a.owner).Scan(&actor, &version)
	if err != nil {
		return err
	}
	doc, err := a.api.Resolve(ctx, actor)
	var follows map[string]string
	if err == nil {
		targets := a.targets
		if actor == a.owner {
			targets = nil
		}
		follows, err = inventory(ctx, &xrpc.Client{Host: doc.PDS, Client: a.http}, actor, targets)
	}
	if err != nil {
		_, retryErr := a.ExecContext(ctx, "UPDATE scans SET due=unixepoch()+60 WHERE did=? AND version=?", actor, version)
		return errors.Join(fmt.Errorf("inventory %s: %w", actor, err), retryErr)
	}
	return a.snapshot(ctx, actor, a.owner, version, follows, a.targets)
}

// Record keys distinguish duplicate follows and identify the subject of a delete.
func inventory(ctx context.Context, client *xrpc.Client, actor string, targets map[string]bool) (map[string]string, error) {
	result := map[string]string{}
	params := map[string]any{"repo": actor, "collection": "app.bsky.graph.follow", "limit": 100}
	seen := map[string]bool{}
	for {
		var page struct {
			Cursor  string
			Records []struct {
				URI   string
				Value struct{ Subject string }
			}
		}
		if err := client.LexDo(ctx, lex.Query, "", "com.atproto.repo.listRecords", params, nil, &page); err != nil {
			return nil, err
		}
		for _, record := range page.Records {
			uri, err := syntax.ParseATURI(record.URI)
			if err != nil {
				return nil, err
			}
			if uri.Authority().String() != actor || uri.Collection().String() != "app.bsky.graph.follow" || uri.RecordKey() == "" {
				return nil, fmt.Errorf("unexpected follow URI %s", record.URI)
			}
			if targets == nil || targets[record.Value.Subject] {
				result[uri.RecordKey().String()] = record.Value.Subject
			}
		}
		if page.Cursor == "" {
			return result, nil
		}
		if seen[page.Cursor] {
			return nil, errors.New("repeated follow inventory cursor")
		}
		seen[page.Cursor] = true
		params["cursor"] = page.Cursor
	}
}
