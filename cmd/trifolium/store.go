package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
)

type store struct{ *sql.DB }

const baseSchema = `
CREATE TABLE IF NOT EXISTS audit_wait(cluster TEXT,did TEXT,PRIMARY KEY(cluster,did));
CREATE INDEX IF NOT EXISTS audit_wait_by_did ON audit_wait(did);
CREATE TABLE IF NOT EXISTS binding(owner TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS discovery_schedule(target TEXT,backlinks INTEGER,due INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(target,backlinks));
CREATE TABLE IF NOT EXISTS inventory_schedule(id INTEGER PRIMARY KEY CHECK(id=1),due INTEGER NOT NULL DEFAULT 0);
INSERT OR IGNORE INTO inventory_schedule(id) VALUES(1);
CREATE TABLE IF NOT EXISTS stream_progress(id INTEGER PRIMARY KEY CHECK(id=1),endpoint TEXT NOT NULL,cursor_us INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS partial_targets(did TEXT,target TEXT,PRIMARY KEY(did,target));
CREATE TABLE IF NOT EXISTS cluster_targets(cluster TEXT,target TEXT,PRIMARY KEY(cluster,target));
CREATE TABLE IF NOT EXISTS watched(target TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS edges(actor TEXT,rkey TEXT,target TEXT,PRIMARY KEY(actor,rkey));
CREATE INDEX IF NOT EXISTS by_target ON edges(target,actor);
CREATE TABLE IF NOT EXISTS scans(did TEXT PRIMARY KEY,version INTEGER DEFAULT 1,due INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS scans_ready ON scans(due);
CREATE TABLE IF NOT EXISTS hints(id INTEGER PRIMARY KEY AUTOINCREMENT,did TEXT,rkey TEXT,target TEXT,due INTEGER DEFAULT 0,UNIQUE(did,rkey,target));
CREATE INDEX IF NOT EXISTS hints_ready ON hints(due,id);
CREATE TABLE IF NOT EXISTS scanning(did TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pages(did TEXT PRIMARY KEY,version INTEGER,cursor TEXT);
CREATE TABLE IF NOT EXISTS page_history(did TEXT,cursor TEXT,PRIMARY KEY(did,cursor));
CREATE TABLE IF NOT EXISTS staged(actor TEXT,rkey TEXT,target TEXT,PRIMARY KEY(actor,rkey));
CREATE TABLE IF NOT EXISTS cursor(endpoint TEXT PRIMARY KEY,seq INTEGER);
DROP VIEW IF EXISTS counts;
`

const membershipSchema = `
CREATE TABLE items(cluster TEXT NOT NULL,did TEXT NOT NULL,rkey TEXT NOT NULL,PRIMARY KEY(cluster,rkey));
CREATE INDEX items_by_did ON items(cluster,did);
CREATE TABLE published(cluster TEXT NOT NULL,did TEXT NOT NULL,PRIMARY KEY(cluster,did));
CREATE TABLE pending(cluster TEXT NOT NULL,did TEXT NOT NULL,PRIMARY KEY(cluster,did));
CREATE TABLE retries(cluster TEXT NOT NULL,did TEXT NOT NULL,due INTEGER NOT NULL,PRIMARY KEY(cluster,did));
CREATE TABLE descriptions(cluster TEXT NOT NULL,text TEXT NOT NULL,PRIMARY KEY(cluster,text));
`

func (s store) initialize(ctx context.Context) error {
	if _, err := s.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		var version int
		if err := tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
			return err
		}
		if version > 2 {
			return fmt.Errorf("unknown Trifolium database version %d", version)
		}
		var existing bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='items')").Scan(&existing); err != nil {
			return err
		}
		if _, err := tx.Exec(baseSchema); err != nil {
			return err
		}
		if existing {
			return nil
		}
		_, err := tx.Exec(membershipSchema + "PRAGMA user_version=2;")
		return err
	})
}

func (s store) transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func configureStore(tx *sql.Tx, owner string, targets map[string]bool) error {
	var bound string
	err := tx.QueryRow("SELECT owner FROM binding").Scan(&bound)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec("INSERT INTO binding VALUES(?)", owner)
	} else if err == nil && bound != owner {
		return fmt.Errorf("database belongs to %s", bound)
	}
	if err != nil {
		return err
	}
	// Existing paginated inventories did not check their earlier pages for newly
	// added targets. Keep those targets' independently verified edges on completion.
	for did := range targets {
		if _, err = tx.Exec(`INSERT OR IGNORE INTO partial_targets SELECT did,? FROM pages
 WHERE did<>? AND NOT EXISTS(SELECT 1 FROM watched WHERE target=?)`, did, owner, did); err != nil {
			return err
		}
	}
	if _, err = tx.Exec("DELETE FROM watched"); err != nil {
		return err
	}
	for did := range targets {
		if _, err = tx.Exec("INSERT INTO watched VALUES(?)", did); err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT OR IGNORE INTO discovery_schedule(target,backlinks) VALUES(?,0),(?,1)", did, did); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DELETE FROM discovery_schedule WHERE target NOT IN (SELECT target FROM watched); DELETE FROM scanning; DELETE FROM staged WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched); DELETE FROM hints WHERE target NOT IN (SELECT target FROM watched); DELETE FROM edges WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched);
 `)
	return err
}
func (s store) MemberKeys(ctx context.Context, clusterID, did string) ([]string, error) {
	rows, err := s.QueryContext(ctx, "SELECT rkey FROM items WHERE cluster=? AND did=? ORDER BY rkey", clusterID, did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
func (s store) RememberMember(ctx context.Context, clusterID, did, key string) error {
	_, err := s.ExecContext(ctx, "INSERT OR REPLACE INTO items VALUES(?,?,?)", clusterID, did, key)
	return err
}
func (s store) ForgetMember(ctx context.Context, clusterID, key string) error {
	_, err := s.ExecContext(ctx, "DELETE FROM items WHERE cluster=? AND rkey=?", clusterID, key)
	return err
}
