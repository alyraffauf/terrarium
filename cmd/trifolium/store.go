package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
)

type store struct{ *sql.DB }

const schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS descriptions(text TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS binding(owner TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS cluster_settings(cluster TEXT PRIMARY KEY,threshold INTEGER);
CREATE TABLE IF NOT EXISTS discovered(target TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS partial_targets(did TEXT,target TEXT,PRIMARY KEY(did,target));
CREATE TABLE IF NOT EXISTS cluster_targets(cluster TEXT,target TEXT,PRIMARY KEY(cluster,target));
CREATE TABLE IF NOT EXISTS watched(target TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS edges(actor TEXT,rkey TEXT,target TEXT,PRIMARY KEY(actor,rkey));
CREATE INDEX IF NOT EXISTS by_target ON edges(target,actor);
CREATE TABLE IF NOT EXISTS scans(did TEXT PRIMARY KEY,version INTEGER DEFAULT 1,due INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS scans_ready ON scans(due);
CREATE TABLE IF NOT EXISTS items(did TEXT PRIMARY KEY,rkey TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS published(did TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pending(did TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS retries(did TEXT PRIMARY KEY,due INTEGER);
CREATE TABLE IF NOT EXISTS hints(id INTEGER PRIMARY KEY AUTOINCREMENT,did TEXT,rkey TEXT,target TEXT,due INTEGER DEFAULT 0,UNIQUE(did,rkey,target));
CREATE INDEX IF NOT EXISTS hints_ready ON hints(due,id);
CREATE TABLE IF NOT EXISTS scanning(did TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pages(did TEXT PRIMARY KEY,version INTEGER,cursor TEXT);
CREATE TABLE IF NOT EXISTS page_history(did TEXT,cursor TEXT,PRIMARY KEY(did,cursor));
CREATE TABLE IF NOT EXISTS staged(actor TEXT,rkey TEXT,target TEXT,PRIMARY KEY(actor,rkey));
CREATE TABLE IF NOT EXISTS cursor(endpoint TEXT PRIMARY KEY,seq INTEGER);
CREATE VIEW IF NOT EXISTS counts AS SELECT actor,count(DISTINCT target) AS total FROM edges JOIN watched USING(target)
 WHERE actor NOT IN (SELECT target FROM edges JOIN binding ON edges.actor=binding.owner) AND actor NOT IN (SELECT owner FROM binding) GROUP BY actor;
`

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
	}
	_, err = tx.Exec(`DELETE FROM discovered WHERE target NOT IN (SELECT target FROM watched); DELETE FROM scanning; DELETE FROM staged WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched); DELETE FROM hints WHERE target NOT IN (SELECT target FROM watched); DELETE FROM edges WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched);
 `)
	return err
}
func (s store) MemberKeys(ctx context.Context, clusterID, did string) ([]string, error) {
	var key string
	err := s.QueryRowContext(ctx, "SELECT rkey FROM items WHERE cluster=? AND did=?", clusterID, did).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return []string{key}, err
}
func (s store) RememberMember(ctx context.Context, clusterID, did, key string) error {
	_, err := s.ExecContext(ctx, "INSERT OR REPLACE INTO items VALUES(?,?,?)", clusterID, did, key)
	return err
}
func (s store) ForgetMember(ctx context.Context, clusterID, key string) error {
	_, err := s.ExecContext(ctx, "DELETE FROM items WHERE cluster=? AND rkey=?", clusterID, key)
	return err
}
