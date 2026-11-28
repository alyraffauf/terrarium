package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type store struct{ *sql.DB }

func (s store) transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	start := time.Now()
	defer func() { txSeconds.Observe(time.Since(start).Seconds()) }()
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
	_, err = tx.Exec(`
DELETE FROM discovery_schedule WHERE target NOT IN (SELECT target FROM watched);
DELETE FROM discovery_cursors WHERE target NOT IN (SELECT target FROM watched);
DELETE FROM scanning;
DELETE FROM staged WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched);
DELETE FROM hints WHERE target NOT IN (SELECT target FROM watched);
DELETE FROM edges WHERE actor NOT IN (SELECT owner FROM binding) AND target NOT IN (SELECT target FROM watched);
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
