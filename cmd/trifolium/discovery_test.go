package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestDiscoveryDeadlinesPersistAcrossRestart(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	const target = "did:example:target"
	const owner = "did:example:owner"

	open := func() *service {
		t.Helper()
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		a := &service{store: store{db}, owner: owner, interval: time.Hour}
		if err := a.initialize(ctx); err != nil {
			t.Fatal(err)
		}
		configs := map[string]clusterConfig{"garden": {Accounts: []string{target}, Threshold: 1}}
		if _, err := a.setupClusters(ctx, configs, 2); err != nil {
			t.Fatal(err)
		}
		return a
	}
	a := open()
	firstDue := time.Now().Add(time.Hour).Unix()
	secondDue := firstDue + 600
	if _, err := a.Exec("UPDATE discovery_schedule SET due=CASE backlinks WHEN 0 THEN ? ELSE ? END", firstDue, secondDue); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("UPDATE inventory_schedule SET due=?", secondDue); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	a = open()
	delay, err := a.backfillStep(ctx)
	if err != nil || delay < 59*time.Minute || delay > time.Hour {
		t.Fatalf("restart scheduled discovery after %s: %v", delay, err)
	}
}
