package main

import (
	"database/sql"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/trifolium/storage"
	"github.com/pressly/goose/v3"
)

func TestDiscoveryRotatesPagesAndResumesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	a := openStore(t, path)
	configure(t, a)
	execute(t, a, "DELETE FROM discovery_schedule WHERE target=? OR backlinks", targets[2])

	var requests []string
	client := &http.Client{Transport: remoteFunc(func(r *http.Request) (*http.Response, error) {
		query := r.URL.Query()
		target, cursor := query.Get("actor"), query.Get("cursor")
		requests = append(requests, target+"/"+cursor)
		next := "second"
		if cursor == "second" {
			next = "third"
		}
		return reply(200, `{"subject":{"did":"`+target+`"},"followers":[],"cursor":"`+next+`"}`), nil
	})}
	a.http = client
	a.interval = time.Hour
	_, err := a.backfillStep(t.Context())
	requireOK(t, err)
	requireOK(t, a.Close())

	a = openStore(t, path)
	a.http = client
	a.interval = time.Hour
	for range 3 {
		_, err := a.backfillStep(t.Context())
		requireOK(t, err)
	}
	want := []string{targets[0] + "/", targets[1] + "/", targets[0] + "/second", targets[1] + "/second"}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests %v, want %v", requests, want)
	}
}

func TestDiscoveryRetriesFailedContinuation(t *testing.T) {
	a := openStore(t, ":memory:")
	configure(t, a)
	execute(t, a, "DELETE FROM discovery_schedule WHERE target<>? OR backlinks", targets[0])
	a.interval = time.Hour
	var requests []string
	a.http = &http.Client{Transport: remoteFunc(func(r *http.Request) (*http.Response, error) {
		cursor := r.URL.Query().Get("cursor")
		requests = append(requests, cursor)
		if cursor == "" {
			return reply(200, `{"subject":{"did":"`+targets[0]+`"},"followers":[],"cursor":"tail"}`), nil
		}
		return reply(500, `{"error":"InternalServerError"}`), nil
	})}
	for range 3 {
		_, err := a.backfillStep(t.Context())
		requireOK(t, err)
		execute(t, a, "UPDATE discovery_schedule SET due=0")
	}
	_, err := a.backfillStep(t.Context())
	requireOK(t, err)
	if want := []string{"", "tail", "tail", ""}; !slices.Equal(requests, want) {
		t.Fatalf("requested cursors %v, want %v", requests, want)
	}
}

func TestDiscoveryDiscardsPageFromBeforeStreamRecovery(t *testing.T) {
	a := openStore(t, ":memory:")
	configure(t, a)
	requireOK(t, a.prepareStream(t.Context()))
	a.http = &http.Client{Transport: remoteFunc(func(r *http.Request) (*http.Response, error) {
		requireOK(t, a.resetStream(t.Context()))
		return reply(200, `{"subject":{"did":"`+targets[0]+`"},"followers":[{"did":"`+actor+`"}],"cursor":"tail"}`), nil
	})}
	_, err := a.backfillStep(t.Context())
	requireOK(t, err)
	var queued, paginated int
	requireOK(t, a.QueryRow(`SELECT (SELECT count(*) FROM scans WHERE did=?),
		(SELECT count(*) FROM discovery_schedule WHERE cursor<>'' OR due<>0)`, actor).Scan(&queued, &paginated))
	if queued != 0 || paginated != 0 {
		t.Fatalf("obsolete page queued %d candidates and advanced %d schedules", queued, paginated)
	}
}

func TestDiscoveryMigrationPreservesScheduledWork(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "state.db"))
	requireOK(t, err)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, storage.Migrations,
		goose.WithGoMigrations(goose.NewGoMigration(1, nil, nil), goose.NewGoMigration(3, nil, nil)))
	requireOK(t, err)
	_, err = provider.UpTo(t.Context(), 3)
	requireOK(t, err)
	_, err = db.Exec("INSERT INTO discovery_schedule VALUES(?,1,1234,7)", targets[0])
	requireOK(t, err)
	requireOK(t, storage.Migrate(t.Context(), db))
	var target, cursor string
	var due, generation int64
	var backlinks bool
	requireOK(t, db.QueryRow("SELECT target,backlinks,due,generation,cursor FROM discovery_schedule").
		Scan(&target, &backlinks, &due, &generation, &cursor))
	if target != targets[0] || !backlinks || due != 1234 || generation != 7 || cursor != "" {
		t.Fatalf("upgraded schedule = %s %t %d %d %q", target, backlinks, due, generation, cursor)
	}
}
