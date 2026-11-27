package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStreamRecoveryPreservesWorkAndInvalidatesOldEndpointCursor(t *testing.T) {
	ctx := t.Context()
	a := openTestService(t)
	const target = "did:example:target"
	const actor = "did:example:actor"
	a.stream = "first"
	if _, err := a.setupClusters(ctx, map[string]clusterConfig{"test": {Accounts: []string{target}, Threshold: 1}}, 2); err != nil {
		t.Fatal(err)
	}
	if err := a.prepareStream(ctx); err != nil {
		t.Fatal(err)
	}
	raw := func(seq int, subject string) []byte {
		return []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"time":"2026-01-01T00:00:00Z","collection":"app.bsky.graph.follow","did":%q,"rkey":"key","operation":"update","record":{"$type":"app.bsky.graph.follow","subject":%q}}}`, seq, actor, subject))
	}

	for _, event := range [][]byte{raw(10, target), raw(11, "invalid")} {
		if err := a.ingest(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	var cursor, queued, edges, discoveryGeneration int
	err := a.QueryRow(`SELECT (SELECT seq FROM cursor WHERE endpoint='first'),(SELECT count(*) FROM scans WHERE did=?),
(SELECT count(*) FROM edges WHERE actor=?),(SELECT max(generation) FROM discovery_schedule)`, actor, actor).Scan(&cursor, &queued, &edges, &discoveryGeneration)
	if err != nil || cursor != 11 || queued != 1 || edges != 1 || discoveryGeneration != 1 {
		t.Fatalf("malformed record affected unrelated state: cursor=%d queued=%d edges=%d discovery generation=%d err=%v", cursor, queued, edges, discoveryGeneration, err)
	}

	if err := a.ingest(ctx, []byte(strings.Replace(string(raw(12, "invalid")), actor, "did:example:unrelated", 1))); err != nil {
		t.Fatal(err)
	}
	if err := a.QueryRow("SELECT (SELECT seq FROM cursor WHERE endpoint='first'),(SELECT count(*) FROM scans WHERE did='did:example:unrelated')").Scan(&cursor, &queued); err != nil || cursor != 12 || queued != 0 {
		t.Fatalf("unrelated malformed record: cursor=%d queued=%d err=%v", cursor, queued, err)
	}

	if _, err := a.Exec("INSERT INTO cursor VALUES('second',99); UPDATE discovery_schedule SET due=9999999999"); err != nil {
		t.Fatal(err)
	}
	a.stream = "second"
	if err := a.prepareStream(ctx); err != nil {
		t.Fatal(err)
	}
	witnessed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if seq, err := a.resumeStream(ctx); err != nil || seq != witnessed.Add(-5*time.Second).UnixMicro() {
		t.Fatalf("switched endpoint resumed from %d: %v", seq, err)
	}
	var due int64
	if err := a.QueryRow("SELECT max(due),max(generation) FROM discovery_schedule").Scan(&due, &discoveryGeneration); err != nil || due != 0 || discoveryGeneration != 2 {
		t.Fatalf("endpoint switch failed to schedule rediscovery: due=%d generation=%d err=%v", due, discoveryGeneration, err)
	}
	if err := a.ingest(ctx, raw(3, target)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec(`CREATE TRIGGER fail_recovery BEFORE UPDATE ON discovery_schedule BEGIN SELECT raise(ABORT,'failed recovery'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := a.resetStream(ctx); err == nil {
		t.Fatal("expected gap recovery to fail")
	}
	if seq, err := a.resumeStream(ctx); err != nil || seq != 3 {
		t.Fatalf("failed gap recovery advanced cursor: %d %v", seq, err)
	}
}

func TestLegacyStreamWithoutCursorSchedulesRecoveryOnce(t *testing.T) {
	ctx := t.Context()
	a := openTestService(t)
	const target = "did:example:target"
	const actor = "did:example:actor"
	a.stream = "stream"
	configs := map[string]clusterConfig{"test": {Accounts: []string{target}, Threshold: 1}}
	if _, err := a.setupClusters(ctx, configs, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("INSERT INTO edges VALUES(?,'key',?); UPDATE discovery_schedule SET due=9999999999", actor, target); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := a.prepareStream(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var due, generation, scans int
	err := a.QueryRow("SELECT max(due),max(generation),(SELECT count(*) FROM scans WHERE did=?) FROM discovery_schedule", actor).Scan(&due, &generation, &scans)
	if err != nil || due != 0 || generation != 2 || scans != 1 {
		t.Fatalf("legacy recovery due=%d generation=%d scans=%d err=%v", due, generation, scans, err)
	}
}
