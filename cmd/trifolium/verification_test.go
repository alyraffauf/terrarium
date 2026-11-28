package main

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
)

func FuzzPagedVerification(f *testing.F) {
	f.Add([]byte{1, 2, 4, 8, 3, 0})
	f.Fuzz(func(t *testing.T, trace []byte) {
		a := openStore(t, ":memory:")
		configure(t, a)
		visible := map[string]string{"old": targets[0]}
		staged := map[string]string{}
		execute(t, a, "INSERT INTO edges VALUES(?,'old',?)", actor, targets[0])
		cursor := ""
		for i, b := range trace[:min(len(trace), 24)] {
			execute(t, a, "INSERT OR IGNORE INTO scans(did) VALUES(?)", actor)
			execute(t, a, "UPDATE scans SET due=0")
			job, err := a.claim(t.Context(), freshLane)
			requireOK(t, err)
			if _, err := a.claim(t.Context(), hintLane); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("concurrent claim", err)
			}
			key := fmt.Sprint(b % 4)
			subject := targets[int(b>>2)%3]
			next := fmt.Sprint(i)
			if b%5 == 0 {
				next = ""
			}
			cycle := b%5 == 2 && cursor != ""
			if cycle {
				next = cursor
			}
			stale := b%5 == 3
			bad := b%5 == 4
			connect(a, func(r *http.Request) (*http.Response, error) {
				if r.URL.Query().Get("cursor") != cursor {
					t.Fatalf("step %d request cursor %q want %q", i, r.URL.Query().Get("cursor"), cursor)
				}
				if stale {
					execute(t, a, dirty, actor)
				}
				if bad {
					return reply(200, `{"records":null}`), nil
				}
				return reply(200, fmt.Sprintf(`{"cursor":%q,"records":[{"uri":"at://%s/app.bsky.graph.follow/%s","value":{"$type":"app.bsky.graph.follow","subject":%q}}]}`, next, actor, key, subject)), nil
			})
			err = a.scanPage(t.Context(), a.api.Client, job)
			if (err != nil) != bad {
				t.Fatalf("step %d err %v", i, err)
			}
			switch {
			case bad:
			case stale || cycle:
				cursor = ""
				clear(staged)
			default:
				visible[key] = subject
				staged[key] = subject
				cursor = next
				if cursor == "" {
					visible = maps.Clone(staged)
					clear(staged)
				}
			}
			if got := pairs(t, a, "SELECT rkey,target FROM edges WHERE actor=?", actor); !maps.Equal(got, visible) {
				t.Fatalf("step %d edges %v want %v", i, got, visible)
			}
			execute(t, a, "DELETE FROM scanning")
		}
	})
}

func FuzzRecoveryTransaction(f *testing.F) {
	f.Add(uint32(42), true)
	f.Add(uint32(7), false)
	f.Fuzz(func(t *testing.T, n uint32, fail bool) {
		a := openStore(t, ":memory:")
		configure(t, a)
		execute(t, a, "INSERT INTO edges VALUES(?,'a',?)", actor, targets[0])
		requireOK(t, a.prepareStream(t.Context()))
		requireOK(t, a.prepareStream(t.Context()))
		seq := int64(n) + 1
		execute(t, a, "INSERT INTO cursor VALUES(?,?),('other',?)", a.stream, seq, seq+1)
		execute(t, a, "UPDATE stream_progress SET cursor_us=7000000")
		if fail {
			execute(t, a, "CREATE TRIGGER fault BEFORE UPDATE ON discovery_schedule BEGIN SELECT raise(ABORT,'fault'); END")
			if err := a.resetStream(t.Context()); err == nil {
				t.Fatal("missing fault")
			}
			got, err := a.resumeStream(t.Context())
			requireOK(t, err)
			if got != seq {
				t.Fatalf("rollback cursor %d want %d", got, seq)
			}
			execute(t, a, "DROP TRIGGER fault")
		}
		a.stream = "other"
		requireOK(t, a.prepareStream(t.Context()))
		got, err := a.resumeStream(t.Context())
		requireOK(t, err)
		if got != 2000000 {
			t.Fatalf("new endpoint cursor %d", got)
		}
		var generation, queued int
		requireOK(t, a.QueryRow("SELECT min(generation),(SELECT count(*) FROM scans WHERE did=?) FROM discovery_schedule", actor).Scan(&generation, &queued))
		if generation != 3 || queued != 1 {
			t.Fatal(generation, queued)
		}
	})
}

func FuzzHintInvalidation(f *testing.F) {
	for i := range 4 {
		f.Add(byte(i))
	}
	f.Fuzz(func(t *testing.T, b byte) {
		a := openStore(t, ":memory:")
		configure(t, a)
		target := targets[int(b)%3]
		key := fmt.Sprintf("key%d", b)
		execute(t, a, "INSERT INTO hints(did,rkey,target) VALUES(?,?,?)", actor, key, target)
		j, err := a.claim(t.Context(), hintLane)
		requireOK(t, err)
		connect(a, func(*http.Request) (*http.Response, error) {
			if b&1 != 0 {
				requireOK(t, a.ingest(t.Context(), []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","time":"2026-01-01T00:00:00Z","seq":1,"operation":"delete","did":%q,"collection":"app.bsky.graph.follow","rkey":%q}}`, actor, key))))
			}
			subject := target
			if b&2 != 0 {
				subject = owner
			}
			return reply(200, fmt.Sprintf(`{"uri":"at://%s/app.bsky.graph.follow/%s","value":{"$type":"app.bsky.graph.follow","subject":%q}}`, actor, key, subject)), nil
		})
		requireOK(t, a.checkHint(t.Context(), a.api.Client, j))
		want := map[string]string{}
		if b&3 == 0 {
			want[key] = target
		}
		if got := pairs(t, a, "SELECT rkey,target FROM edges"); !maps.Equal(want, got) {
			t.Fatal(got, want)
		}
	})
}

func FuzzClusterInputs(f *testing.F) {
	f.Add(byte(3), int8(2))
	f.Fuzz(func(t *testing.T, b byte, threshold int8) {
		id := "garden"
		account := "a.example"
		legacy := ""
		if b&1 != 0 {
			id = "default"
			legacy = "b.example"
		}
		if b&2 != 0 {
			account = " "
		}
		raw := fmt.Sprintf(`{%q:{"accounts":[%q],"threshold":%d}}`, id, account, threshold)
		if b&4 != 0 {
			raw = raw[:len(raw)-1]
		}
		_, err := parseClusters(raw, legacy, 1)
		if (err == nil) != (b&7 == 0 && threshold > 0) {
			t.Fatal(raw, legacy, err)
		}
		var handles []string
		for i := 0; i < int(b%4); i++ {
			handles = append(handles, fmt.Sprintf("@%s%d.example", strings.Repeat("a", int(b%60)), i))
		}
		line := clusterDescription(handles)
		if !isClusterDescription(line) || isClusterDescription(line+" More.") {
			t.Fatal(line)
		}
	})
}

func FuzzResumeVerification(f *testing.F) {
	f.Add(uint8(4), false)
	f.Add(uint8(1), true)
	f.Fuzz(func(t *testing.T, n uint8, stillFollows bool) {
		a := openStore(t, ":memory:")
		configure(t, a)
		did := fmt.Sprintf("did:example:r%d", n)
		execute(t, a, "INSERT INTO published VALUES('other',?)", did)
		execute(t, a, "INSERT INTO edges VALUES(?,'first',?),(?,'second',?)", did, targets[1], did, targets[2])
		_, err := a.setupClusters(t.Context(), map[string]clusterConfig{"garden": {Accounts: targets[:2], Threshold: 1}}, 1)
		requireOK(t, err)
		configure(t, a)
		var member string
		var include bool
		change := func() error { return a.QueryRow(nextChange, "other", 2, owner).Scan(&member, &include) }
		if err := change(); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("premature change %s %v", member, err)
		}
		job, err := a.claim(t.Context(), freshLane)
		requireOK(t, err)
		if job.did != did {
			t.Fatal(job)
		}
		page := map[string]string{"first": targets[1]}
		if stillFollows {
			page["second"] = targets[2]
		}
		requireOK(t, a.savePage(t.Context(), job, "", "", page))
		if got := a.api.FollowInventoriesCompleted.Load(); got != 1 {
			t.Fatalf("completed inventories = %d, want one", got)
		}
		err = change()
		if stillFollows {
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatal(member, err)
			}
		} else if err != nil || member != did || include {
			t.Fatal(member, include, err)
		}
	})
}

func FuzzMalformedStreamRecord(f *testing.F) {
	f.Add(uint8(1), true)
	f.Add(uint8(0), false)
	f.Fuzz(func(t *testing.T, b uint8, known bool) {
		a := openStore(t, ":memory:")
		configure(t, a)
		if known {
			execute(t, a, "INSERT INTO edges VALUES(?,'key',?)", actor, targets[0])
			execute(t, a, "INSERT INTO hints(did,rkey,target) VALUES(?,'key',?)", actor, targets[0])
		}
		seq := int(b) + 1
		requireOK(t, a.ingest(t.Context(), []byte(fmt.Sprintf(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":%d,"time":"2026-01-01T00:00:00Z","collection":"app.bsky.graph.follow","did":%q,"rkey":"key","operation":"update","record":{"$type":"app.bsky.graph.follow","subject":"invalid"}}}`, seq, actor))))
		var queued, edges, hints bool
		var cursor int
		requireOK(t, a.QueryRow(`SELECT EXISTS(SELECT 1 FROM scans WHERE did=?1),EXISTS(SELECT 1 FROM edges WHERE actor=?1 AND rkey='key' AND target=?2),EXISTS(SELECT 1 FROM hints WHERE did=?1),(SELECT seq FROM cursor WHERE endpoint=?3)`, actor, targets[0], a.stream).Scan(&queued, &edges, &hints, &cursor))
		if queued != known || edges != known || hints || cursor != seq {
			t.Fatal(queued, edges, hints, cursor)
		}
	})
}

func TestOwnerInventorySkipsMalformedFollowValues(t *testing.T) {
	a := openStore(t, ":memory:")
	configure(t, a)
	execute(t, a, dirty, owner)
	job, err := a.claim(t.Context(), freshLane)
	requireOK(t, err)
	connect(a, func(*http.Request) (*http.Response, error) {
		return reply(200, fmt.Sprintf(`{"records":[
			{"uri":"at://%s/app.bsky.graph.follow/bad","value":{"$type":"example.unknown.record","subject":123}},
			{"uri":"at://%s/app.bsky.graph.follow/valid","value":{"$type":"app.bsky.graph.follow","subject":%q}}
		]}`, owner, owner, actor)), nil
	})
	requireOK(t, a.scanPage(t.Context(), a.api.Client, job))
	if got := pairs(t, a, "SELECT rkey,target FROM edges WHERE actor=?", owner); !maps.Equal(got, map[string]string{"valid": actor}) {
		t.Fatalf("owner follows = %v", got)
	}
	err = a.QueryRow("SELECT did FROM scans WHERE did=?", owner).Scan(new(string))
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("owner scan still pending: %v", err)
	}
}

func TestUnavailableFollowInventory(t *testing.T) {
	for _, tc := range []struct {
		name       string
		did        string
		status     int
		statusBody string
		superseded bool
		keepEdges  bool
	}{
		{name: "inactive", did: actor, status: 200, statusBody: `{"active":false}`},
		{name: "deleted", did: actor, status: 400, statusBody: `{"error":"RepoNotFound"}`},
		{name: "temporarily unavailable", did: actor, status: 503, statusBody: `{"error":"RepoNotFound"}`, keepEdges: true},
		{name: "still active", did: actor, status: 200, statusBody: `{"active":true}`, keepEdges: true},
		{name: "superseded", did: actor, status: 200, statusBody: `{"active":false}`, superseded: true},
		{name: "unavailable owner", did: owner, keepEdges: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := openStore(t, ":memory:")
			configure(t, a)
			execute(t, a, dirty, tc.did)
			job, err := a.claim(t.Context(), freshLane)
			requireOK(t, err)
			requireOK(t, a.savePage(t.Context(), job, "", "more", map[string]string{"old": targets[0]}))
			connect(a, func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/xrpc/com.atproto.repo.listRecords":
					return reply(400, `{"error":"RepoNotFound"}`), nil
				case "/xrpc/com.atproto.sync.getRepoStatus":
					if tc.superseded {
						execute(t, a, dirty, tc.did)
					}
					body := strings.TrimSuffix(tc.statusBody, "}") + fmt.Sprintf(`,"did":%q}`, tc.did)
					return reply(tc.status, body), nil
				default:
					t.Fatalf("unexpected request to %s", r.URL)
					return nil, nil
				}
			})
			err = a.scanPage(t.Context(), a.api.Client, job)
			wantErr := tc.status == 0 || tc.status == 503 || strings.Contains(tc.statusBody, `"active":true`)
			if (err != nil) != wantErr {
				t.Fatalf("scan error = %v, want error %t", err, wantErr)
			}
			wantEdges := map[string]string{}
			if wantErr || tc.superseded {
				wantEdges["old"] = targets[0]
			}
			if got := pairs(t, a, "SELECT rkey,target FROM edges WHERE actor=?", tc.did); !maps.Equal(got, wantEdges) {
				t.Fatalf("follows = %v, want %v", got, wantEdges)
			}
		})
	}
}

func TestFailedVerificationReleasesWorkerAndPersistsBackoff(t *testing.T) {
	a := openStore(t, ":memory:")
	configure(t, a)
	execute(t, a, "DELETE FROM scans")
	connect(a, func(*http.Request) (*http.Response, error) { return reply(503, `{"error":"Unavailable"}`), nil })
	a.api.HTTP = a.api.Client.Client
	a.api.Config.Slingshot = "https://resolver.invalid"
	for _, did := range []string{actor, a.owner} {
		execute(t, a, "INSERT INTO scans(did) VALUES(?)", did)
		for i := range 8 {
			requireOK(t, a.verify(t.Context(), freshLane))
			var attempts, delay, claimed int
			requireOK(t, a.QueryRow("SELECT attempts,due-unixepoch(),(SELECT count(*) FROM scanning) FROM scans WHERE did=?", did).Scan(&attempts, &delay, &claimed))
			want := min(3600, 60<<i)
			if did == a.owner {
				want = 60
			}
			if attempts != min(6, i+1) || claimed != 0 || delay > want || delay < want-1 {
				t.Fatalf("attempt %d: attempts=%d delay=%d claimed=%d", i, attempts, delay, claimed)
			}
			execute(t, a, "UPDATE scans SET due=0 WHERE did=?", did)
		}
		execute(t, a, "DELETE FROM scans WHERE did=?", did)
	}
}

func TestClaimReservesLanesAndPrioritizesOwner(t *testing.T) {
	a := openStore(t, ":memory:")
	execute(t, a, "INSERT INTO scans(did,continuation) VALUES('old',1),('new',0),(?,0)", a.owner)
	execute(t, a, "INSERT INTO hints(did,rkey,target) VALUES('hint','key','target')")
	for _, tc := range []struct {
		lane verificationLane
		want string
	}{{hintLane, a.owner}, {continuationLane, "old"}, {hintLane, "hint"}, {continuationLane, "new"}} {
		j, err := a.claim(t.Context(), tc.lane)
		if err != nil || j.did != tc.want {
			t.Fatalf("lane %d got %q, %v; want %s", tc.lane, j.did, err, tc.want)
		}
	}
}
