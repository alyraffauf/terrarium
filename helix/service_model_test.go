package helix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/internal/followers"
	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

// Exercise a single selection from the durable queue without scheduling retries.
func processBatch(ctx context.Context, targets []*listTarget, jobs []job, db *store) error {
	actors := activeActors(targets)
	if len(actors) == 0 {
		return nil
	}
	queue := readQueue{ReadPool: followers.NewReadPool[checked](ctx, len(jobs))}
	defer queue.Close()
	for _, work := range jobs {
		queue.start(targets[0].session, db, work, actors)
	}
	for range jobs {
		result, ok := queue.Next(ctx, time.Minute)
		if !ok {
			return fmt.Errorf("waiting for job: %v", ctx.Err())
		}
		if err := completeCheck(ctx, targets, db, result); err != nil {
			return err
		}
	}
	return nil
}

func FuzzServiceMembership(f *testing.F) {
	m.Service(f, false, func(t *testing.T, w *m.World) func(string, bool, int, bool) error {
		path := t.TempDir() + "/store"
		db, err := openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		client := &http.Client{Transport: w}
		owner := &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}}
		api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, owner)
		return func(did string, excluded bool, paused int, restart bool) error {
			if restart {
				if err := db.Close(); err != nil {
					return err
				}
				db, err = openStore(path)
				if err != nil {
					return err
				}
			}
			ctx := t.Context()
			following := map[string]bool{}
			if excluded {
				following[m.Subject] = true
			}
			if err := db.ApplyFollowSnapshot(ctx, 0, following); err != nil {
				return err
			}
			var targets []*listTarget
			for i := range 2 {
				target := listTarget{session: api, actor: m.Actors[i], listURI: m.List(i)}
				target.suspended.Store(i == paused)
				targets = append(targets, &target)
			}
			if err := db.Ingest(ctx, did, followers.Checkpoint{}, false); err != nil {
				return err
			}
			work := job{DID: did}
			if err := db.conn.QueryRow("SELECT generation FROM pending WHERE did=?", did).Scan(&work.Generation); err != nil {
				return err
			}
			return processBatch(ctx, targets, []job{work}, db)
		}
	})
}

func TestOwnerChangeDuringMembershipWrite(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := &m.World{Active: true, Follows: 1}
	w.BeforeRecordRead = func() {
		if err := db.Ingest(ctx, "", followers.Checkpoint{}, true); err != nil {
			t.Error(err)
		}
	}
	client := &http.Client{Transport: w}
	api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	target := &listTarget{actor: m.Actors[0], session: api, listURI: m.List(0)}
	if err := processBatch(ctx, []*listTarget{target}, []job{work}, db); err != nil {
		t.Fatal(err)
	}
	if w.BeforeRecordRead != nil || w.RecordCount() != 0 {
		t.Fatal("expected owner invalidation before write")
	}
	next, err := db.next(ctx)
	if err != nil || next != work {
		t.Fatalf("deferred job %+v want %+v %v", next, work, err)
	}
}

func TestInterruptedRemovalAfterMigration(t *testing.T) {
	path := t.TempDir() + "/db"
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	w := &m.World{Unresolved: true, RejectDelete: "old"}
	client := &http.Client{Transport: w}
	api := newSession(Config{WriteInterval: time.Nanosecond, Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.saveCandidatePDS(ctx, m.Subject, m.Hosts[0]); err != nil {
		t.Fatal(err)
	}
	keys := []string{"old", "new"}
	var targets []*listTarget
	for i, key := range keys {
		id := m.Actors[i]
		target := &listTarget{session: api, actor: id, listURI: m.List(i)}
		targets = append(targets, target)
		w.SeedItem(key, m.List(i), m.Subject)
		if err := db.RememberMember(ctx, id, m.Subject, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		work, err := db.next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := processBatch(ctx, targets, []job{work}, db); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			if w.Record("old") == nil || w.Record("new") != nil {
				t.Fatal("healthy target failed to progress independently")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			w.RejectDelete = ""
			if err := db.Ingest(ctx, m.Subject, followers.Checkpoint{}, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if w.RecordCount() != 0 {
		t.Fatal("retry retained deleted subject")
	}
}

func TestBatchProgressWhileResolverStalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	stalled := make(chan struct{})
	released := make(chan struct{})
	slow := m.Actors[1]
	w := &m.World{Active: true, Follows: 1}
	w.BeforeResolve = func(r *http.Request) error {
		if r.URL.Query().Get("identifier") == slow {
			close(stalled)
			select {
			case <-released:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-stalled:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.AfterCreate = func(did string) {
		if did == m.Subject {
			// A fresh exclusion arrives while the slower job is fetching public state.
			if err := db.ApplyFollowSnapshot(ctx, 0, map[string]bool{slow: true}); err != nil {
				t.Error(err)
			}
			close(released)
		}
	}
	client := &http.Client{Transport: w}
	api := newSession(Config{Slingshot: "https://resolver.example", WriteInterval: time.Nanosecond}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	var jobs []job
	for _, did := range []string{slow, m.Subject} {
		if err := db.queueScan(ctx, did); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job{DID: did, Generation: 1})
	}
	if err := processBatch(ctx, []*listTarget{{actor: m.Actors[0], session: api, listURI: m.List(0)}}, jobs, db); err != nil {
		t.Fatal(err)
	}
	if api.ChecksCompleted.Load() != 1 || w.RecordCount() != 1 {
		t.Fatalf("completed %d jobs with %d items", api.ChecksCompleted.Load(), w.RecordCount())
	}
}

type incompleteFollowInventory struct {
	*m.World
}

func (transport incompleteFollowInventory) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path != "/xrpc/com.atproto.repo.listRecords" {
		return transport.World.RoundTrip(request)
	}
	if request.URL.Query().Get("cursor") != "" {
		return nil, errors.New("follow inventory unavailable")
	}
	body := fmt.Sprintf(`{"cursor":"next","records":[{"uri":"at://%s/app.bsky.graph.follow/first","value":{"$type":"app.bsky.graph.follow","subject":"%s"}}]}`, m.Subject, m.Actors[0])
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestSuspendedTargetDoesNotBlockActiveMembership(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	world := &m.World{Active: true}
	client := &http.Client{Transport: incompleteFollowInventory{world}}
	api := newSession(Config{Slingshot: "https://resolver.example", WriteInterval: time.Nanosecond}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	active := &listTarget{actor: m.Actors[0], listURI: m.List(0), session: api}
	suspended := &listTarget{actor: m.Actors[1], listURI: m.List(1), session: api}
	suspended.suspended.Store(true)
	if err := processBatch(ctx, []*listTarget{active, suspended}, []job{work}, db); err != nil {
		t.Fatal(err)
	}
	if record := world.Record(recordKey(active.listURI + "\n" + m.Subject)); record == nil {
		t.Fatal("active list is missing the follower found on the first page")
	}
	if pending, err := db.queries.PendingCount(ctx); err != nil || pending != 0 {
		t.Fatalf("pending jobs = %d, err = %v", pending, err)
	}
}

func TestSuspendedBatchRetainsJobsUntilTargetResumes(t *testing.T) {
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := t.Context()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, m.Subject); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	world := &m.World{Active: true, Follows: 1}
	client := &http.Client{Transport: world}
	api := newSession(Config{Slingshot: "https://resolver.example", WriteInterval: time.Nanosecond}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	target := &listTarget{actor: m.Actors[0], listURI: m.List(0), session: api}
	targets := []*listTarget{target}
	target.suspended.Store(true)
	if err := processBatch(ctx, targets, []job{work}, db); err != nil {
		t.Fatal(err)
	}
	next, err := db.next(ctx)
	if err != nil || next != work {
		t.Fatalf("after suspension got job %+v and error %v, want %+v", next, err, work)
	}

	target.suspended.Store(false)
	if err := processBatch(ctx, targets, []job{next}, db); err != nil {
		t.Fatal(err)
	}
	if world.Record(recordKey(target.listURI+"\n"+m.Subject)) == nil {
		t.Fatal("resumed target did not add the queued follower")
	}
	if pending, err := db.queries.PendingCount(ctx); err != nil || pending != 0 {
		t.Fatalf("pending jobs = %d, err = %v", pending, err)
	}
}

// Keep the first reader stalled while more accounts than the pool can hold
// finish. Every other DID should run once, including the ninth candidate.
func TestReadPoolRefillsWhileOneCandidateStalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ApplyFollowSnapshot(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	const slow = "did:example:candidate0"
	world := &m.World{BeforeResolve: func(req *http.Request) error {
		if req.URL.Query().Get("identifier") == slow {
			<-req.Context().Done()
			return req.Context().Err()
		}
		return nil
	}}
	client := &http.Client{Transport: world}
	api := newSession(Config{Slingshot: "https://resolver.example"}, client, &xrpc.Client{Host: "https://owner.example", Client: client, Auth: &xrpc.AuthInfo{Did: m.Owner}})
	targets := []*listTarget{{actor: m.Actors[0], session: api, listURI: m.List(0)}}
	queue := readQueue{ReadPool: followers.NewReadPool[checked](ctx, readConcurrency)}
	defer queue.Close()
	for i := range readConcurrency + 2 {
		if err := db.queueScan(ctx, fmt.Sprintf("did:example:candidate%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for range readConcurrency + 1 {
		if err := queue.fill(ctx, targets, db); err != nil {
			t.Fatal(err)
		}
		result, ok := queue.Next(ctx, 5*time.Second)
		if !ok || result.err != nil || result.work.DID == slow || seen[result.work.DID] {
			t.Fatalf("unexpected result %+v, ready=%v", result, ok)
		}
		seen[result.work.DID] = true
		if err := completeCheck(ctx, targets, db, result); err != nil {
			t.Fatal(err)
		}
	}
	if !queue.Busy(slow) {
		t.Fatal("lost stalled reader")
	}
}
