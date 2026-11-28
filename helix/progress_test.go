package helix

import (
	"fmt"
	"testing"
	"time"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/internal/followers"
)

func TestProgressThroughRetryAndSupersession(t *testing.T) {
	ctx := t.Context()
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	checkErr(db.queueScan(ctx, "actor"))
	_, err = db.conn.Exec("UPDATE pending SET ready=100, ready_since=100 WHERE did='actor'")
	checkErr(err)
	before := time.Now().Unix()
	event := func() { t.Helper(); checkErr(db.Ingest(ctx, "actor", followers.Checkpoint{}, false)) }
	event()
	age, err := db.queries.OldestReadyAgeSeconds(ctx)
	checkErr(err)
	if age < before-100 || age > time.Now().Unix()-100 {
		t.Fatalf("promotion lost wait time: %d", age)
	}

	stale, err := db.next(ctx)
	checkErr(err)
	event()
	checkErr(db.finish(ctx, stale))
	if n := db.completions.count(time.Now()); n != 0 {
		t.Fatalf("superseded job counted: %d", n)
	}
	current, err := db.next(ctx)
	checkErr(err)
	checkErr(db.retry(ctx, current))
	var future bool
	checkErr(db.conn.QueryRow("SELECT ready_since=ready AND ready>unixepoch() FROM pending").Scan(&future))
	if !future {
		t.Fatal("retry must restart age when it becomes eligible")
	}
	event()
	var readyNow bool
	checkErr(db.conn.QueryRow("SELECT ready=0 AND ready_since BETWEEN ? AND unixepoch() FROM pending", before).Scan(&readyNow))
	if !readyNow {
		t.Fatal("live event did not make delayed job ready")
	}
	current, err = db.next(ctx)
	checkErr(err)
	checkErr(db.finish(ctx, current))
	if n := db.completions.count(time.Now()); n != 1 {
		t.Fatalf("completed accounts=%d", n)
	}
	age, err = db.queries.OldestReadyAgeSeconds(ctx)
	checkErr(err)
	if age != 0 {
		t.Fatalf("empty queue age=%d", age)
	}
}

// Repeated work for one DID counts once until its last completion falls out of
// the trailing minute. Check every prefix against the raw event history.
func FuzzCompletedAccountWindow(f *testing.F) {
	f.Add([]byte{0, 0, 1, 255, 32, 32, 32})
	f.Fuzz(func(t *testing.T, trace []byte) {
		var window recentCompletions
		type event struct {
			did string
			at  time.Time
		}
		history := []event{}
		now := time.Unix(0, 0)
		for _, op := range trace[:min(96, len(trace))] {
			now = now.Add(time.Duration(op>>2) * time.Second)
			did := string(rune('a' + op%4))
			window.record(did, now)
			history = append(history, event{did, now})
			want := map[string]bool{}
			for _, e := range history {
				if now.Sub(e.at) < time.Minute {
					want[e.did] = true
				}
			}
			if got := window.count(now); got != int64(len(want)) {
				t.Fatalf("unique completions=%d want %d", got, len(want))
			}
		}
		if got := window.count(now.Add(time.Minute)); got != 0 {
			t.Fatalf("idle completions=%d", got)
		}
	})
}

// A saturated priority queue must still service its oldest eligible account.
func TestBatchReservesOldestReadySlot(t *testing.T) {
	ctx := t.Context()
	db, err := openStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.conn.Exec(`INSERT INTO pending(did,ready,ready_since) VALUES ('old-scan',1,1),('future-retry',200,200)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if _, err := db.conn.Exec("INSERT INTO pending(did,ready,ready_since) VALUES (?,0,10)", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.queries.NextJobs(ctx, storage.NextJobsParams{Now: 100, Busy: "[]", PrioritySlots: 7, OldestSlot: 1})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Did] = true
	}
	if len(seen) != 8 || !seen["old-scan"] || seen["future-retry"] {
		t.Fatalf("batch %v", rows)
	}
}
