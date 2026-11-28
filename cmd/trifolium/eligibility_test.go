package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math/bits"
	"net/http"
	"testing"
)

// Exercise both membership queries through publish. The first repository read
// is after candidate selection and before BeforeWrite, so changing the graph
// there simulates a Jetstream update while publication is in flight.
func FuzzPublishEligibility(f *testing.F) {
	f.Add(byte(3), byte(3), byte(1), false)     // add at the threshold
	f.Add(byte(1|8), byte(1|8), byte(1), false) // two records for just one target
	f.Add(byte(3), byte(2), byte(1), false)     // unfollow during add
	f.Add(byte(1), byte(3), byte(1), false)     // follow during removal
	f.Add(byte(1), byte(1|16), byte(0), false)  // newly followed by owner
	f.Add(byte(1), byte(1|32), byte(0), false)  // owner inventory starts during add
	f.Add(byte(3), byte(3), byte(0), true)      // owner cannot join
	f.Add(byte(3), byte(67), byte(1), false)    // block during add
	f.Add(byte(67), byte(3), byte(1), false)    // unblock during removal
	f.Fuzz(func(t *testing.T, before, after, thresholdByte byte, self bool) {
		a := openStore(t, ":memory:")
		c := configure(t, a)[0] // garden watches targets[0] and targets[1]
		c.threshold = int(thresholdByte%3) + 1
		did := actor
		if self {
			did = owner
		}

		// Bits 0–2 add follows. Target 2 is watched by another cluster.
		// Bit 3 adds a second record for target 0. Bits 4 and 5 change the owner.
		setState := func(state byte) {
			execute(t, a, "DELETE FROM edges; DELETE FROM scans; DELETE FROM follower_blocks")
			for i := 0; i < 2; i++ {
				if state&(64<<i) != 0 {
					execute(t, a, "INSERT INTO follower_blocks VALUES(?,?)", targets[i], did)
				}
			}
			for i, target := range targets {
				if state&(1<<i) != 0 {
					execute(t, a, "INSERT INTO edges VALUES(?,?,?)", did, fmt.Sprint(i), target)
				}
			}
			if state&8 != 0 {
				execute(t, a, "INSERT INTO edges VALUES(?,'extra',?)", did, targets[0])
			}
			if state&16 != 0 {
				execute(t, a, "INSERT INTO edges VALUES(?,'friend',?)", owner, did)
			}
			if state&32 != 0 {
				execute(t, a, "INSERT INTO scans(did) VALUES(?)", owner)
			}
		}
		qualifies := func(state byte) bool {
			distinct := state & 3
			if state&8 != 0 {
				distinct |= 1
			}
			distinct &^= state >> 6
			return !self && state&16 == 0 && bits.OnesCount8(distinct) >= c.threshold
		}

		setState(before)
		include := qualifies(before)
		if !include {
			execute(t, a, "INSERT INTO published VALUES(?,?)", c.id, did)
			requireOK(t, a.RememberMember(t.Context(), c.id, did, "old"))
		}
		reads, writes := 0, 0
		connect(a, func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/xrpc/com.atproto.repo.getRecord":
				reads++
				setState(after)
				if include {
					return reply(404, `{"error":"RecordNotFound"}`), nil
				}
				return reply(200, fmt.Sprintf(`{"cid":"old","value":{"$type":"app.bsky.graph.listitem","list":%q,"subject":%q}}`, c.list, did)), nil
			case "/xrpc/com.atproto.repo.createRecord", "/xrpc/com.atproto.repo.deleteRecord":
				writes++
				if (r.URL.Path == "/xrpc/com.atproto.repo.createRecord") != include {
					t.Fatalf("wrong membership change: %s, include=%v", r.URL.Path, include)
				}
				return reply(200, "{}"), nil
			default:
				return nil, fmt.Errorf("unexpected request: %s", r.URL.Path)
			}
		})
		err := c.publish(t.Context())
		selected := !include || before&32 == 0
		allowed := selected && qualifies(after) == include && (!include || after&32 == 0)
		if selected != (reads == 1) || allowed != (writes == 1) {
			t.Fatalf("selected=%v allowed=%v; got %d reads, %d writes, err=%v", selected, allowed, reads, writes, err)
		}
		if allowed {
			requireOK(t, err)
		} else if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected deferred publication, got %v", err)
		}
	})
}
