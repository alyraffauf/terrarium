// Package syncmodel exercises the durable contracts shared by the two list services.
package syncmodel

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
)

type Store interface {
	MemberKeys(context.Context, string, string) ([]string, error)
	RememberMember(context.Context, string, string, string) error
	ForgetMember(context.Context, string, string) error
}
type Job struct {
	DID        string
	Generation int64
	Attempts   int
}
type API struct {
	CoalesceScans bool
	Elapse        func() error
	Store
	Event         func(string, string, int64, int64, bool) error
	Recover       func(string, int64) error
	Cursor        func(string) (int64, error)
	Scan, Members func(string) error
	Next          func() (Job, error)
	Finish, Retry func(Job) error
	Snapshot      func(int64, map[string]bool) error
	Followed      func(string) (bool, error)
	Versions      func() (int64, int64, error)
	Close         func() error
}

func Fuzz(f *testing.F, open func(*testing.T, string) API) {
	f.Add([]byte{1, 2, 1, 3, 2, 4, 2, 0, 2, 3, 2})
	f.Add([]byte{1, 16, 2, 3, 2})
	// Retry twice across a reopen, then enqueue an urgent event to reset the attempts.
	f.Add([]byte{1, 2, 4, 8, 13, 2, 4, 13, 2, 0, 2})
	f.Add([]byte{0, 16, 1, 18, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	f.Add([]byte{1, 2, 1, 3, 2, 4, 16, 7, 10, 6, 8, 9, 12, 14})
	f.Fuzz(func(t *testing.T, trace []byte) {
		path := t.TempDir() + "/state.db"
		db := open(t, path)
		defer func() { db.Close() }()
		ctx := t.Context()
		ids := []string{"did:plc:a", "did:plc:b"}
		targets := []string{"one.example", "two.example"}
		keys := []string{"x", "y"}
		members := map[[2]string]string{}
		// Work is either event-priority, ordinary scan, or waiting for retry.
		pending := map[string]int{}
		attempts := map[string]int{}
		var held Job
		var valid bool
		changed := func(did string, urgent bool) {
			if held.DID == did && (urgent || !db.CoalesceScans) {
				valid = false
			}
			if _, ok := pending[did]; !ok || urgent {
				pending[did] = 1
				if urgent {
					pending[did] = 0
					delete(attempts, did)
				}
			}
		}
		var portable, seq, version int64
		var applied int64 = -1
		var endpoint string
		follows := map[string]bool{}
		check := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		for step, b := range trace[:min(len(trace), 48)] {
			did, target, key := ids[b>>4&1], targets[b>>5&1], keys[b>>6&1]
			ep := "wss://" + target
			n := int64(b >> 4)
			timestamp := int64(5000001) + n
			index := [2]string{target, key}
			switch b % 16 {
			case 13:
				check(db.Elapse())
				for did, p := range pending {
					if p == 2 {
						pending[did] = 1
					}
				}
			case 0, 15:
				dirty := b%16 == 15
				if dirty {
					version++
				}
				check(db.Event(did, ep, n, timestamp, dirty))
				changed(did, true)
				portable = max(portable, timestamp)
				if n > 0 && (ep != endpoint || n > seq) {
					endpoint, seq = ep, n
				}
			case 1:
				check(db.Scan(did))
				changed(did, false)
			case 2:
				j, err := db.Next()
				priority := 2
				for _, p := range pending {
					priority = min(priority, p)
				}
				if priority == 2 {
					if !errors.Is(err, sql.ErrNoRows) {
						t.Fatalf("step %d expected no ready jobs, got %+v %v", step, j, err)
					}
					break
				}
				check(err)
				p, ok := pending[j.DID]
				if !ok || p != priority {
					t.Fatalf("step %d next %+v pending %v", step, j, pending)
				}
				if j.Attempts != attempts[j.DID] {
					t.Fatalf("step %d job %s: got %d attempts, want %d", step, j.DID, j.Attempts, attempts[j.DID])
				}
				held, valid = j, true
			case 3, 4:
				if held.DID != "" {
					if b%16 == 3 {
						check(db.Finish(held))
						if valid {
							delete(pending, held.DID)
							delete(attempts, held.DID)
						}
					} else {
						check(db.Retry(held))
						if valid {
							pending[held.DID] = 2
							attempts[held.DID]++
						}
					}
					held = Job{}
					valid = false
				}
			case 5:
				check(db.RememberMember(ctx, target, did, key))
				members[index] = did
			case 6:
				check(db.ForgetMember(ctx, target, key))
				delete(members, index)
			case 7:
				check(db.Members(target))
				for pair, member := range members {
					if pair[0] == target {
						changed(member, false)
					}
				}
			case 8:
				check(db.Close())
				db = open(t, path)
			case 9, 10:
				next := map[string]bool{}
				if b%16 == 9 {
					next[did] = true
				}
				check(db.Snapshot(version, next))
				applied = version
				for _, id := range ids {
					if follows[id] != next[id] {
						changed(id, true)
					}
				}
				follows = next
			case 11:
				if err := db.Snapshot(version-1, map[string]bool{did: true}); err == nil {
					t.Fatal("accepted stale owner snapshot")
				}
			case 12:
				check(db.Recover(ep, n))
				version++
				if n > 0 && (ep != endpoint || n > seq) {
					endpoint, seq = ep, n
				}
			case 14: // Observe without mutating.
			}
			v, a, err := db.Versions()
			check(err)
			if v != version || a != applied {
				t.Fatalf("follow versions %d/%d want %d/%d", v, a, version, applied)
			}
			for _, target := range targets {
				ep := "wss://" + target
				want := max(0, portable-5000000)
				if ep == endpoint {
					want = seq
				}
				got, err := db.Cursor(ep)
				check(err)
				if got != want {
					t.Fatalf("step %d cursor %s=%d want %d", step, ep, got, want)
				}
				for _, id := range ids {
					got, err := db.Followed(id)
					check(err)
					if got != follows[id] {
						t.Fatalf("followed %s=%t", id, got)
					}
					var want []string
					for _, k := range keys {
						if members[[2]string{target, k}] == id {
							want = append(want, k)
						}
					}
					actual, err := db.MemberKeys(ctx, target, id)
					check(err)
					if !slices.Equal(actual, want) {
						t.Fatalf("step %d %s/%s keys %v want %v", step, target, id, actual, want)
					}
				}
			}
		}
	})
}
