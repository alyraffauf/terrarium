package syncmodel

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"slices"
	"testing"
)

type Binding struct {
	Target, ListURI string
}

type BindingStore struct {
	Bind   func([]Binding) ([]string, error)
	Lookup func(context.Context, string) (string, error)
}

// Bindings drives each daemon's public binding behavior with the same target and list choices.
func Bindings(f *testing.F, targets [2]string, setup func(*testing.T) BindingStore) {
	f.Add([]byte{0, 1, 3, 2, 6, 9, 11})
	f.Add([]byte{8, 10}) // conflicting pair, followed by a valid reversed pair
	f.Fuzz(func(t *testing.T, history []byte) {
		db := setup(t)
		saved := map[string]string{}
		for step, b := range history[:min(32, len(history))] {
			proposed := maps.Clone(saved)
			var requests []Binding
			var want []string
			valid := true
			for j := range 1 + int(b>>3&1) {
				target := targets[(int(b)+j)%2]
				uri := List(int(b >> (1 + j) & 1))
				requested := uri
				old := saved[target]
				if b&16 != 0 && old != "" {
					requested, uri = "", old
				}
				if old != "" && old != uri {
					valid = false
				}
				for other, used := range proposed {
					if other != target && used == uri {
						valid = false
					}
				}
				proposed[target] = uri
				requests = append(requests, Binding{Target: target, ListURI: requested})
				want = append(want, uri)
			}
			got, err := db.Bind(requests)
			if valid != (err == nil) || valid && !slices.Equal(got, want) {
				t.Fatalf("step %d bindings %v %v want %v valid %t", step, got, err, want, valid)
			}
			if valid {
				saved = proposed
			}
			for _, target := range targets {
				uri, err := db.Lookup(t.Context(), target)
				if errors.Is(err, sql.ErrNoRows) && saved[target] == "" {
					continue
				}
				if err != nil || uri != saved[target] {
					t.Fatalf("step %d persisted %s = %q (%v), want %q", step, target, uri, err, saved[target])
				}
			}
		}
	})
}
