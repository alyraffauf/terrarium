package helix

import (
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzListBindings(f *testing.F) {
	m.Bindings(f, m.Actors, func(t *testing.T) m.BindingStore {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		bind := func(requests []m.Binding) ([]string, error) {
			targets := make([]Target, len(requests))
			for i, req := range requests {
				targets[i] = Target{Actor: req.Target, ListURI: req.ListURI}
			}
			return db.bindLists(t.Context(), targets, m.Owner)
		}
		return m.BindingStore{Bind: bind, Lookup: db.queries.GetList}
	})
}

func TestParsedBindingsMatchCanonicalTargets(t *testing.T) {
	raw := " @ONE.Example "
	canonical := "one.example"
	uri := m.List(0)
	targets, err := ParseTargets([]string{raw}, []string{"  one.example \t=  " + uri + " "})
	if err != nil || len(targets) != 1 || targets[0].Actor != canonical || targets[0].ListURI != uri {
		t.Fatalf("parsed bindings %v %v", targets, err)
	}
	if _, err := ParseTargets([]string{raw, canonical}, nil); err == nil {
		t.Fatal("accepted duplicate canonical target")
	}
}
