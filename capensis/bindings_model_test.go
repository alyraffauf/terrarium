package capensis

import (
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzListBindings(f *testing.F) {
	m.Bindings(f, m.Hosts, func(t *testing.T) m.BindingStore {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		bind := func(requests []m.Binding) ([]string, error) {
			targets := make([]Target, len(requests))
			for i, req := range requests {
				targets[i] = Target{PDS: req.Target, ListURI: req.ListURI}
			}
			return db.bindLists(t.Context(), targets, m.Owner)
		}
		return m.BindingStore{Bind: bind, Lookup: db.queries.GetList}
	})
}

func TestParsedBindingsMatchCanonicalTargets(t *testing.T) {
	raw := " https://ONE.example:443 "
	canonical := "https://one.example"
	uri := m.List(0)
	targets, err := ParseTargets([]string{raw}, []string{"  https://one.example \t=  " + uri + " "})
	if err != nil || len(targets) != 1 || targets[0].PDS != canonical || targets[0].ListURI != uri {
		t.Fatalf("parsed bindings %v %v", targets, err)
	}
	if _, err := ParseTargets([]string{raw, canonical}, nil); err == nil {
		t.Fatal("accepted duplicate canonical target")
	}
}
