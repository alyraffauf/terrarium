package capensis

import (
	"slices"
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzListBindings(f *testing.F) {
	f.Add([]byte{0, 1, 3, 2, 6, 9, 11})
	f.Fuzz(func(t *testing.T, history []byte) {
		db, err := openStore(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		saved := map[string]string{}
		for step, b := range history[:min(32, len(history))] {
			proposed := map[string]string{}
			var targets []Target
			var want []string
			valid := true
			for j := range 1 + int((b>>2)&1) {
				i := (int(b) + j) % 2
				target := m.Hosts[i]
				uri := m.List(int((b >> (1 + j)) & 1))
				requested := uri
				if b&16 != 0 && saved[target] != "" {
					requested = ""
					uri = saved[target]
				}
				if old := saved[target]; old != "" && old != uri {
					valid = false
				}
				for other, u := range saved {
					if other != target && u == uri {
						valid = false
					}
				}
				for _, u := range proposed {
					if u == uri {
						valid = false
					}
				}
				proposed[target] = uri
				want = append(want, uri)
				targets = append(targets, Target{PDS: target, ListURI: requested})
			}
			got, err := db.bindLists(t.Context(), targets, m.Owner)
			if valid != (err == nil) || valid && !slices.Equal(got, want) {
				t.Fatalf("step %d bindings %v %v want %v valid %t", step, got, err, want, valid)
			}
			if valid {
				for k, v := range proposed {
					saved[k] = v
				}
			}
			var count int
			if err := db.conn.QueryRow("SELECT count(*) FROM targets").Scan(&count); err != nil || count != len(saved) {
				t.Fatalf("step %d persisted %d want %d %v", step, count, len(saved), err)
			}
		}
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
