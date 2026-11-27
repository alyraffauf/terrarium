package syncmodel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
)

// Inventory pages can interleave records from managed, paused and unrelated lists.
func Inventory(f *testing.F, setup func(*testing.T, *httptest.Server, int) (Store, func() error)) {
	f.Add([]byte{0, 1, 2, 3, 4}, byte(1))
	f.Fuzz(func(t *testing.T, trace []byte, mask byte) {
		pages := append([]byte{0}, trace[:min(len(trace), 12)]...)
		page := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cursor := ""
			if page > 0 {
				cursor = strconv.Itoa(page)
			}
			if page >= len(pages) || r.URL.Query().Get("cursor") != cursor {
				http.Error(w, "unexpected cursor", 400)
				return
			}
			i := page
			b := pages[i]
			page++
			value := map[string]string{"$type": "app.bsky.graph.listitem", "list": List(int(b % 3)), "subject": Subject}
			record := map[string]any{"uri": fmt.Sprintf("at://%s/app.bsky.graph.listitem/k%02d", Owner, i), "value": value}
			out := map[string]any{"records": []any{record}}
			if page < len(pages) {
				out["cursor"] = strconv.Itoa(page)
			}
			json.NewEncoder(w).Encode(out)
		}))
		defer srv.Close()
		paused := int(mask % 3)
		db, step := setup(t, srv, paused)
		var expected [2][]string
		for i, b := range pages {
			if err := step(); err != nil {
				t.Fatal(err)
			}
			if page != i+1 {
				t.Fatalf("step %d fetched %d pages", i, page)
			}
			target := int(b % 3)
			if target < 2 && target != paused {
				expected[target] = append(expected[target], fmt.Sprintf("k%02d", i))
			}
			for j := range 2 {
				keys, err := db.MemberKeys(t.Context(), Actors[j], Subject)
				if err != nil || !slices.Equal(keys, expected[j]) {
					t.Fatalf("step %d target %d keys %v want %v %v", i, j, keys, expected[j], err)
				}
			}
		}
		if err := step(); err != nil || page != len(pages) {
			t.Fatalf("inventory immediately restarted %v", err)
		}
	})
}
