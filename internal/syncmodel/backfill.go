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

func Backfill(f *testing.F, byHost bool, run func(*testing.T, *httptest.Server) ([]string, error)) {
	f.Add([]byte{3, 1, 2, 0}, false)
	f.Add([]byte{0, 1}, true)
	f.Fuzz(func(t *testing.T, data []byte, cycle bool) {
		pages := append([]byte{0}, data[:min(8, len(data))]...)
		expected := []string{Owner}
		for _, mask := range pages {
			for i, actor := range Actors {
				if mask&(1<<i) != 0 && !slices.Contains(expected, actor) {
					expected = append(expected, actor)
				}
			}
		}
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			page := calls
			calls++
			if page >= len(pages) {
				http.Error(w, "too many pages", 400)
				return
			}
			cursor := ""
			if page > 0 {
				cursor = strconv.Itoa(page)
			}
			if r.URL.Query().Get("cursor") != cursor {
				t.Errorf("page %d cursor %q", page, r.URL.Query().Get("cursor"))
			}
			values := []any{}
			for i, actor := range Actors {
				if pages[page]&(1<<i) != 0 {
					values = append(values, map[string]any{"did": actor, "active": true})
				}
			}
			out := map[string]any{}
			if byHost {
				out["repos"] = values
			} else {
				out["followers"] = values
				out["subject"] = map[string]string{"did": Actors[0]}
			}
			if page+1 < len(pages) {
				out["cursor"] = fmt.Sprint(page + 1)
			} else if cycle && page > 0 {
				out["cursor"] = cursor
			}
			json.NewEncoder(w).Encode(out)
		}))
		defer srv.Close()
		got, err := run(t, srv)
		if (err != nil) != (cycle && len(pages) > 1) || calls != len(pages) {
			t.Fatalf("pages %d/%d err %v", calls, len(pages), err)
		}
		slices.Sort(expected)
		slices.Sort(got)
		if !slices.Equal(got, expected) {
			t.Fatalf("queued %v want %v", got, expected)
		}
	})
}
