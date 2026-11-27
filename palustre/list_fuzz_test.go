package palustre

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func FuzzListEdits(f *testing.F) {
	for i := byte(0); i < 5; i++ {
		f.Add(i, byte(4))
	}
	f.Fuzz(func(t *testing.T, mode, n byte) {
		prefix := strings.Repeat("é", int(n)%40+1)
		desc := prefix + "\nold"
		name := ListTitle(listURI)
		if mode%5 == 1 {
			name = "foreign"
		}
		exists := mode%5 >= 2
		writes := 0
		read := 0
		facet := func(a, b int) object {
			return object{"index": object{"byteStart": a, "byteEnd": b}, "features": []any{object{"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com"}}}
		}
		api := session(func(r *http.Request) (int, any) {
			switch {
			case strings.HasSuffix(r.URL.Path, "getRecord"):
				read++
				if !exists {
					return 400, object{"error": "RecordNotFound"}
				}
				return 200, object{"cid": "observed", "uri": listURI, "value": object{"$type": ListCollection, "name": name, "purpose": moderationListPurpose, "createdAt": "original", "description": desc, "descriptionFacets": []any{facet(0, len(prefix)), facet(len(prefix)+1, len(desc))}}}
			case strings.HasSuffix(r.URL.Path, "createRecord"):
				exists = true
				if mode%5 == 0 {
					desc = "new"
				}
				return 400, object{"error": "RecordAlreadyExists"}
			case strings.HasSuffix(r.URL.Path, "putRecord"):
				writes++
				var req struct {
					SwapRecord string
					Record     struct {
						Description, Name, CreatedAt string
						DescriptionFacets            []struct {
							Index struct{ ByteStart, ByteEnd int }
						}
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				if req.SwapRecord != "observed" || req.Record.Description != prefix+"\nnew" || req.Record.Name != name || req.Record.CreatedAt != "original" || len(req.Record.DescriptionFacets) != 1 || req.Record.DescriptionFacets[0].Index.ByteEnd != len(prefix) {
					t.Fatal(req)
				}
				if mode%5 == 4 {
					return 400, object{"error": "InvalidSwap"}
				}
				desc = req.Record.Description
				return 200, object{}
			}
			t.Fatal(r.URL.Path)
			return 0, nil
		})
		err := api.EnsureClusterList(t.Context(), listURI, "new", func(s string) bool { return s == "old" })
		switch mode % 5 {
		case 0, 1:
			if read != 2 || writes != 0 || ((mode%5 == 1) != errors.Is(err, ErrUnrelatedList)) || (mode%5 == 0 && err != nil) {
				t.Fatal(read, writes, err)
			}
		case 4:
			if !XRPCCode(err, "InvalidSwap") {
				t.Fatal(err)
			}
		default:
			if err != nil {
				t.Fatal(err)
			}
			if err := api.EnsureList(t.Context(), listURI, "new"); err != nil || writes != 1 {
				t.Fatal(writes, err)
			}
		}
	})
}

func FuzzAvatar(f *testing.F) {
	f.Add([]byte{0, 1, 1, 0, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		var avatar any
		var data []byte
		uploads, writes := 0, 0
		prior := -1
		api := session(func(r *http.Request) (int, any) {
			switch {
			case strings.HasSuffix(r.URL.Path, "getRecord"):
				return 200, object{"cid": "observed", "uri": listURI, "value": object{"$type": ListCollection, "purpose": moderationListPurpose, "name": "sample", "createdAt": "original", "description": "hand edited", "avatar": avatar}}
			case strings.HasSuffix(r.URL.Path, "uploadBlob"):
				uploads++
				b, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(b, data) || r.Header.Get("Content-Type") != "image/png" {
					t.Fatal(err)
				}
				hash, _ := multihash.Sum(b, multihash.SHA2_256, -1)
				return 200, object{"blob": object{"$type": "blob", "mimeType": "image/png", "ref": object{"$link": cid.NewCidV1(cid.Raw, hash).String()}, "size": len(b)}}
			case strings.HasSuffix(r.URL.Path, "putRecord"):
				writes++
				var req struct {
					SwapRecord string
					Record     object
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				if req.SwapRecord != "observed" || req.Record["description"] != "hand edited" || req.Record["createdAt"] != "original" {
					t.Fatal(req)
				}
				avatar = req.Record["avatar"]
				return 200, object{}
			}
			t.Fatal(r.URL.Path)
			return 0, nil
		})
		expected := 0
		for _, v := range ops[:min(len(ops), 24)] {
			value := int(v % 4)
			data = []byte{byte(value)}
			if prior != value {
				expected++
				prior = value
			}
			if err := api.UpdateListIcon(t.Context(), listURI, "sample", data); err != nil || uploads != expected || writes != expected {
				t.Fatal(err, uploads, writes, expected)
			}
		}
	})
}

func FuzzLimits(f *testing.F) {
	for _, n := range []uint16{299, 300, 301, 1499, 1500, 1501} {
		f.Add(n)
	}
	f.Fuzz(func(t *testing.T, n uint16) {
		count := int(n) % 1600
		for _, s := range []struct {
			text     string
			clusters int
		}{{strings.Repeat("e\u0301", count), count}, {"x" + strings.Repeat("\u0301", count), 1}} {
			want := len(s.text) <= 3000 && s.clusters <= 300
			if (validateListDescription(s.text) == nil) != want {
				t.Fatal(count, s.clusters)
			}
		}
	})
}

func FuzzServerRetryAfter(f *testing.F) {
	f.Add(uint16(120), uint16(240), true)
	f.Fuzz(func(t *testing.T, seconds, reset uint16, throttle bool) {
		start := time.Now().Truncate(time.Second)
		headers := http.Header{}
		headers.Set("Retry-After", start.Add(time.Duration(seconds)*time.Second).UTC().Format(http.TimeFormat))
		headers.Set("RateLimit-Reset", fmt.Sprint(start.Unix()+int64(reset)))
		code := 503
		want := seconds
		if throttle {
			code = 429
			want = max(seconds, reset)
		}
		var deadline time.Time
		captureAuthRetryAfter(&deadline)(&http.Response{StatusCode: code, Header: headers})
		if !deadline.Equal(start.Add(time.Duration(want) * time.Second)) {
			t.Fatal(deadline, want)
		}
	})
}
