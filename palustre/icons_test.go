package palustre

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func iconPNG(t *testing.T) []byte {
	t.Helper()
	source := image.NewNRGBA(image.Rect(0, 0, 512, 256))
	source.Set(0, 0, color.NRGBA{R: 255, A: 255})
	var data bytes.Buffer
	if err := png.Encode(&data, source); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func TestFaviconDiscoveryAndICOFallback(t *testing.T) {
	original := iconPNG(t)
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "relative link", true: "ICO fallback"}[fallback], func(t *testing.T) {
			ico := make([]byte, 22)
			copy(ico, []byte{0, 0, 1, 0, 1, 0})
			binary.LittleEndian.PutUint32(ico[14:18], uint32(len(original)))
			binary.LittleEndian.PutUint32(ico[18:22], 22)
			ico = append(ico, original...)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/":
					http.Redirect(w, r, "/home/", http.StatusFound)
				case "/home/":
					if fallback {
						w.Write([]byte(`<link rel="icon" href="missing.svg">`))
					} else {
						w.Write([]byte(`<link rel="shortcut icon" href="image.png">`))
					}
				case "/home/image.png":
					w.Write(original)
				case "/favicon.ico":
					w.Write(ico)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			api := &Session{HTTP: server.Client()}
			data, err := api.Favicon(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			config, format, err := image.DecodeConfig(bytes.NewReader(data))
			if err != nil || format != "png" || config.Width != 256 || config.Height != 128 {
				t.Fatalf("unexpected normalized icon: %+v %s %v", config, format, err)
			}
		})
	}
}

func TestIconUpdatePreservesListAndSkipsUnchangedImage(t *testing.T) {
	data, err := normalizeIcon(iconPNG(t))
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := multihash.Sum(data, multihash.SHA2_256, -1)
	blob := &lexutil.LexBlob{Ref: lexutil.LexLink(cid.NewCidV1(cid.Raw, hash)), MimeType: "image/png", Size: int64(len(data))}
	purpose, description := "app.bsky.graph.defs#modlist", "Keep this description"
	list := &bsky.GraphList{LexiconTypeID: ListCollection, Name: "Example", Purpose: &purpose, Description: &description, CreatedAt: "2026-01-01T00:00:00Z"}
	uploads, writes := 0, 0
	api := &Session{Config: Config{WriteInterval: time.Nanosecond}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/com.atproto.repo.getRecord":
			json.NewEncoder(w).Encode(map[string]any{"cid": "saved-record-cid", "uri": "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/example", "value": list})
		case "/xrpc/com.atproto.repo.uploadBlob":
			uploads++
			if r.Header.Get("Content-Type") != "image/png" {
				t.Error("incorrect upload MIME type")
			}
			json.NewEncoder(w).Encode(atproto.RepoUploadBlob_Output{Blob: blob})
		case "/xrpc/com.atproto.repo.putRecord":
			writes++
			var input atproto.RepoPutRecord_Input
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			if input.SwapRecord == nil || *input.SwapRecord != "saved-record-cid" {
				t.Error("missing record precondition")
			}
			updated := input.Record.Val.(*bsky.GraphList)
			if updated.Description == nil || *updated.Description != description || updated.CreatedAt != list.CreatedAt {
				t.Error("list metadata changed")
			}
			list = updated
			w.Write([]byte(`{}`))
		default:
			t.Error("unexpected endpoint", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api.HTTP = server.Client()
	api.Client = &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}}
	for i := 0; i < 2; i++ {
		if err := api.UpdateListIcon(context.Background(), "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/example", "Example", data); err != nil {
			t.Fatal(err)
		}
	}
	if uploads != 1 || writes != 1 {
		t.Fatalf("uploads=%d writes=%d, want one each", uploads, writes)
	}
}

func TestMalformedIconIsRejected(t *testing.T) {
	for _, data := range [][]byte{[]byte("not an image"), {0, 0, 1, 0, 0, 0}, {0, 0, 1, 0, 1, 0}} {
		if _, err := normalizeIcon(data); err == nil {
			t.Fatal("accepted malformed icon")
		}
	}
}
