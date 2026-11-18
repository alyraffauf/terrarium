package palustre

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// Icon URLs discovered from remote content (a page's <link rel=icon>, a
// profile avatar field, or a redirect) are not operator configuration.
// Fetching a non-public address among them would let the remote content
// choose requests inside this process's network, and a response that
// decodes as an image is then uploaded as a public list avatar. Such
// fetches must never reach a private target; the operator-configured
// source host itself remains reachable, including on a private network.

func policyTestIcon(t *testing.T, w, h int, seed int64) []byte {
	t.Helper()
	return icoTestPNG(t, w, h, seed)
}

func faviconFallbackICO(t *testing.T, png []byte) []byte {
	t.Helper()
	return buildTestICO([]icoTestEntry{icoEntry(png, 16, 16)})
}

func TestFaviconIgnoresPrivateIconLink(t *testing.T) {
	var privateHits atomic.Int32
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		privateHits.Add(1)
		w.Write(policyTestIcon(t, 32, 32, 51))
	}))
	defer private.Close()
	privateURL := strings.Replace(private.URL, "127.0.0.1", "localhost", 1)

	fallback := faviconFallbackICO(t, policyTestIcon(t, 16, 16, 52))
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			fmt.Fprintf(w, `<link rel="icon" href=%q>`, privateURL+"/icon.png")
		case "/favicon.ico":
			w.Write(fallback)
		default:
			http.NotFound(w, r)
		}
	}))
	defer page.Close()
	api := &Session{HTTP: page.Client()}
	out, err := api.Favicon(context.Background(), page.URL)
	if err != nil {
		t.Fatal(err)
	}
	if privateHits.Load() != 0 {
		t.Fatalf("private icon target was fetched %d times", privateHits.Load())
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(out))
	if err != nil || cfg.Width != 16 {
		t.Fatalf("did not fall back to the source's own favicon: %dx%d %v", cfg.Width, cfg.Height, err)
	}
}

func TestFetchIconRejectsPrivateTargetAndRedirect(t *testing.T) {
	var privateHits atomic.Int32
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		privateHits.Add(1)
		w.Write(policyTestIcon(t, 32, 32, 53))
	}))
	defer private.Close()
	privateURL := strings.Replace(private.URL, "127.0.0.1", "localhost", 1)

	api := &Session{HTTP: private.Client()}
	if _, err := api.FetchIcon(context.Background(), privateURL+"/avatar.png"); err == nil {
		t.Fatal("private avatar target fetched")
	}
	// A redirect from a reachable address to a private one is the same
	// fetch, not a new decision.
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, privateURL+"/avatar.png", http.StatusFound)
	}))
	defer redirector.Close()
	api = &Session{HTTP: redirector.Client()}
	origin, _ := url.Parse(redirector.URL)
	if _, err := api.fetchIcon(context.Background(), redirector.URL+"/avatar.png", origin.Hostname()); err == nil {
		t.Fatal("redirect to a private avatar target fetched")
	}
	if privateHits.Load() != 0 {
		t.Fatalf("private target was fetched %d times", privateHits.Load())
	}
}

func TestFaviconStillUsesSourceHostIcon(t *testing.T) {
	// The configured source may itself live on a private address (a
	// self-hosted PDS or a test server); its own icon stays reachable.
	icon := policyTestIcon(t, 24, 24, 54)
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Write([]byte(`<link rel="icon" href="/icon.png">`))
		case "/icon.png":
			w.Write(icon)
		default:
			http.NotFound(w, r)
		}
	}))
	defer page.Close()
	api := &Session{HTTP: page.Client()}
	out, err := api.Favicon(context.Background(), page.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(out))
	if err != nil || cfg.Width != 24 {
		t.Fatalf("source-host icon rejected: %dx%d %v", cfg.Width, cfg.Height, err)
	}
}

func TestIconRejectsSharedAddressSpace(t *testing.T) {
	for _, address := range []string{"100.100.100.200", "100.64.0.1", "198.18.0.1", "0.1.2.3", "::ffff:100.100.100.200"} {
		if isPublicIP(net.ParseIP(address)) {
			t.Fatalf("accepted non-public address %s", address)
		}
	}
}
