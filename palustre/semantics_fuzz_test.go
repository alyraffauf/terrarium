package palustre

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
)

func FuzzEndpoints(f *testing.F) {
	for i := byte(0); i < 6; i++ {
		f.Add(i, uint16(443), true)
		f.Add(i, uint16(80), false)
	}
	f.Fuzz(func(t *testing.T, form byte, p uint16, secure bool) {
		h, raw := "example.com", "EXAMPLE.COM"
		if form&1 != 0 {
			h, raw = "[::1]", "[0:0:0:0:0:0:0:1]"
		}
		web, ws, standard := "http", "ws", 80
		if secure {
			web, ws, standard = "https", "wss", 443
		}
		suffix := fmt.Sprintf(":00%d", p)
		want := h
		if int(p) != standard {
			want += fmt.Sprintf(":%d", p)
		}
		for _, stream := range []bool{false, true} {
			normalize := NormalizePDS
			expected := web + "://" + want
			scheme := web
			if stream {
				normalize = NormalizeJetstream
				expected = ws + "://" + want + StreamPath
				if form&2 != 0 {
					scheme = ws
				}
			}
			input := scheme + "://" + raw + suffix + "/"
			if form&4 != 0 {
				input += "?"
			}
			got, err := normalize(input)
			if p == 0 {
				if err == nil {
					t.Fatal(input)
				}
				continue
			}
			if err != nil || got != expected {
				t.Fatal(input, got, err, expected)
			}
			again, err := normalize(got)
			if err != nil || again != got || stream && !StreamEndpointsEqual(input, expected) {
				t.Fatal("unstable endpoint", got)
			}
			for _, tail := range []string{"?a=b", "#part", "bad"} {
				if _, err := normalize(strings.TrimSuffix(input, "?") + tail); err == nil {
					t.Fatal("accepted", tail)
				}
			}
		}
	})
}

func FuzzFrame(f *testing.F) {
	for i := byte(0); i < 14; i++ {
		f.Add(i, int64(5))
	}
	f.Fuzz(func(t *testing.T, kind byte, seq int64) {
		rec := object{"$type": FollowCollection, "subject": owner}
		stamp := "2025-01-01T00:00:00Z"
		p := object{"$type": EventPrefix + "commit", "collection": FollowCollection, "operation": "create", "rkey": "abc", "did": owner, "seq": seq, "time": stamp, "record": rec}
		envelope := object{"$type": "message", "payload": p}
		gap := seq <= 0
		recover, malformed := false, false
		subject := ""
		switch kind % 14 {
		case 0, 1:
			subject = owner
			if kind%14 == 1 {
				p["operation"] = "update"
			}
		case 2:
			p["operation"] = "delete"
		case 3:
			rec["subject"] = ""
			malformed = true
		case 4:
			p["rkey"] = "/"
			malformed = true
		case 5:
			p["did"] = ""
			recover = true
		case 6:
			p["witnessedAt"] = "bad"
			recover = true
		case 7:
			p["$type"] = EventPrefix + "identity"
		case 8:
			p["$type"] = "commit"
			recover = true
		case 9:
			p["$type"] = EventPrefix + "info"
			gap = false
		case 10:
			envelope = object{"$type": "error", "error": "FutureCursor"}
			gap = true
		case 11:
			p["collection"] = ListCollection
		case 12:
			rec["$type"] = ListCollection
			malformed = true
		case 13:
			p["operation"] = "other"
			malformed = true
		}
		raw, _ := json.Marshal(envelope)
		e, err := ClassifyFrame(raw)
		if gap != errors.Is(err, ErrStreamGap) || !gap && (err != nil || e.Recover != recover || e.MalformedFollow != malformed || e.FollowSubject != subject) {
			t.Fatal(string(raw), e, err)
		}
	})
}

func FuzzAuthHistory(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 1, 1, 1, 1, 1, 1, 1, 5})
	f.Fuzz(func(t *testing.T, ops []byte) {
		synctest.Test(t, func(t *testing.T) {
			var op byte
			login := false
			calls := 0
			api := session(func(r *http.Request) (int, any) {
				calls++
				method := "refreshSession"
				if login {
					method = "createSession"
				}
				if !strings.HasSuffix(r.URL.Path, method) {
					t.Fatal(r.URL.Path, method)
				}
				switch op % 6 {
				case 0:
					if !login {
						login = true
						return 400, object{"error": "ExpiredToken"}
					}
					fallthrough
				case 1:
					return 429, object{"error": "RateLimitExceeded"}
				case 2:
					return 200, object{"did": owner, "handle": "owner.example", "accessJwt": "partial"}
				case 3:
					return 200, object{"did": "did:web:other.example", "handle": "owner.example", "accessJwt": "x", "refreshJwt": "y"}
				default:
					return 200, object{"did": owner, "handle": "owner.example", "accessJwt": "ok", "refreshJwt": "fresh"}
				}
			})
			failures := 0
			for _, step := range ops[:min(len(ops), 40)] {
				op = step
				old := *api.Client.Auth
				err := api.Refresh(t.Context())
				if op%6 < 4 {
					failures++
					minutes := []int{1, 2, 4, 8, 16, 30}[min(failures-1, 5)]
					delay := time.Duration(minutes) * time.Minute
					if err == nil || *api.Client.Auth != old || api.retryAuthentication.after != time.Now().Add(delay) {
						t.Fatal(step, err, api.retryAuthentication)
					}
					prev := calls
					time.Sleep(delay - time.Nanosecond)
					if api.Refresh(t.Context()) == nil || calls != prev {
						t.Fatal("premature retry")
					}
					time.Sleep(time.Nanosecond)
				} else {
					if err != nil || api.Client.Auth.AccessJwt != "ok" || api.Client.Auth.RefreshJwt != "fresh" {
						t.Fatal(err)
					}
					failures = 0
					login = false
					time.Sleep(31 * time.Minute)
				}
			}
		})
	})
}

func FuzzPacing(f *testing.F) {
	f.Add([]byte{1, 2, 3, 0, 0}, uint8(3))
	f.Fuzz(func(t *testing.T, ops []byte, n byte) {
		synctest.Test(t, func(t *testing.T) {
			interval := time.Duration(n+1) * time.Second
			if interval == 0 {
				interval = DefaultWriteInterval
			}
			api := Session{Config: Config{WriteInterval: interval}}
			due := time.Now()
			for _, op := range ops[:min(len(ops), 32)] {
				switch op % 4 {
				case 1:
					time.Sleep(time.Hour)
				case 2:
					api.ObserveWriteError(&xrpc.Error{StatusCode: 429})
					due = time.Now().Add(time.Minute + interval)
				case 3:
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					if api.WaitWrite(ctx) == nil {
						t.Fatal("cancelled write")
					}
				default:
					want := time.Now()
					if due.After(want) {
						want = due
					}
					if err := api.WaitWrite(t.Context()); err != nil || !time.Now().Equal(want) {
						t.Fatal(err, time.Now(), want)
					}
					due = want.Add(interval)
				}
			}
		})
	})
}

func FuzzScrapes(f *testing.F) {
	f.Add([]byte{17, 0, 255, 3})
	f.Fuzz(func(t *testing.T, ns []byte) {
		api := Session{}
		var n byte
		verification := api.metricsHandler(nil)
		h := api.metricsHandler(func(context.Context) (int64, error) {
			if n == 255 {
				return 0, errors.New("unavailable")
			}
			return int64(n), nil
		})
		for i, b := range ns[:min(len(ns), 12)] {
			n = b
			counts := []int{i, i + 1, i + 2, i + 3}
			api.MembersAdded.Store(uint64(i))
			api.MembersRemoved.Store(uint64(i + 1))
			api.ChecksCompleted.Store(uint64(i + 2))
			api.CheckRetries.Store(uint64(i + 3))
			api.VerificationDeferred.Store(uint64(i + 4))
			api.VerificationRecovered.Store(uint64(i + 5))
			v := httptest.NewRecorder()
			verification.ServeHTTP(v, httptest.NewRequest("GET", "/metrics", nil))
			for k, name := range []string{"verification_deferred_total", "verification_recovered_total"} {
				if v.Code != 200 || !strings.Contains(v.Body.String(), fmt.Sprintf("\nterrarium_%s %d\n", name, i+4+k)) {
					t.Fatal(name, v.Body.String())
				}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
			if b == 255 {
				if w.Code != 500 {
					t.Fatal(w.Code)
				}
				continue
			}
			for j, name := range []string{"members_added_total", "members_removed_total", "checks_completed_total", "check_retries_total", "pending_accounts"} {
				values := append(counts, int(n))
				if w.Code != 200 || !strings.Contains(w.Body.String(), fmt.Sprintf("\nterrarium_%s %d\n", name, values[j])) {
					t.Fatal(name, w.Body.String())
				}
			}
		}
	})
}

func FuzzFavicon(f *testing.F) {
	for mode := byte(0); mode < 5; mode++ {
		f.Add(mode, byte(17))
	}
	f.Fuzz(func(t *testing.T, mode, n byte) {
		w := int(n) + 2
		h := w
		img := image.NewNRGBA(image.Rect(0, 0, w, h))
		img.Set(0, 0, color.White)
		var data bytes.Buffer
		if err := png.Encode(&data, img); err != nil {
			t.Fatal(err)
		}
		path := "/icon.png"
		switch mode % 5 {
		case 1, 2, 3, 4:
			path = "/favicon.ico"
		}
		// A second loopback server records any request that escapes the origin policy.
		foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("unexpected request to foreign loopback origin")
			w.Write(data.Bytes())
		}))
		defer foreign.Close()
		foreignURL := strings.Replace(foreign.URL, "127.0.0.1", "[::ffff:127.0.0.1]", 1)
		server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/":
				link := []string{"/icon.png", foreignURL, "/redirect", "/invalid", "/absent"}[mode%5]
				fmt.Fprintf(out, "<link rel=icon href=%q>", link)
			case "/redirect":
				http.Redirect(out, r, foreignURL, 302)
			case "/invalid":
				fmt.Fprint(out, "ordinary text")
			case path:
				if mode%5 == 4 && w <= 255 {
					ico := make([]byte, 22)
					ico[2] = 1
					ico[4] = 1
					ico[6] = byte(w)
					ico[7] = byte(h)
					binary.LittleEndian.PutUint32(ico[14:], uint32(data.Len()))
					ico[18] = 22
					out.Write(ico)
				}
				out.Write(data.Bytes())
			default:
				http.NotFound(out, r)
			}
		}))
		defer server.Close()
		got, err := (&Session{HTTP: server.Client()}).Favicon(t.Context(), strings.Replace(server.URL, "127.0.0.1", "localhost", 1))
		if err != nil {
			t.Fatal(err)
		}
		cfg, format, err := image.DecodeConfig(bytes.NewReader(got))
		if err != nil || format != "png" || cfg.Width != min(w, 256) || cfg.Height != min(h, 256) {
			t.Fatal(cfg, format, err)
		}
	})
}

// Multiple directory entries may share a PNG. Candidate metadata should
// reference the original payload even when most entries share its offset.
func FuzzIconDirectory(f *testing.F) {
	f.Add(byte(255), byte(16))
	f.Add(byte(0), byte(32))
	f.Fuzz(func(t *testing.T, aliases, size byte) {
		n := 1 + int(aliases)
		width := 1 + int(size)%32
		var pngData bytes.Buffer
		if err := png.Encode(&pngData, image.NewGray(image.Rect(0, 0, width, width))); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 6+n*16)
		data[2] = 1
		binary.LittleEndian.PutUint16(data[4:], uint16(n))
		for i := range n {
			entry := data[6+i*16:]
			entry[0], entry[1] = byte(width), byte(width)
			binary.LittleEndian.PutUint32(entry[8:], uint32(pngData.Len()))
			binary.LittleEndian.PutUint32(entry[12:], uint32(len(data)))
		}
		if n > 1 {
			// The first and largest entry is unavailable. The other entries
			// all reference the same valid PNG.
			data[6], data[7] = 0, 0
			binary.LittleEndian.PutUint32(data[14:], 1)
			binary.LittleEndian.PutUint32(data[18:], uint32(len(data)+pngData.Len()))
		}
		data = append(data, pngData.Bytes()...)
		data = append(data, 0)
		entries, err := icoCandidates(data)
		if err != nil || len(entries) != n {
			t.Fatal(len(entries), err)
		}
		out, err := normalizeIcon(data)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := png.DecodeConfig(bytes.NewReader(out))
		if err != nil || cfg.Width != width || cfg.Height != width {
			t.Fatal(cfg, err)
		}
	})
}

func FuzzIconHostPolicy(f *testing.F) {
	f.Add(byte(3), false)
	f.Add(byte(128), true)
	f.Fuzz(func(t *testing.T, last byte, trusted bool) {
		for _, prefix := range []string{"100.64.0.", "100.127.255.", "198.19.0.", "0.1.2.", "::ffff:100.100.1."} {
			ip := net.ParseIP(prefix + fmt.Sprint(last))
			if isPublicIP(ip) {
				t.Fatal(ip)
			}
			host := ip.String()
			u := url.URL{Scheme: "https", Host: host}
			allowed := ""
			if trusted {
				allowed = host
			}
			if (checkIconTarget(t.Context(), &u, allowed) == nil) != trusted {
				t.Fatal(host, trusted)
			}
		}
	})
}
