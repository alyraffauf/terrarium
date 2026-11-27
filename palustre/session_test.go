package palustre

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
)

func TestRefreshBacksOffAfterFailedReauthentication(t *testing.T) {
	var refreshes, logins atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/xrpc/com.atproto.server.refreshSession":
			refreshes.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"ExpiredToken"}`)
		case "/xrpc/com.atproto.server.createSession":
			if logins.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"error":"RateLimitExceeded"}`)
				return
			}
			fmt.Fprintf(w, `{"did":%q,"handle":"owner.example","accessJwt":"renewed-access","refreshJwt":"renewed-refresh"}`, testListOwner)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api := newTestListSession(server)
	api.HTTP = server.Client()
	api.Config.LoginPDS = server.URL
	for range 3 {
		if err := api.Refresh(t.Context()); !XRPCCode(err, "RateLimitExceeded") {
			t.Fatalf("expected last login failure, got %v", err)
		}
	}
	if refreshes.Load() != 1 || logins.Load() != 1 {
		t.Fatalf("expected one refresh and one login, got %d refreshes and %d logins", refreshes.Load(), logins.Load())
	}

	api.retryAuthentication.after = time.Now()
	if err := api.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if refreshes.Load() != 1 || logins.Load() != 2 || api.Client.Auth.AccessJwt != "renewed-access" {
		t.Fatalf("expected login to resume with new tokens, got %d refreshes and %d logins", refreshes.Load(), logins.Load())
	}
}

func TestRefreshHonorsThrottleAndKeepsValidCredentials(t *testing.T) {
	reset := time.Now().Add(40 * time.Minute).Truncate(time.Second)
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.server.refreshSession" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if refreshes.Add(1) == 1 {
			w.Header().Set("Retry-After", "120")
			w.Header().Set("RateLimit-Reset", fmt.Sprint(reset.Unix()))
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":"RateLimitExceeded"}`)
			return
		}
		fmt.Fprintf(w, `{"did":%q,"handle":"owner.example","accessJwt":"replacement-without-refresh"}`, testListOwner)
	}))
	defer server.Close()

	api := newTestListSession(server)
	api.Client.Auth.AccessJwt = "existing-access"
	api.Client.Auth.RefreshJwt = "existing-refresh"
	for range 2 {
		if err := api.Refresh(t.Context()); !XRPCCode(err, "RateLimitExceeded") {
			t.Fatalf("expected last refresh failure, got %v", err)
		}
	}
	if refreshes.Load() != 1 || !api.retryAuthentication.after.Equal(reset) {
		t.Fatalf("throttle deadline = %v, refresh requests = %d", api.retryAuthentication.after, refreshes.Load())
	}

	api.retryAuthentication.after = time.Now()
	if err := api.Refresh(t.Context()); err == nil {
		t.Fatal("accepted refresh with missing refresh token")
	}
	if api.Client.Auth.AccessJwt != "existing-access" || api.Client.Auth.RefreshJwt != "existing-refresh" {
		t.Fatal("incomplete refresh replaced session credentials")
	}
	if api.retryAuthentication.delay != 2*time.Minute {
		t.Fatalf("retry delay = %v, want 2m", api.retryAuthentication.delay)
	}
}

func TestAuthenticationBackoffCapsAndResets(t *testing.T) {
	api := &Session{}
	for _, want := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
		api.deferAuthentication(t.Context(), true, time.Time{}, fmt.Errorf("authentication failed"))
		if got := api.retryAuthentication.delay; got != want*time.Minute {
			t.Fatalf("retry delay = %v, want %dm", got, want)
		}
	}
	api.markAuthenticated()
	if api.retryAuthentication != (authenticationRetry{}) {
		t.Fatal("successful authentication retained previous backoff")
	}
}

const testListOwner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"

func newTestListSession(server *httptest.Server) *Session {
	return &Session{
		Config: Config{WriteInterval: time.Nanosecond},
		Client: &xrpc.Client{Host: server.URL, Client: server.Client(), Auth: &xrpc.AuthInfo{Did: testListOwner}},
	}
}
