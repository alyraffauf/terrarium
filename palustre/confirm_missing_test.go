package palustre

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/xrpc"
)

func TestConfirmMissingRepo(t *testing.T) {
	const did = "did:example:actor"
	missing := &xrpc.Error{StatusCode: 400, Wrapped: &xrpc.XRPCError{ErrStr: "InvalidRequest", Message: "Could not find repo: " + did}}
	for _, tc := range []struct {
		body   string
		absent bool
	}{
		{`{"did":"did:example:actor","active":false}`, true},
		{`{"did":"did:example:actor","active":true}`, false},
		{`{"did":"did:example:actor"}`, false},
		{`{"did":"did:example:other","active":false}`, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, tc.body) }))
		err := VerifyRepoReadError(t.Context(), &xrpc.Client{Host: srv.URL, Client: srv.Client()}, did, missing)
		srv.Close()
		if (err == nil) != tc.absent {
			t.Errorf("status %s: err=%v", tc.body, err)
		}
	}
}
