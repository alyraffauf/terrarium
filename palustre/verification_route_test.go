package palustre

import (
	"context"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/xrpc"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVerificationRouteWithoutHandle(t *testing.T) {
	const did = "did:plc:doygk5djs7vbtzslfifui3ld"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+did {
			t.Error(r.URL.Path)
		}
		w.Write([]byte(`{"id":"did:plc:doygk5djs7vbtzslfifui3ld","service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://example.com"}]}`))
	}))
	defer s.Close()
	resolver := identity.BaseDirectory{PLCURL: s.URL, PLCClient: s.Client()}
	doc, err := verificationRoute(context.Background(), did, &xrpc.XRPCError{ErrStr: "InvalidRequest", Message: "Failed to get DID doc's handle"}, &resolver)
	if err != nil || !doc.FromDIDDocument || doc.PDS != "https://example.com" || doc.DID != did {
		t.Fatalf("doc=%v error=%v", doc, err)
	}
}
