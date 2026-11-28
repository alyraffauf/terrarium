package palustre

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCachedDIDRevalidatesMovedRepo(t *testing.T) {
	const did = "did:example:actor"
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		host := "one"
		if reads > 1 {
			host = "two"
		}
		fmt.Fprintf(w, `{"did":%q,"handle":"actor.example","pds":"https://%s.example"}`, did, host)
	}))
	defer server.Close()
	api := Session{Config: Config{Slingshot: server.URL}, HTTP: server.Client()}
	for _, wantHit := range []bool{false, true, true} {
		doc, hit, err := api.CachedDID(t.Context(), did)
		if err != nil || hit != wantHit || doc.PDS != "https://one.example" {
			t.Fatal(doc, hit, err)
		}
	}
	if err := api.ConfirmPDS(t.Context(), did, "https://one.example"); err == nil {
		t.Fatal("accepted old PDS after migration")
	}
	doc, hit, err := api.CachedDID(t.Context(), did)
	if err != nil || !hit || doc.PDS != "https://two.example" || reads != 2 {
		t.Fatal(doc, hit, err, reads)
	}
}
