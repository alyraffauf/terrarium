package palustre

import (
	"testing"
)

// Regression: Jetstream canonicalization kept an explicit default port,
// so wss://jet.example, wss://jet.example:443 and wss://jet.example:0443
// were three different endpoint strings even though the stream cursor is
// keyed by that string. Respelling the flag abandoned the saved cursor.
func TestNormalizeJetstreamEquivalentForms(t *testing.T) {
	equivalent := []struct {
		canonical string
		forms     []string
	}{
		{"wss://jet.example" + StreamPath, []string{
			"wss://jet.example", "wss://jet.example:443", "wss://jet.example:0443",
			"WSS://JET.EXAMPLE", "https://jet.example", "https://JET.example:443/",
			"wss://jet.example" + StreamPath, "wss://jet.example:443" + StreamPath,
		}},
		{"ws://jet.example" + StreamPath, []string{"ws://jet.example", "ws://jet.example:80", "http://jet.example:080"}},
		{"wss://jet.example:8443" + StreamPath, []string{"wss://jet.example:8443", "wss://jet.example:08443"}},
		{"wss://[::1]" + StreamPath, []string{"wss://[0:0:0:0:0:0:0:1]", "wss://[0:0:0:0:0:0:0:1]:0443"}},
	}
	for _, group := range equivalent {
		for _, form := range group.forms {
			got, err := NormalizeJetstream(form)
			if err != nil || got != group.canonical {
				t.Fatalf("NormalizeJetstream(%q) = %q, %v; want %q", form, got, err, group.canonical)
			}
			again, err := NormalizeJetstream(got)
			if err != nil || again != got {
				t.Fatalf("NormalizeJetstream not idempotent: %q -> %q -> %q (%v)", form, got, again, err)
			}
			if !StreamEndpointsEqual(form, group.canonical) {
				t.Fatalf("StreamEndpointsEqual(%q, %q) = false", form, group.canonical)
			}
		}
	}
	for _, invalid := range []string{
		"wss://jet.example:0", "wss://jet.example:99999", "wss://jet.example:65536",
		"ftp://jet.example", "wss://user@jet.example", "wss://jet.example/other?x=1",
		"wss://jet.example" + StreamPath + "?cursor=1", "wss://",
	} {
		if got, err := NormalizeJetstream(invalid); err == nil {
			t.Fatalf("NormalizeJetstream(%q) = %q, want error", invalid, got)
		}
	}
	if StreamEndpointsEqual("wss://a.example"+StreamPath, "wss://b.example"+StreamPath) {
		t.Fatal("different endpoints reported equal")
	}
}
