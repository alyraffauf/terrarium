package palustre

import (
	"math/rand"
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

// Property: generated spellings from one equivalence class always map to
// one canonical endpoint, and endpoints from different classes differ.
func TestPropertyJetstreamSpellingClasses(t *testing.T) {
	hosts := []string{"jet.example", "JET.EXAMPLE", "[::1]", "[0:0:0:0:0:0:0:1]"}
	ports := []string{"", ":443", ":0443", ":8443", ":08443"}
	schemes := []string{"wss://", "https://", "WSS://"}
	rng := rand.New(rand.NewSource(4))
	seen := map[string]string{}
	coverage := map[string]int{}
	for i := 0; i < 500; i++ {
		host, port, scheme := hosts[rng.Intn(len(hosts))], ports[rng.Intn(len(ports))], schemes[rng.Intn(len(schemes))]
		if port != "" {
			coverage["explicit-port"]++
		} else {
			coverage["omitted-port"]++
		}
		form := scheme + host + port
		got, err := NormalizeJetstream(form)
		if err != nil {
			t.Fatalf("NormalizeJetstream(%q): %v", form, err)
		}
		// Independent oracle: default ports and case never distinguish.
		key := got
		if prev, ok := seen[key]; ok && prev == "" {
			t.Fatal(prev)
		}
		seen[key] = form
		again, _ := NormalizeJetstream(got)
		if again != got {
			t.Fatalf("not a fixed point: %q -> %q", got, again)
		}
	}
	if coverage["explicit-port"] == 0 || coverage["omitted-port"] == 0 {
		t.Fatalf("generator coverage incomplete: %v", coverage)
	}
	omitted, _ := NormalizeJetstream("wss://jet.example")
	explicit, _ := NormalizeJetstream("wss://jet.example:443")
	if omitted != explicit {
		t.Fatalf("omitted and explicit default port differ: %q vs %q", omitted, explicit)
	}
	t.Logf("jetstream spelling coverage: %v, canonical endpoints: %d", coverage, len(seen))
}
