package palustre

import "testing"
import "strings"

func FuzzEmptyEndpointQuery(f *testing.F) {
	f.Add("example.com")
	f.Fuzz(func(t *testing.T, host string) {
		for _, scheme := range []string{"https", "wss"} {
			normalize := NormalizePDS
			if scheme == "wss" {
				normalize = NormalizeJetstream
			}
			base := scheme + "://" + host
			// Construct an authority only. Otherwise the suffix can extend a path,
			// fragment, or an existing query.
			if strings.ContainsAny(host, "/?#") {
				return
			}
			want, err := normalize(base)
			if err != nil {
				continue
			}
			got, err := normalize(base + "?")
			if err != nil || got != want {
				t.Fatalf("%q: got %q, %v; want %q", base+"?", got, err, want)
			}
		}
	})
}
