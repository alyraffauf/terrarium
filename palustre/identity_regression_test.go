package palustre

import "testing"

// Regression: equivalent PDS spellings (leading-zero ports, expanded IPv6)
// normalized to different strings, so duplicate detection and list-binding
// lookups treated one endpoint as two. Impossible ports were accepted.
func TestNormalizePDSRegressionEquivalentPortsAndIPs(t *testing.T) {
	equivalent := []struct {
		canonical string
		forms     []string
	}{
		{"https://a.example", []string{"https://a.example:0443", "https://a.example:443", "a.example:0443"}},
		{"http://a.example", []string{"http://a.example:080", "http://a.example:80"}},
		{"https://a.example:8443", []string{"https://a.example:08443"}},
		{"https://[::1]", []string{"https://[0:0:0:0:0:0:0:1]", "https://[0:0:0:0:0:0:0:1]:0443"}},
		{"https://[2001:db8::1]:8443", []string{"https://[2001:0db8:0000:0000:0000:0000:0000:0001]:08443"}},
	}
	for _, group := range equivalent {
		for _, form := range group.forms {
			got, err := NormalizePDS(form)
			if err != nil || got != group.canonical {
				t.Fatalf("NormalizePDS(%q) = %q, %v; want %q", form, got, err, group.canonical)
			}
		}
	}
	for _, invalid := range []string{"https://a.example:0", "https://a.example:65536", "https://a.example:99999", "http://a.example:00"} {
		if got, err := NormalizePDS(invalid); err == nil {
			t.Fatalf("NormalizePDS(%q) = %q, want error", invalid, got)
		}
	}
}
