package capensis

import "testing"

// Regression: spaces around "=" in a list binding either failed with a
// URL parse error about a space in the host name (before the "=") or
// stored a list URI with a leading space that validate() later rejected.
func TestParseTargetsTrimsBindingWhitespace(t *testing.T) {
	uri := "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/key1"
	for _, binding := range []string{
		"a.example=" + uri,
		"a.example = " + uri,
		" a.example = " + uri + " ",
		"a.example= " + uri,
		"A.EXAMPLE =" + uri,
	} {
		targets, err := ParseTargets([]string{"a.example"}, []string{binding})
		if err != nil {
			t.Fatalf("binding %q: %v", binding, err)
		}
		if len(targets) != 1 || targets[0].ListURI != uri {
			t.Fatalf("binding %q stored %q, want %q", binding, targets[0].ListURI, uri)
		}
	}
}
