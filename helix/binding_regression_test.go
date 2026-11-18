package helix

import "testing"

// Regression: spaces around "=" in a list binding stored a list URI with
// a leading space that validate() later rejected as a malformed AT URI.
func TestParseTargetsTrimsBindingWhitespace(t *testing.T) {
	uri := "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/key1"
	for _, binding := range []string{
		"a.example=" + uri,
		"a.example = " + uri,
		" a.example = " + uri + " ",
		"a.example= " + uri,
		"@A.example =" + uri,
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
