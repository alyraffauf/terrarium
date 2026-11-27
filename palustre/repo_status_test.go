package palustre

import (
	"net/http"
	"testing"
)

func FuzzRepoStatusEvidence(f *testing.F) {
	for mode := byte(0); mode < 6; mode++ {
		f.Add(mode)
	}
	f.Fuzz(func(t *testing.T, mode byte) {
		reply := object{"did": owner}
		switch mode % 6 {
		case 0:
			reply["active"] = true
		case 1:
			reply["active"] = false
		case 2: // Missing required field.
		case 3:
			reply["active"] = nil
		case 4:
			reply["active"] = "false"
		case 5:
			reply["active"] = false
			reply["did"] = "did:example:other"
		}
		api := session(func(*http.Request) (int, any) { return 200, reply })
		active, err := RepoActive(t.Context(), api.Client, owner)
		if (err == nil) != (mode%6 < 2) || active != (mode%6 == 0) {
			t.Fatalf("status %v: active=%v error=%v", reply, active, err)
		}
	})
}
