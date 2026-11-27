package palustre

import (
	"context"
	"net/http"
	"testing"
)

type repeatedMemberKey struct{ keys }

func (store repeatedMemberKey) MemberKeys(ctx context.Context, target, did string) ([]string, error) {
	result, err := store.keys.MemberKeys(ctx, target, did)
	return append(result, result...), err
}

func TestReconcileKeepsMemberWhenInventoryRepeatsRecord(t *testing.T) {
	const key = "existing"
	api := session(func(req *http.Request) (int, any) {
		if req.URL.Path != "/xrpc/com.atproto.repo.getRecord" || req.URL.Query().Get("rkey") != key {
			t.Fatalf("unexpected request to %s", req.URL)
		}
		return 200, object{"cid": "current", "value": object{"$type": ItemCollection, "list": listURI, "subject": owner}}
	})

	store := repeatedMemberKey{keys{key: true}}
	member := Membership{Target: "target", ListURI: listURI, DID: owner, Include: true}
	if err := api.Reconcile(t.Context(), store, member); err != nil {
		t.Fatal(err)
	}
	if !store.keys[key] {
		t.Fatal("reconciliation forgot the retained list item")
	}
}
