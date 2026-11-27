package palustre

import (
	"context"
	"fmt"
	"github.com/bluesky-social/indigo/api/atproto"
	lex "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

func RepoActive(ctx context.Context, client *xrpc.Client, did string) (bool, error) {
	// The generated boolean has the same zero value for false and an absent
	// field. Require an explicit answer before a worker can remove membership.
	var status struct {
		atproto.SyncGetRepoStatus_Output
		Active *bool `json:"active"`
	}
	err := client.LexDo(ctx, lex.Query, "", "com.atproto.sync.getRepoStatus", map[string]any{"did": did}, nil, &status)
	if err != nil {
		return false, err
	}
	if status.Did != did {
		return false, fmt.Errorf("unexpected status DID %q", status.Did)
	}
	if status.Active == nil {
		return false, fmt.Errorf("repository status for %s missing active flag", did)
	}
	return *status.Active, nil
}
