package palustre

import (
	"context"
	"errors"
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

// VerifyRepoReadError checks ambiguous missing-repo responses against the
// repository status on the confirmed PDS. A nil result confirms absence.
func VerifyRepoReadError(ctx context.Context, c *xrpc.Client, did string, readErr error) error {
	var response *xrpc.Error
	if !errors.As(readErr, &response) || response.StatusCode < 400 || response.StatusCode >= 500 {
		return readErr
	}
	var details *xrpc.XRPCError
	missing := errors.As(readErr, &details) && details.ErrStr == "InvalidRequest" && details.Message == "Could not find repo: "+did
	if !missing && !repoUnavailable(readErr) {
		return readErr
	}
	active, err := RepoActive(ctx, c, did)
	if err == nil && !active || repoUnavailable(err) {
		return nil
	}
	return errors.Join(readErr, err)
}

func repoUnavailable(err error) bool {
	var response *xrpc.Error
	return errors.As(err, &response) && response.StatusCode >= 400 && response.StatusCode < 500 &&
		(XRPCCode(err, "RepoNotFound") || XRPCCode(err, "RepoDeactivated") || XRPCCode(err, "RepoTakendown"))
}
