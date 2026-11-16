package helix

import (
	"context"

	"github.com/alyraffauf/terrarium/palustre"
)

func (api *session) syncFollows(ctx context.Context, db *store, force bool) error {
	version, applied, err := db.followVersion(ctx)
	if err != nil {
		return err
	}
	if !force && version == applied {
		return nil
	}
	follows, err := palustre.FetchFollows(ctx, api.Client, api.Client.Auth.Did)
	if err != nil {
		return err
	}
	return db.applyFollowSnapshot(ctx, version, follows)
}
