// Package followers shares owner-follow and stream recovery logic between Helix and Capensis.
package followers

import (
	"context"
	"errors"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

var ErrSnapshotSuperseded = errors.New("owner follows changed during refresh")

type SnapshotStore interface {
	FollowVersions(context.Context) (current, applied int64, err error)
	ApplyFollowSnapshot(context.Context, int64, map[string]bool) error
}

// RefreshSnapshot applies a complete owner inventory at the version observed before fetching.
func RefreshSnapshot(ctx context.Context, db SnapshotStore, client *xrpc.Client, force bool) error {
	version, applied, err := db.FollowVersions(ctx)
	if err != nil || (!force && version == applied) {
		return err
	}
	follows, err := palustre.FetchFollows(ctx, client, client.Auth.Did)
	if err != nil {
		return err
	}
	return db.ApplyFollowSnapshot(ctx, version, follows)
}

func CheckSnapshot(ctx context.Context, db SnapshotStore) error {
	version, applied, err := db.FollowVersions(ctx)
	if err != nil {
		return err
	}
	if version != applied {
		return ErrSnapshotSuperseded
	}
	return nil
}
