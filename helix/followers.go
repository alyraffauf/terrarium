package helix

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

func (api *listTarget) backfill(ctx context.Context, db *store) error {
	if err := db.queueMembers(ctx, api.actor); err != nil {
		return err
	}
	client := &xrpc.Client{Host: api.cfg.AppView, Client: api.HTTP}
	seen := map[string]bool{}
	cursor := ""
	for {
		page, err := bsky.GraphGetFollowers(ctx, client, api.actor, cursor, 100, "")
		if err != nil {
			return err
		}
		if page.Subject == nil || page.Subject.Did != api.actor {
			return fmt.Errorf("follower response subject does not match %s", api.actor)
		}
		if page.Followers == nil {
			return fmt.Errorf("follower response has no followers array")
		}
		dids := make([]string, 0, len(page.Followers))
		for _, follower := range page.Followers {
			if follower == nil {
				return fmt.Errorf("follower response includes a null profile")
			}
			if _, err := syntax.ParseDID(follower.Did); err != nil {
				return err
			}
			dids = append(dids, follower.Did)
		}
		if err := db.queueScan(ctx, dids...); err != nil {
			return err
		}
		if page.Cursor == nil || *page.Cursor == "" {
			return nil
		}
		if seen[*page.Cursor] {
			return fmt.Errorf("follower backfill repeated pagination cursor")
		}
		seen[*page.Cursor] = true
		cursor = *page.Cursor
	}
}

func (api *session) subject(ctx context.Context, db *store, did string, targets ...string) (subjectState, error) {
	subject := subjectState{did: did}
	followed, err := db.isFollowed(ctx, did)
	if err != nil {
		return subject, err
	}
	subject.excluded = followed || did == api.Client.Auth.Did
	if subject.excluded {
		return subject, nil
	}
	doc, resolveErr := api.Resolve(ctx, did)
	host := doc.PDS
	if resolveErr != nil {
		host, err = db.candidatePDS(ctx, did)
		if err != nil || host == "" {
			return subject, resolveErr
		}
	} else if err := db.saveCandidatePDS(ctx, did, host); err != nil {
		return subject, err
	}
	client := &xrpc.Client{Host: host, Client: api.HTTP}
	status, err := atproto.SyncGetRepoStatus(ctx, client, did)
	if xrpcCode(err, "RepoNotFound") || xrpcCode(err, "RepoDeactivated") || xrpcCode(err, "RepoTakendown") {
		return subject, nil
	}
	if err != nil {
		return subject, err
	}
	if status.Did != did {
		return subject, fmt.Errorf("repository status DID does not match %s", did)
	}
	if !status.Active {
		return subject, nil
	}
	if resolveErr != nil {
		return subject, resolveErr
	}
	watched := make(map[string]bool, len(targets))
	for _, target := range targets {
		watched[target] = true
	}
	subject.follows, err = palustre.FetchTargetFollows(ctx, client, did, watched)
	return subject, err
}
