package helix

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

func (api *listTarget) backfill(ctx context.Context, db *store) error {
	if err := db.queueMembers(ctx, api.actor); err != nil {
		return err
	}
	client := &xrpc.Client{Host: api.cfg.AppView, Client: api.HTTP}
	cursor := ""
	for {
		var page struct {
			Cursor    *string `json:"cursor"`
			Followers []struct {
				Did string `json:"did"`
			} `json:"followers"`
		}
		err := client.LexDo(ctx, lexutil.Query, "", "app.bsky.graph.getFollowers", map[string]any{"actor": api.actor, "cursor": cursor, "limit": 100}, nil, &page)
		if err != nil {
			return err
		}
		for _, follower := range page.Followers {
			if _, err := syntax.ParseDID(follower.Did); err != nil {
				return err
			}
			if err := db.queueScan(ctx, follower.Did); err != nil {
				return err
			}
		}
		if page.Cursor == nil || *page.Cursor == "" {
			return nil
		}
		if *page.Cursor == cursor {
			return fmt.Errorf("follower backfill repeated pagination cursor")
		}
		cursor = *page.Cursor
	}
}

func (api *session) subject(ctx context.Context, db *store, did string) (subjectState, error) {
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
	if !status.Active {
		return subject, nil
	}
	if resolveErr != nil {
		return subject, resolveErr
	}
	subject.follows, err = palustre.FetchFollows(ctx, client, did)
	return subject, err
}
