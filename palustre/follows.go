package palustre

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

// Only complete snapshots may replace exclusions or drive membership removals.
func FetchFollows(ctx context.Context, client *xrpc.Client, did string) (map[string]bool, error) {
	return fetchFollows(ctx, client, did, nil)
}

// FetchTargetFollows stops once every requested target is found. Missing targets
// are absent only after all pages have been read successfully.
func FetchTargetFollows(ctx context.Context, client *xrpc.Client, did string, targets map[string]bool) (map[string]bool, error) {
	return fetchFollows(ctx, client, did, targets)
}

func fetchFollows(ctx context.Context, client *xrpc.Client, did string, targets map[string]bool) (map[string]bool, error) {
	follows := make(map[string]bool)
	seen := make(map[string]bool)
	cursor := ""
	for {
		// Indigo's RepoListRecords decoder rejects the whole page if a single
		// record has an unknown type or malformed value. Decode values separately.
		var page struct {
			atproto.RepoListRecords_Output
			Records []struct {
				atproto.RepoListRecords_Record
				Value json.RawMessage `json:"value"`
			} `json:"records"`
		}
		params := map[string]any{"repo": did, "collection": FollowCollection, "limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		if err := client.LexDo(ctx, lexutil.Query, "", "com.atproto.repo.listRecords", params, nil, &page); err != nil {
			return nil, err
		}
		if page.Records == nil {
			return nil, fmt.Errorf("follow inventory response has no records array")
		}
		for _, record := range page.Records {
			uri, err := syntax.ParseATURI(record.Uri)
			if err != nil || uri.Authority().String() != did || uri.Collection().String() != FollowCollection || uri.RecordKey().String() == "" {
				return nil, fmt.Errorf("unexpected follow record URI %q in repository %s", record.Uri, did)
			}
			var follow bsky.GraphFollow
			if err := json.Unmarshal(record.Value, &follow); err != nil {
				slog.WarnContext(ctx, "skipping malformed follow record", "error", err)
				continue
			}
			if _, err := syntax.ParseDID(follow.Subject); err != nil {
				slog.WarnContext(ctx, "skipping invalid follow subject", "error", err)
				continue
			}
			if follow.LexiconTypeID != FollowCollection {
				slog.WarnContext(ctx, "skipping unexpected record type in follow inventory", "type", follow.LexiconTypeID)
				continue
			}
			if targets == nil || targets[follow.Subject] {
				follows[follow.Subject] = true
			}
		}
		if len(targets) > 0 && len(follows) == len(targets) {
			return follows, nil
		}
		if page.Cursor == nil || *page.Cursor == "" {
			break
		}
		if seen[*page.Cursor] {
			return nil, fmt.Errorf("follow inventory repeated pagination cursor")
		}
		seen[*page.Cursor] = true
		cursor = *page.Cursor
	}
	return follows, nil
}
