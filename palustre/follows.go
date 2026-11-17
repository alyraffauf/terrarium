package palustre

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

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
	cursor := ""
	for {
		var page struct {
			Cursor  string `json:"cursor"`
			Records []struct {
				Value json.RawMessage `json:"value"`
			} `json:"records"`
		}
		params := map[string]any{"repo": did, "collection": "app.bsky.graph.follow", "limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		if err := client.LexDo(ctx, lexutil.Query, "", "com.atproto.repo.listRecords", params, nil, &page); err != nil {
			return nil, err
		}
		for _, record := range page.Records {
			var follow struct {
				Subject string `json:"subject"`
			}
			if err := json.Unmarshal(record.Value, &follow); err != nil {
				slog.Warn("skipping malformed follow record", "error", err)
				continue
			}
			if _, err := syntax.ParseDID(follow.Subject); err != nil {
				slog.Warn("skipping invalid follow subject", "error", err)
				continue
			}
			if targets == nil || targets[follow.Subject] {
				follows[follow.Subject] = true
			}
			if len(targets) > 0 && len(follows) == len(targets) {
				return follows, nil
			}
		}
		if page.Cursor == "" {
			break
		}
		if cursor == page.Cursor {
			return nil, fmt.Errorf("follow inventory repeated pagination cursor")
		}
		cursor = page.Cursor
	}
	return follows, nil
}
