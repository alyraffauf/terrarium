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
			follows[follow.Subject] = true
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
