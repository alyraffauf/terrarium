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
		next, err := ListFollowPage(ctx, client, did, cursor, func(_, subject string) error {
			if subject != "" && (targets == nil || targets[subject]) {
				follows[subject] = true
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(targets) > 0 && len(follows) == len(targets) {
			return follows, nil
		}
		if next == "" {
			break
		}
		if seen[next] {
			return nil, fmt.Errorf("follow inventory repeated pagination cursor")
		}
		seen[next] = true
		cursor = next
	}
	return follows, nil
}

// ListFollowPage reads a PDS page and visits records with valid envelope URIs.
// Malformed record values are reported with an empty subject.
func ListFollowPage(ctx context.Context, client *xrpc.Client, did, cursor string, visit func(key, subject string) error) (string, error) {
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
		return "", err
	}
	if page.Records == nil {
		return "", fmt.Errorf("follow inventory response has no records array")
	}
	for _, record := range page.Records {
		uri, err := syntax.ParseATURI(record.Uri)
		if err != nil || uri.Authority().String() != did || uri.Collection().String() != FollowCollection || uri.RecordKey().String() == "" {
			return "", fmt.Errorf("unexpected follow record URI %q in repository %s", record.Uri, did)
		}
		var follow bsky.GraphFollow
		subject := ""
		err = json.Unmarshal(record.Value, &follow)
		_, didErr := syntax.ParseDID(follow.Subject)
		switch {
		case err != nil:
			slog.WarnContext(ctx, "skipping malformed follow record", "error", err)
		case didErr != nil:
			slog.WarnContext(ctx, "skipping invalid follow subject", "error", didErr)
		case follow.LexiconTypeID != FollowCollection:
			slog.WarnContext(ctx, "skipping unexpected record type in follow inventory", "type", follow.LexiconTypeID)
		default:
			subject = follow.Subject
		}
		if err := visit(uri.RecordKey().String(), subject); err != nil {
			return "", err
		}
	}
	if page.Cursor == nil {
		return "", nil
	}
	return *page.Cursor, nil
}
