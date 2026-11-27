package palustre

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

// WalkListItems reads the owner's entire list-item collection. Callers stage
// observations until the walk succeeds so a partial page cannot imply removal.
func WalkListItems(ctx context.Context, client *xrpc.Client, visit func(list, did, key string) error) error {
	cursor := ""
	seen := map[string]bool{}
	for {
		next, err := ListItemPage(ctx, client, cursor, visit)
		if err != nil || next == "" {
			return err
		}
		cursor = next
		if seen[cursor] {
			return fmt.Errorf("repeated list-item inventory cursor")
		}
		seen[cursor] = true
	}
}

// ListItemPage validates each record before passing its list URI, subject DID,
// and record key to visit. Callers control pagination and retry scheduling.
func ListItemPage(ctx context.Context, client *xrpc.Client, cursor string, visit func(list, did, key string) error) (string, error) {
	owner := client.Auth.Did
	page, err := atproto.RepoListRecords(ctx, client, ItemCollection, cursor, 100, owner, false)
	if err != nil {
		return "", err
	}
	if page.Records == nil {
		return "", fmt.Errorf("list-item inventory missing records")
	}
	for _, record := range page.Records {
		if record == nil || record.Value == nil {
			return "", fmt.Errorf("empty list item")
		}
		item, ok := record.Value.Val.(*bsky.GraphListitem)
		if !ok {
			return "", fmt.Errorf("unexpected record type in list-item inventory")
		}
		uri, err := syntax.ParseATURI(record.Uri)
		if err != nil || uri.Authority().String() != owner || uri.Collection().String() != ItemCollection || uri.RecordKey() == "" {
			return "", fmt.Errorf("unexpected list-item URI %q", record.Uri)
		}
		if _, err := syntax.ParseDID(item.Subject); err != nil {
			return "", err
		}
		list, err := syntax.ParseATURI(item.List)
		if err != nil || list.Collection().String() != ListCollection || list.RecordKey() == "" {
			return "", fmt.Errorf("invalid list reference %q", item.List)
		}
		if err := visit(item.List, item.Subject, uri.RecordKey().String()); err != nil {
			return "", err
		}
	}
	if page.Cursor == nil {
		return "", nil
	}
	return *page.Cursor, nil
}
