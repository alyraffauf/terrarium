package palustre

import (
	"context"
	"fmt"

	"github.com/alyraffauf/terrarium/palustre/microcosm"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

const DefaultConstellation = "https://constellation.microcosm.blue"

type Backlink = microcosm.LinksGetBacklinks_LinkRecord

func BacklinksPage(ctx context.Context, client *xrpc.Client, subject, collection, path, cursor string) ([]*Backlink, *string, error) {
	page, err := microcosm.LinksGetBacklinks(ctx, client, cursor, nil, 100, collection+":"+path, subject)
	if err != nil {
		return nil, nil, err
	}
	if page.Records == nil {
		return nil, nil, fmt.Errorf("backlinks response missing records")
	}
	for _, r := range page.Records {
		if r == nil || r.Collection != collection {
			return nil, nil, fmt.Errorf("unexpected backlink for %s", collection)
		}
		if _, err := syntax.ParseDID(r.Did); err != nil {
			return nil, nil, err
		}
		if _, err := syntax.ParseRecordKey(r.Rkey); err != nil {
			return nil, nil, err
		}
	}
	return page.Records, page.Cursor, nil
}

func WalkPages(read func(string) (*string, error)) error {
	seen := map[string]bool{}
	cursor := ""
	for {
		next, err := read(cursor)
		if err != nil {
			return err
		}
		if next == nil || *next == "" {
			return nil
		}
		if seen[*next] {
			return fmt.Errorf("repeated pagination cursor")
		}
		cursor = *next
		seen[cursor] = true
	}
}
