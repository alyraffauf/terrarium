package cordata

import (
	"context"
	"fmt"
	"github.com/alyraffauf/terrarium/palustre"
	"github.com/alyraffauf/terrarium/palustre/listlex"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func postURI(ctx context.Context, client *xrpc.Client, raw string) (string, error) {
	const collection = "app.bsky.feed.post"
	if !strings.HasPrefix(raw, "at://") {
		link, err := url.Parse(raw)
		if err != nil || link.Scheme != "https" || link.Host != "bsky.app" {
			return "", fmt.Errorf("expected a Bluesky post link or AT URI")
		}
		parts := strings.Split(strings.Trim(link.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "profile" || parts[2] != "post" {
			return "", fmt.Errorf("expected a link to a single post")
		}
		raw = "at://" + parts[1] + "/" + collection + "/" + parts[3]
	}
	uri, err := syntax.ParseATURI(raw)
	if err != nil || uri.Collection().String() != collection || uri.RecordKey() == "" {
		return "", fmt.Errorf("invalid post URI")
	}
	actor := uri.Authority().String()
	if !strings.HasPrefix(actor, "did:") {
		resolved, err := atproto.IdentityResolveHandle(ctx, client, actor)
		if err != nil {
			return "", err
		}
		actor = resolved.Did
	}
	return "at://" + actor + "/" + collection + "/" + uri.RecordKey().String(), nil
}

// The owner's repo is the durable membership store. Rebuild this index each scan.
type inventory map[string][]string

func (items inventory) MemberKeys(_ context.Context, _, did string) ([]string, error) {
	return items[did], nil
}
func (items inventory) RememberMember(_ context.Context, _, did, key string) error {
	return nil // Reconcile visits each DID once per scan.
}
func (items inventory) ForgetMember(_ context.Context, _, key string) error { return nil }

func pages(read func(string) (*string, error)) error {
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

func syncPost(ctx context.Context, api *palustre.Session, view *xrpc.Client, post, list string) error {
	wanted := map[string]bool{}
	err := pages(func(cursor string) (*string, error) {
		var page struct {
			Cursor *string `json:"cursor"`
			Likes  []struct {
				Actor struct {
					Did string `json:"did"`
				} `json:"actor"`
			} `json:"likes"`
		}
		params := map[string]any{"uri": post, "limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		err := view.LexDo(ctx, lexutil.Query, "", "app.bsky.feed.getLikes", params, nil, &page)
		if err != nil {
			return nil, err
		}
		for _, like := range page.Likes {

			if _, err := syntax.ParseDID(like.Actor.Did); err != nil {
				return nil, err
			}
			wanted[like.Actor.Did] = true
		}
		return page.Cursor, nil
	})
	if err != nil {
		return err
	}
	items := inventory{}
	owner := api.Client.Auth.Did
	err = pages(func(cursor string) (*string, error) {
		page, err := atproto.RepoListRecords(ctx, api.Client, palustre.ItemCollection, cursor, 100, owner, false)
		if err != nil {
			return nil, err
		}
		for _, record := range page.Records {
			if record == nil || record.Value == nil {
				return nil, fmt.Errorf("empty list item")
			}
			item, ok := record.Value.Val.(*listlex.GraphListitem)
			if !ok || item.List != list {
				continue
			}
			uri, err := syntax.ParseATURI(record.Uri)
			if err != nil {
				return nil, err
			}
			items[item.Subject] = append(items[item.Subject], uri.RecordKey().String())
			if _, ok := wanted[item.Subject]; !ok {
				wanted[item.Subject] = false
			}
		}
		return page.Cursor, nil
	})
	if err != nil {
		return err
	}
	for did, include := range wanted {
		if err := api.Refresh(ctx); err != nil {
			return err
		}
		if err := api.Reconcile(ctx, items, palustre.Membership{Target: post, ListURI: list, DID: did, Include: include}); err != nil {
			return err
		}
	}
	return nil
}

func Run(ctx context.Context, cfg palustre.Config, appView, raw string, interval time.Duration) error {
	if cfg.Identifier == "" || cfg.Password == "" {
		return fmt.Errorf("identifier and app-password are required")
	}
	if interval <= 0 || cfg.WriteInterval < 0 {
		return fmt.Errorf("rescan-interval must be positive and write-interval cannot be negative")
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	view := &xrpc.Client{Host: appView, Client: httpClient}
	post, err := postURI(ctx, view, raw)
	if err != nil {
		return err
	}
	api := &palustre.Session{Config: cfg, HTTP: httpClient}
	if err := api.Login(ctx); err != nil {
		return err
	}
	key := palustre.RecordKey("cordata\n" + post)
	list := "at://" + api.Client.Auth.Did + "/" + palustre.ListCollection + "/" + key
	for ctx.Err() == nil {
		err := api.Refresh(ctx)
		if err == nil {
			err = api.EnsurePostList(ctx, list, post)
		}
		if err == nil {
			slog.Info("maintaining list", "post", post, "list", list)
			err = syncPost(ctx, api, view, post, list)
		}
		delay := interval
		if err != nil && ctx.Err() == nil {
			slog.Error("post sync failed", "error", err)
			delay = time.Minute
		}
		if !palustre.Pause(ctx, delay) {
			break
		}
	}
	return nil
}
