package cordata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

func parsePost(raw string) (syntax.ATURI, error) {
	const collection = "app.bsky.feed.post"
	if !strings.HasPrefix(raw, "at://") {
		link, err := url.Parse(raw)
		if err != nil || link.Scheme != "https" || link.Host != "bsky.app" || link.User != nil {
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
	return uri, nil
}

func resolvePost(ctx context.Context, api *palustre.Session, uri syntax.ATURI) (string, error) {
	actor := uri.Authority().String()
	if !strings.HasPrefix(actor, "did:") {
		resolved, err := api.Resolve(ctx, actor)
		if err != nil {
			return "", err
		}
		actor = resolved.DID
	}
	return "at://" + actor + "/" + uri.Collection().String() + "/" + uri.RecordKey().String(), nil
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

func syncPost(ctx context.Context, api *palustre.Session, view *xrpc.Client, constellation, post, list string) error {
	likers := map[string]bool{}
	appErr := palustre.WalkPages(func(cursor string) (*string, error) {
		page, err := bsky.FeedGetLikes(ctx, view, "", cursor, 100, post)
		if err != nil {
			return nil, err
		}
		if page.Uri != post || page.Likes == nil {
			return nil, fmt.Errorf("invalid likes inventory for %s", post)
		}
		for _, like := range page.Likes {
			if like == nil || like.Actor == nil {
				return nil, fmt.Errorf("like missing actor")
			}
			if _, err := syntax.ParseDID(like.Actor.Did); err != nil {
				return nil, err
			}
			likers[like.Actor.Did] = true
		}
		return page.Cursor, nil
	})
	if appErr != nil {
		clear(likers)
	}
	linkErr := scanLikeBacklinks(ctx, api, constellation, post, likers)
	discoveryErr := errors.Join(appErr, linkErr)
	items := inventory{}
	err := palustre.WalkListItems(ctx, api.Client, func(uri, did, key string) error {
		if uri == list {
			items[did] = append(items[did], key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	subjects := maps.Clone(likers)
	for did := range items {
		if discoveryErr == nil {
			subjects[did] = true
		}
	}
	var firstFailure error
	for _, did := range slices.Sorted(maps.Keys(subjects)) {
		if err := api.Refresh(ctx); err != nil {
			return err
		}
		if err := api.Reconcile(ctx, items, palustre.Membership{Target: post, ListURI: list, DID: did, Include: likers[did]}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if firstFailure == nil {
				firstFailure = fmt.Errorf("reconcile %s: %w", did, err)
			}
		}
	}
	return errors.Join(firstFailure, discoveryErr)
}

func scanLikeBacklinks(ctx context.Context, api *palustre.Session, endpoint, post string, likers map[string]bool) error {
	const collection = "app.bsky.feed.like"
	client := &xrpc.Client{Host: endpoint, Client: api.HTTP}
	var verifyErr error
	pageErr := palustre.WalkPages(func(cursor string) (*string, error) {
		rows, next, err := palustre.BacklinksPage(ctx, client, post, collection, "subject.uri", cursor)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if likers[row.Did] {
				continue
			}
			ok, err := verifyLike(ctx, api, row, post)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if verifyErr == nil {
					verifyErr = err
				}
				continue
			}
			if ok {
				likers[row.Did] = true
			}
		}
		return next, nil
	})
	return errors.Join(pageErr, verifyErr)
}

func verifyLike(ctx context.Context, api *palustre.Session, row *palustre.Backlink, post string) (bool, error) {
	doc, cached, err := api.CachedDID(ctx, row.Did)
	if err != nil {
		return false, err
	}
	record, err := atproto.RepoGetRecord(ctx, &xrpc.Client{Host: doc.PDS, Client: api.HTTP}, "", row.Collection, row.Did, row.Rkey)
	if cached && palustre.MayHaveMoved(err) {
		if confirmErr := api.ConfirmPDS(ctx, row.Did, doc.PDS); confirmErr != nil {
			return false, confirmErr
		}
	}
	if palustre.XRPCCode(err, "RecordNotFound") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if record.Uri != "at://"+row.Did+"/"+row.Collection+"/"+row.Rkey || record.Value == nil {
		return false, fmt.Errorf("invalid like record for %s", row.Did)
	}
	like, ok := record.Value.Val.(*bsky.FeedLike)
	if !ok || like.Subject == nil {
		return false, fmt.Errorf("invalid like value for %s", row.Did)
	}
	return like.Subject.Uri == post, nil
}

type Config struct {
	palustre.Config
	AppView, Post, Constellation string
	RescanInterval               time.Duration
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Identifier == "" || cfg.Password == "" {
		return fmt.Errorf("identifier and app-password are required")
	}
	if cfg.RescanInterval <= 0 || cfg.WriteInterval < 0 {
		return fmt.Errorf("rescan-interval must be positive and write-interval cannot be negative")
	}
	uri, err := parsePost(cfg.Post)
	if err != nil {
		return err
	}
	for _, base := range []*string{&cfg.AppView, &cfg.Slingshot, &cfg.Constellation} {
		*base, err = palustre.NormalizePDS(*base)
		if err != nil {
			return err
		}
	}
	if cfg.LoginPDS != "" {
		cfg.LoginPDS, err = palustre.NormalizePDS(cfg.LoginPDS)
		if err != nil {
			return err
		}
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	view := &xrpc.Client{Host: cfg.AppView, Client: httpClient}
	api := &palustre.Session{Config: cfg.Config, HTTP: httpClient}
	post, err := resolvePost(ctx, api, uri)
	if err != nil {
		return err
	}
	if err := api.Login(ctx); err != nil {
		return err
	}
	key := palustre.RecordKey("cordata\n" + post)
	list := "at://" + api.Client.Auth.Did + "/" + palustre.ListCollection + "/" + key
	var retryDelay time.Duration
	for ctx.Err() == nil {
		err := api.Refresh(ctx)
		if err == nil {
			err = api.EnsurePostList(ctx, list, post)
		}
		if err == nil {
			slog.Info("maintaining list", "post", post, "list", list)
			err = syncPost(ctx, api, view, cfg.Constellation, post, list)
		}
		delay := cfg.RescanInterval
		if err != nil && ctx.Err() == nil {
			slog.Error("post sync failed", "error", err)
			retryDelay = palustre.ScanRetryDelay(retryDelay, cfg.RescanInterval)
			delay = retryDelay
		} else {
			retryDelay = 0
		}
		if !palustre.Pause(ctx, delay) {
			break
		}
	}
	return nil
}
