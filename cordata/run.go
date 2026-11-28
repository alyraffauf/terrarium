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
	"sync"
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

// Serialize authenticated operations across posts. A worker holds the lock for
// one membership decision, never for a whole scan or retry delay.
type ownerSession struct {
	inventories ownerInventories
	readSlots   chan struct{}
	readHosts   palustre.HostLimiter
	initSlots   sync.Once
	*palustre.Session
	mu sync.Mutex
}

func (owner *ownerSession) write(ctx context.Context, apply func() error) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.Refresh(ctx); err != nil {
		return err
	}
	return apply()
}

func (owner *ownerSession) reader() *xrpc.Client {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	client := *owner.Client
	auth := *client.Auth
	client.Auth = &auth
	return &client
}

type likeRecordKey struct{ did, rkey string }

type verificationRetry struct {
	failures int
	due      time.Time
}

type postScan struct {
	post, list string
	retries    map[likeRecordKey]verificationRetry
}

func (scan *postScan) sync(ctx context.Context, api *ownerSession, view *xrpc.Client, constellation string, now func() time.Time) error {
	post, list := scan.post, scan.list
	partition := api.inventories.request(list)
	defer partition.close()
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
	uncertain, linkErr := scan.backlinks(ctx, api, constellation, likers, now)
	discoveryErr := errors.Join(appErr, linkErr)
	items, err := partition.read(ctx, api.reader())
	if err != nil {
		return err
	}
	subjects := maps.Clone(likers)
	for did := range items {
		if discoveryErr == nil && !uncertain[did] {
			subjects[did] = true
		}
	}
	var firstFailure error
	for _, did := range slices.Sorted(maps.Keys(subjects)) {
		if err := api.write(ctx, func() error {
			return api.Reconcile(ctx, items, palustre.Membership{Target: post, ListURI: list, DID: did, Include: likers[did]})
		}); err != nil {
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

func (scan *postScan) backlinks(ctx context.Context, api *ownerSession, endpoint string, likers map[string]bool, now func() time.Time) (map[string]bool, error) {
	nextRetries := map[likeRecordKey]verificationRetry{}
	const collection = "app.bsky.feed.like"
	client := &xrpc.Client{Host: endpoint, Client: api.HTTP}
	pageErr := palustre.WalkPages(func(cursor string) (*string, error) {
		rows, next, err := palustre.BacklinksPage(ctx, client, scan.post, collection, "subject.uri", cursor)
		if err != nil {
			return nil, err
		}
		byActor := make(map[string][]*palustre.Backlink)
		seen := make(map[likeRecordKey]bool)
		for _, row := range rows {
			key := likeRecordKey{row.Did, row.Rkey}
			if likers[row.Did] || seen[key] {
				continue
			}
			seen[key] = true
			if _, deferred := nextRetries[key]; deferred {
				continue
			}
			if retry := scan.retries[key]; now().Before(retry.due) {
				nextRetries[key] = retry
				continue
			}
			byActor[row.Did] = append(byActor[row.Did], row)
		}
		type result struct {
			key   likeRecordKey
			liked bool
			err   error
		}
		results := make(chan result, len(rows))
		api.initSlots.Do(func() { api.readSlots = make(chan struct{}, 4) })
		var workers sync.WaitGroup
		for _, records := range byActor {
			select {
			case api.readSlots <- struct{}{}:
			case <-ctx.Done():
				workers.Wait()
				return nil, ctx.Err()
			}
			workers.Go(func() {
				defer func() { <-api.readSlots }()
				for _, row := range records {
					ok, err := verifyLike(ctx, api, row, scan.post)
					results <- result{likeRecordKey{row.Did, row.Rkey}, ok, err}
					if ok || ctx.Err() != nil {
						break
					}
				}
			})
		}
		workers.Wait()
		close(results)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for verified := range results {
			key := verified.key
			retry := scan.retries[key]
			if verified.err != nil {
				api.VerificationDeferred.Add(1)
				delay := palustre.VerificationRetryDelay(retry.failures)
				nextRetries[key] = verificationRetry{failures: retry.failures + 1, due: now().Add(delay)}
				slog.Warn("like verification deferred", "post", scan.post, "did", key.did, "rkey", key.rkey, "retry_in", delay, "error", verified.err)
				continue
			}
			if retry.failures > 0 {
				api.VerificationRecovered.Add(1)
			}
			delete(scan.retries, key)
			if verified.liked {
				likers[key.did] = true
			}
		}
		return next, nil
	})
	if pageErr != nil {
		// An interrupted walk can omit records still waiting for their retry.
		for key, retry := range scan.retries {
			if _, seen := nextRetries[key]; !seen && !likers[key.did] {
				nextRetries[key] = retry
			}
		}
	}
	scan.retries = nextRetries
	uncertain := make(map[string]bool, len(nextRetries))
	for key := range nextRetries {
		uncertain[key.did] = true
	}
	return uncertain, pageErr
}

func verifyLike(ctx context.Context, api *ownerSession, row *palustre.Backlink, post string) (bool, error) {
	doc, cached, err := api.CachedDID(ctx, row.Did)
	if err != nil {
		return false, err
	}
	release, err := api.readHosts.Acquire(ctx, doc.PDS)
	if err != nil {
		return false, err
	}
	defer release()
	client := &xrpc.Client{Host: doc.PDS, Client: api.HTTP}
	reader := api.PDSVerificationClient(client)
	record, err := atproto.RepoGetRecord(ctx, reader, "", row.Collection, row.Did, row.Rkey)
	if cached && palustre.MayHaveMoved(err) {
		if confirmErr := api.ConfirmPDS(ctx, row.Did, doc.PDS); confirmErr != nil {
			return false, confirmErr
		}
	}
	if palustre.XRPCCode(err, "RecordNotFound") {
		return false, nil
	}
	if err != nil {
		return false, palustre.VerifyRepoReadError(ctx, reader, row.Did, err)
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
	AppView, Post, Constellation, MetricsAddress string
	Posts                                        []string
	RescanInterval                               time.Duration
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Identifier == "" || cfg.Password == "" {
		return fmt.Errorf("identifier and app-password are required")
	}
	if cfg.RescanInterval <= 0 || cfg.WriteInterval < 0 {
		return fmt.Errorf("rescan-interval must be positive and write-interval cannot be negative")
	}
	inputs := slices.Clone(cfg.Posts)
	if cfg.Post != "" {
		inputs = append(inputs, cfg.Post)
	}
	if len(inputs) == 0 {
		return fmt.Errorf("at least one post is required")
	}
	uris := make([]syntax.ATURI, 0, len(inputs))
	for _, input := range inputs {
		uri, err := parsePost(input)
		if err != nil {
			return fmt.Errorf("%q: %w", input, err)
		}
		uris = append(uris, uri)
	}
	var err error
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
	stopMetrics, err := api.StartMetrics(cfg.MetricsAddress, nil)
	if err != nil {
		return err
	}
	defer stopMetrics()
	if err := api.Login(ctx); err != nil {
		return err
	}
	owner := &ownerSession{Session: api}
	ownerDID := api.Client.Auth.Did
	var claimed sync.Map
	var workers sync.WaitGroup
	for _, uri := range uris {
		workers.Go(func() { maintainPost(ctx, owner, view, cfg, ownerDID, uri, &claimed) })
	}
	workers.Wait()
	return nil
}

func maintainPost(ctx context.Context, owner *ownerSession, view *xrpc.Client, cfg Config, ownerDID string, uri syntax.ATURI, claimed *sync.Map) {
	var scan postScan
	var retryDelay time.Duration
	for ctx.Err() == nil {
		var err error
		if scan.post == "" {
			scan.post, err = resolvePost(ctx, owner.Session, uri)
			if err == nil {
				if _, already := claimed.LoadOrStore(scan.post, true); already {
					return
				}
				key := palustre.RecordKey("cordata\n" + scan.post)
				scan.list = "at://" + ownerDID + "/" + palustre.ListCollection + "/" + key
				slog.Info("maintaining list", "post", scan.post, "list", scan.list)
			}
		}
		if err == nil {
			err = owner.write(ctx, func() error { return owner.EnsurePostList(ctx, scan.list, scan.post) })
		}
		if err == nil {
			err = scan.sync(ctx, owner, view, cfg.Constellation, time.Now)
		}
		delay := cfg.RescanInterval
		if err != nil && ctx.Err() == nil {
			slog.Error("post sync failed", "post", uri, "error", err)
			retryDelay = palustre.ScanRetryDelay(retryDelay, cfg.RescanInterval)
			delay = retryDelay
		} else {
			retryDelay = 0
		}
		if !palustre.Pause(ctx, delay) {
			break
		}
	}
}
