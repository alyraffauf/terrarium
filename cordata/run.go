package cordata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	health      *palustre.Health
	inventories ownerInventories
	readSlots   chan struct{}
	readHosts   palustre.HostLimiter
	initSlots   sync.Once
	*palustre.Session
	mu sync.Mutex
}

func (owner *ownerSession) write(ctx context.Context, apply func() error) error {
	resume := palustre.HealthWait(ctx)
	owner.mu.Lock()
	resume()
	defer owner.mu.Unlock()
	if err := owner.Refresh(ctx); err != nil {
		return err
	}
	return apply()
}

func (owner *ownerSession) reader(ctx context.Context) *xrpc.Client {
	resume := palustre.HealthWait(ctx)
	owner.mu.Lock()
	resume()
	defer owner.mu.Unlock()
	client := *owner.Client
	auth := *client.Auth
	client.Auth = &auth
	return &client
}

const likeCollection = "app.bsky.feed.like"
const repostCollection = "app.bsky.feed.repost"

type reactionKey struct{ did, collection, rkey string }

type verificationRetry struct {
	failures int
	due      time.Time
}

type postScan struct {
	post, list string
	retries    map[reactionKey]verificationRetry
}

func appViewActors(ctx context.Context, view *xrpc.Client, post, collection string) (map[string]bool, error) {
	actors := map[string]bool{}
	err := palustre.WalkPages(func(cursor string) (*string, error) {
		var next *string
		var profiles []*bsky.ActorDefs_ProfileView
		if collection == likeCollection {
			page, err := bsky.FeedGetLikes(ctx, view, "", cursor, 100, post)
			if err != nil {
				return nil, err
			}
			if page.Uri != post || page.Likes == nil {
				return nil, fmt.Errorf("invalid likes inventory for %s", post)
			}
			for _, like := range page.Likes {
				if like == nil {
					return nil, fmt.Errorf("missing like")
				}
				profiles = append(profiles, like.Actor)
			}
			next = page.Cursor
		} else {
			page, err := bsky.FeedGetRepostedBy(ctx, view, "", cursor, 100, post)
			if err != nil {
				return nil, err
			}
			if page.Uri != post || page.RepostedBy == nil {
				return nil, fmt.Errorf("invalid repost inventory for %s", post)
			}
			profiles, next = page.RepostedBy, page.Cursor
		}
		for _, actor := range profiles {
			if actor == nil {
				return nil, fmt.Errorf("missing actor for %s", collection)
			}
			if _, err := syntax.ParseDID(actor.Did); err != nil {
				return nil, err
			}
			actors[actor.Did] = true
		}
		return next, nil
	})
	return actors, err
}

func (scan *postScan) sync(ctx context.Context, api *ownerSession, view *xrpc.Client, constellation string, now func() time.Time) error {
	post, list := scan.post, scan.list
	partition := api.inventories.request(list)
	defer partition.close()
	members := map[string]bool{}
	var discoveryErr error
	collections := [...]string{likeCollection, repostCollection}
	for _, collection := range collections {
		actors, err := appViewActors(ctx, view, post, collection)
		if err == nil {
			maps.Copy(members, actors)
		} else {
			discoveryErr = errors.Join(discoveryErr, err)
		}
	}
	uncertain := map[string]bool{}
	for _, collection := range collections {
		deferred, err := scan.backlinks(ctx, api, constellation, collection, members, now)
		maps.Copy(uncertain, deferred)
		discoveryErr = errors.Join(discoveryErr, err)
	}
	items, err := partition.read(ctx, api.reader(ctx))
	if err != nil {
		return err
	}
	subjects := maps.Clone(members)
	for did := range items {
		if discoveryErr == nil && !uncertain[did] {
			subjects[did] = true
		}
	}
	var firstFailure error
	for _, did := range slices.Sorted(maps.Keys(subjects)) {
		palustre.HealthProgress(ctx)
		if err := api.write(ctx, func() error {
			return api.Reconcile(ctx, items, palustre.Membership{Target: post, ListURI: list, DID: did, Include: members[did]})
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

func (scan *postScan) backlinks(ctx context.Context, api *ownerSession, endpoint, collection string, members map[string]bool, now func() time.Time) (map[string]bool, error) {
	nextRetries := map[reactionKey]verificationRetry{}

	client := &xrpc.Client{Host: endpoint, Client: api.HTTP}
	pageErr := palustre.WalkPages(func(cursor string) (*string, error) {
		rows, next, err := palustre.BacklinksPage(ctx, client, scan.post, collection, "subject.uri", cursor)
		if err != nil {
			return nil, err
		}
		byActor := make(map[string][]*palustre.Backlink)
		seen := make(map[reactionKey]bool)
		for _, row := range rows {
			key := reactionKey{row.Did, collection, row.Rkey}
			if members[row.Did] || seen[key] {
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
			key     reactionKey
			matches bool
			err     error
		}
		results := make(chan result, len(rows))
		api.initSlots.Do(func() { api.readSlots = make(chan struct{}, 4) })
		var workers sync.WaitGroup
		for _, records := range byActor {
			resume := palustre.HealthWait(ctx)
			select {
			case api.readSlots <- struct{}{}:
				resume()
			case <-ctx.Done():
				resume()
				workers.Wait()
				return nil, ctx.Err()
			}
			workers.Go(func() {
				ctx, done := api.health.Worker(ctx)
				defer done()
				defer func() { <-api.readSlots }()
				for _, row := range records {
					ok, err := verifyReaction(ctx, api, row, scan.post)
					results <- result{reactionKey{row.Did, collection, row.Rkey}, ok, err}
					if ok || ctx.Err() != nil {
						break
					}
				}
			})
		}
		resume := palustre.HealthWait(ctx)
		workers.Wait()
		resume()
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
				slog.Warn("reaction verification deferred", "collection", collection, "post", scan.post, "did", key.did, "rkey", key.rkey, "retry_in", delay, "error", verified.err)
				continue
			}
			if retry.failures > 0 {
				api.VerificationRecovered.Add(1)
			}
			delete(scan.retries, key)
			if verified.matches {
				members[key.did] = true
			}
		}
		return next, nil
	})
	if pageErr != nil {
		// An interrupted walk can omit records still waiting for their retry.
		for key, retry := range scan.retries {
			if _, seen := nextRetries[key]; !seen && !members[key.did] {
				nextRetries[key] = retry
			}
		}
	}
	for key, retry := range scan.retries {
		if key.collection != collection {
			nextRetries[key] = retry
		}
	}
	scan.retries = nextRetries
	uncertain := make(map[string]bool, len(nextRetries))
	for key := range nextRetries {
		if key.collection == collection {
			uncertain[key.did] = true
		}
	}
	return uncertain, pageErr
}

func verifyReaction(ctx context.Context, api *ownerSession, row *palustre.Backlink, post string) (bool, error) {
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
		return false, fmt.Errorf("invalid reaction record for %s", row.Did)
	}
	switch v := record.Value.Val.(type) {
	case *bsky.FeedLike:
		if row.Collection == likeCollection && v.Subject != nil {
			return v.Subject.Uri == post, nil
		}
	case *bsky.FeedRepost:
		if row.Collection == repostCollection && v.Subject != nil {
			return v.Subject.Uri == post, nil
		}
	}
	return false, fmt.Errorf("invalid %s value for %s", row.Collection, row.Did)
}

type Config struct {
	palustre.Config
	AppView, Post, Constellation, MetricsAddress, HealthAddress string
	Posts                                                       []string
	RescanInterval                                              time.Duration
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
	health, err := palustre.StartHealth(cfg.HealthAddress)
	if err != nil {
		return fmt.Errorf("start health listener: %w", err)
	}
	defer health.Close()
	ctx = health.StartupContext(ctx)
	httpClient := health.HTTPClient()
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
	owner := &ownerSession{Session: api, health: health}
	ownerDID := api.Client.Auth.Did
	var claimed sync.Map
	var workers sync.WaitGroup
	for _, uri := range uris {
		workers.Go(func() { maintainPost(ctx, owner, view, cfg, ownerDID, uri, &claimed) })
	}
	health.Ready()
	workers.Wait()
	return nil
}

func maintainPost(ctx context.Context, owner *ownerSession, view *xrpc.Client, cfg Config, ownerDID string, uri syntax.ATURI, claimed *sync.Map) {
	ctx, done := owner.health.Worker(ctx)
	defer done()
	var scan postScan
	var retryDelay time.Duration
	for ctx.Err() == nil {
		palustre.HealthProgress(ctx)
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
