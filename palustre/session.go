package palustre

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

type MiniDoc struct {
	DID string `json:"did"`
	PDS string `json:"pds"`
}

// session owns authentication and the write limiter shared by every list.
type Session struct {
	Config          Config
	HTTP            *http.Client
	Client          *xrpc.Client
	Refreshed       time.Time
	LastWrite       time.Time
	MembersAdded    atomic.Uint64
	MembersRemoved  atomic.Uint64
	ChecksCompleted atomic.Uint64
	CheckRetries    atomic.Uint64
}

func (api *Session) Resolve(ctx context.Context, identifier string) (MiniDoc, error) {
	endpoint := api.Config.Slingshot + "/xrpc/blue.microcosm.identity.resolveMiniDoc?identifier=" + url.QueryEscape(identifier)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return MiniDoc{}, err
	}
	response, err := api.HTTP.Do(request)
	if err != nil {
		return MiniDoc{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return MiniDoc{}, fmt.Errorf("Slingshot returned HTTP %d for %s", response.StatusCode, identifier)
	}
	var doc MiniDoc
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&doc); err != nil {
		return doc, err
	}
	if _, err := syntax.ParseDID(doc.DID); err != nil {
		return doc, err
	}
	if strings.HasPrefix(identifier, "did:") && doc.DID != identifier {
		return doc, fmt.Errorf("Slingshot returned a different DID for %s", identifier)
	}
	doc.PDS, err = NormalizePDS(doc.PDS)
	if err != nil {
		return doc, err
	}
	return doc, nil
}

func (api *Session) Login(ctx context.Context) error {
	host := api.Config.LoginPDS
	if host == "" {
		doc, err := api.Resolve(ctx, api.Config.Identifier)
		if err != nil {
			return err
		}
		host = doc.PDS
	}
	client := &xrpc.Client{Host: host, Client: api.HTTP}
	session, err := atproto.ServerCreateSession(ctx, client, &atproto.ServerCreateSession_Input{Identifier: api.Config.Identifier, Password: api.Config.Password})
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	if _, err := syntax.ParseDID(session.Did); err != nil {
		return err
	}
	if api.Client != nil && api.Client.Auth != nil && api.Client.Auth.Did != session.Did {
		return fmt.Errorf("login resolved to a different list owner")
	}
	client.Auth = &xrpc.AuthInfo{Did: session.Did, Handle: session.Handle, AccessJwt: session.AccessJwt, RefreshJwt: session.RefreshJwt}
	api.Client = client
	api.Refreshed = time.Now()
	return nil
}

func (api *Session) Refresh(ctx context.Context) error {
	if time.Since(api.Refreshed) < 30*time.Minute {
		return nil
	}
	client := *api.Client
	auth := *api.Client.Auth
	auth.AccessJwt = auth.RefreshJwt
	client.Auth = &auth
	session, err := atproto.ServerRefreshSession(ctx, &client)
	if err != nil {
		if XRPCCode(err, "ExpiredToken") || XRPCCode(err, "InvalidToken") {
			return api.Login(ctx)
		}
		return err
	}
	api.Client.Auth.AccessJwt = session.AccessJwt
	api.Client.Auth.RefreshJwt = session.RefreshJwt
	api.Refreshed = time.Now()
	return nil
}

func (api *Session) Put(ctx context.Context, collection, key string, record *lexutil.LexiconTypeDecoder) error {
	if err := api.WaitWrite(ctx); err != nil {
		return err
	}
	_, err := atproto.RepoPutRecord(ctx, api.Client, &atproto.RepoPutRecord_Input{Repo: api.Client.Auth.Did, Collection: collection, Rkey: key, Record: record})
	api.ObserveWriteError(err)
	return err
}

func (api *Session) ObserveWriteError(err error) {
	var failure *xrpc.Error
	if errors.As(err, &failure) && failure.StatusCode == http.StatusTooManyRequests {
		resume := time.Now().Add(time.Minute)
		if failure.Ratelimit != nil && failure.Ratelimit.Reset.After(resume) {
			resume = failure.Ratelimit.Reset
		}
		api.LastWrite = resume
	}
}

func (api *Session) WaitWrite(ctx context.Context) error {
	interval := api.Config.WriteInterval
	if interval == 0 {
		interval = DefaultWriteInterval
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !Pause(ctx, max(0, interval-time.Since(api.LastWrite))) {
		return ctx.Err()
	}
	api.LastWrite = time.Now()
	return nil
}
func XRPCCode(err error, code string) bool {
	var failure *xrpc.XRPCError
	return errors.As(err, &failure) && failure.ErrStr == code
}

const DefaultWriteInterval = 10 * time.Second

type Config struct {
	Identifier, Password, LoginPDS, Slingshot string
	WriteInterval                             time.Duration
}
