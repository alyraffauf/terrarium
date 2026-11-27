package palustre

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alyraffauf/terrarium/palustre/microcosm"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

type MiniDoc struct {
	Handle string `json:"handle"`
	DID    string `json:"did"`
	PDS    string `json:"pds"`
}

// session owns authentication and the write limiter shared by every list.
type Session struct {
	Config              Config
	HTTP                *http.Client
	Client              *xrpc.Client
	Refreshed           time.Time
	LastWrite           time.Time
	MembersAdded        atomic.Uint64
	MembersRemoved      atomic.Uint64
	ChecksCompleted     atomic.Uint64
	CheckRetries        atomic.Uint64
	retryAuthentication authenticationRetry
}

func (api *Session) Resolve(ctx context.Context, identifier string) (MiniDoc, error) {
	client := &xrpc.Client{Host: api.Config.Slingshot, Client: inspectHTTPResponse(api.HTTP, func(response *http.Response) {
		if response.Body != nil {
			response.Body = http.MaxBytesReader(nil, response.Body, 1<<20)
		}
	})}
	response, err := microcosm.IdentityResolveMiniDoc(ctx, client, identifier)
	if err != nil {
		return MiniDoc{}, fmt.Errorf("Slingshot resolve %s: %w", identifier, err)
	}
	doc := MiniDoc{DID: response.Did, PDS: response.Pds}
	if h, err := syntax.ParseHandle(response.Handle); err == nil && h.Normalize().String() != "handle.invalid" {
		doc.Handle = h.Normalize().String()
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

type responseTransport struct {
	http.RoundTripper
	inspect func(*http.Response)
}

func (transport responseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.RoundTripper.RoundTrip(request)
	if err == nil && response != nil {
		transport.inspect(response)
	}
	return response, err
}

func inspectHTTPResponse(client *http.Client, inspect func(*http.Response)) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	wrapped := *client
	transport := wrapped.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	wrapped.Transport = responseTransport{RoundTripper: transport, inspect: inspect}
	return &wrapped
}

type authenticationRetry struct {
	after time.Time
	delay time.Duration
	err   error
	login bool
}

func (api *Session) Login(ctx context.Context) error {
	host := api.Config.LoginPDS
	if host == "" {
		doc, err := api.Resolve(ctx, api.Config.Identifier)
		if err != nil {
			return api.deferAuthentication(ctx, true, time.Time{}, err)
		}
		host = doc.PDS
	}
	var retryAfter time.Time
	client := &xrpc.Client{Host: host, Client: inspectHTTPResponse(api.HTTP, captureAuthRetryAfter(&retryAfter))}
	session, err := atproto.ServerCreateSession(ctx, client, &atproto.ServerCreateSession_Input{Identifier: api.Config.Identifier, Password: api.Config.Password})
	if err != nil {
		return api.deferAuthentication(ctx, true, retryAfter, fmt.Errorf("login: %w", err))
	}
	if err := api.validateSession(session.Did, session.Handle, session.AccessJwt, session.RefreshJwt); err != nil {
		return api.deferAuthentication(ctx, true, retryAfter, err)
	}
	client.Auth = &xrpc.AuthInfo{Did: session.Did, Handle: session.Handle, AccessJwt: session.AccessJwt, RefreshJwt: session.RefreshJwt}
	client.Client = api.HTTP
	api.Client = client
	api.markAuthenticated()
	return nil
}

func (api *Session) Refresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if time.Now().Before(api.retryAuthentication.after) {
		return api.retryAuthentication.err
	}
	if api.retryAuthentication.login {
		return api.Login(ctx)
	}
	if time.Since(api.Refreshed) < 30*time.Minute {
		return nil
	}
	if api.Client == nil || api.Client.Auth == nil {
		return fmt.Errorf("refresh requires an authenticated session")
	}
	client := *api.Client
	auth := *api.Client.Auth
	auth.AccessJwt = auth.RefreshJwt
	client.Auth = &auth
	var retryAfter time.Time
	client.Client = inspectHTTPResponse(client.Client, captureAuthRetryAfter(&retryAfter))
	session, err := atproto.ServerRefreshSession(ctx, &client)
	if err != nil {
		requiresLogin := XRPCCode(err, "ExpiredToken") || XRPCCode(err, "InvalidToken")
		if requiresLogin && !retryAfter.After(time.Now()) {
			return api.Login(ctx)
		}
		return api.deferAuthentication(ctx, requiresLogin, retryAfter, err)
	}
	if err := api.validateSession(session.Did, session.Handle, session.AccessJwt, session.RefreshJwt); err != nil {
		return api.deferAuthentication(ctx, false, retryAfter, err)
	}
	api.Client.Auth.AccessJwt = session.AccessJwt
	api.Client.Auth.RefreshJwt = session.RefreshJwt
	api.Client.Auth.Handle = session.Handle
	api.markAuthenticated()
	return nil
}

func (api *Session) validateSession(did, handle, accessToken, refreshToken string) error {
	if _, err := syntax.ParseDID(did); err != nil {
		return fmt.Errorf("invalid session DID: %w", err)
	}
	if api.Client != nil && api.Client.Auth != nil && api.Client.Auth.Did != did {
		return fmt.Errorf("authentication resolved to a different list owner")
	}
	if accessToken == "" || refreshToken == "" {
		return fmt.Errorf("authentication response is missing a session token")
	}
	if _, err := syntax.ParseHandle(handle); err != nil {
		return fmt.Errorf("invalid session handle: %w", err)
	}
	return nil
}

func (api *Session) markAuthenticated() {
	api.Refreshed = time.Now()
	api.retryAuthentication = authenticationRetry{}
}

func (api *Session) deferAuthentication(ctx context.Context, login bool, serverRetryAfter time.Time, err error) error {
	if ctx.Err() != nil {
		return err
	}
	delay := max(time.Minute, min(30*time.Minute, 2*api.retryAuthentication.delay))
	after := time.Now().Add(delay)
	if serverRetryAfter.After(after) {
		after = serverRetryAfter
	}
	api.retryAuthentication = authenticationRetry{after: after, delay: delay, err: err, login: login}
	return err
}

func captureAuthRetryAfter(retryAfter *time.Time) func(*http.Response) {
	return func(response *http.Response) {
		if response.StatusCode == http.StatusOK {
			return
		}
		header := response.Header.Get("Retry-After")
		if seconds, err := strconv.ParseInt(header, 10, 32); err == nil && seconds >= 0 {
			*retryAfter = time.Now().Add(time.Duration(seconds) * time.Second)
		} else if after, err := http.ParseTime(header); err == nil {
			*retryAfter = after
		}
		if response.StatusCode != http.StatusTooManyRequests && response.Header.Get("RateLimit-Remaining") != "0" {
			return
		}
		if epoch, err := strconv.ParseInt(response.Header.Get("RateLimit-Reset"), 10, 64); err == nil {
			if reset := time.Unix(epoch, 0); reset.After(*retryAfter) {
				*retryAfter = reset
			}
		}
	}
}

func (api *Session) createRecord(ctx context.Context, collection, key string, record *lexutil.LexiconTypeDecoder) error {
	_, err := atproto.RepoCreateRecord(ctx, api.Client, &atproto.RepoCreateRecord_Input{Repo: api.Client.Auth.Did, Collection: collection, Rkey: &key, Record: record})
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
const DefaultFollowerWriteInterval = 6 * time.Second

type Config struct {
	Identifier, Password, LoginPDS, Slingshot string
	WriteInterval                             time.Duration
}
