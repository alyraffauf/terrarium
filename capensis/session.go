package capensis

import (
	"context"
	"net/http"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

type session struct {
	*palustre.Session
	cfg Config
}
type resolution struct {
	doc palustre.MiniDoc
	err error
}

func newSession(cfg Config, client *http.Client, owner *xrpc.Client) *session {
	return &session{cfg: cfg, Session: &palustre.Session{Config: palustre.Config{Identifier: cfg.Identifier, Password: cfg.Password, LoginPDS: cfg.LoginPDS, Slingshot: cfg.Slingshot}, HTTP: client, Client: owner}}
}

var xrpcCode = palustre.XRPCCode

func (api *session) subject(ctx context.Context, db *store, did string) (subjectState, error) {
	followed, err := db.isFollowed(ctx, did)
	if err != nil {
		return subjectState{}, err
	}
	subject := subjectState{did: did, excluded: followed || did == api.Client.Auth.Did}
	if !subject.excluded {
		subject.resolved.doc, subject.resolved.err = api.Resolve(ctx, did)
	}
	return subject, nil
}
