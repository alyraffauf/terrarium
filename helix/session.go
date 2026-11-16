package helix

import (
	"net/http"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

type session struct {
	*palustre.Session
	cfg Config
}

func newSession(cfg Config, client *http.Client, owner *xrpc.Client) *session {
	return &session{cfg: cfg, Session: &palustre.Session{Config: palustre.Config{Identifier: cfg.Identifier, Password: cfg.Password, LoginPDS: cfg.LoginPDS, Slingshot: cfg.Slingshot}, HTTP: client, Client: owner}}
}

var xrpcCode = palustre.XRPCCode
