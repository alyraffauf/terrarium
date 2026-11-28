package helix

import (
	"net/http"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

type session struct {
	*palustre.Session
	readHosts palustre.HostLimiter
	ownerDID  string
	cfg       Config
}

func newSession(cfg Config, client *http.Client, owner *xrpc.Client) *session {
	var did string
	if owner != nil && owner.Auth != nil {
		did = owner.Auth.Did
	}
	return &session{ownerDID: did, cfg: cfg, Session: &palustre.Session{Config: palustre.Config{Identifier: cfg.Identifier, Password: cfg.Password, LoginPDS: cfg.LoginPDS, Slingshot: cfg.Slingshot, WriteInterval: cfg.WriteInterval}, HTTP: client, Client: owner}}
}

var xrpcCode = palustre.XRPCCode
