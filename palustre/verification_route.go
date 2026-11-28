package palustre

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util/ssrf"
	"github.com/bluesky-social/indigo/xrpc"
)

var publicDialer = ssrf.PublicOnlyDialer()
var documentResolver = identity.BaseDirectory{PLCURL: identity.DefaultPLCURL, HTTPClient: http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: publicDialer.DialContext, IdleConnTimeout: 90 * time.Second}}}

func verificationRoute(ctx context.Context, did string, lookupErr error, resolver *identity.BaseDirectory) (MiniDoc, error) {
	var detail *xrpc.XRPCError
	if !errors.As(lookupErr, &detail) || !strings.Contains(strings.ToLower(detail.Message), "handle") {
		return MiniDoc{}, lookupErr
	}
	id, err := syntax.ParseDID(did)
	if err != nil {
		return MiniDoc{}, err
	}
	doc, err := resolver.ResolveDID(ctx, id)
	if err != nil {
		return MiniDoc{}, errors.Join(lookupErr, err)
	}
	if doc.DID != id {
		return MiniDoc{}, fmt.Errorf("unexpected DID document for %s", did)
	}
	for _, svc := range doc.Service {
		if (svc.ID == "#atproto_pds" || svc.ID == did+"#atproto_pds") && svc.Type == "AtprotoPersonalDataServer" && strings.HasPrefix(svc.ServiceEndpoint, "https://") {
			host, err := NormalizePDS(svc.ServiceEndpoint)
			if err != nil {
				return MiniDoc{}, err
			}
			return MiniDoc{DID: did, PDS: host, FromDIDDocument: true}, nil
		}
	}
	return MiniDoc{}, fmt.Errorf("no valid PDS for %s", did)
}

// ObserveRateLimits forwards server cooldowns to the verifier for this PDS.
func ObserveRateLimits(client *http.Client, observed func(time.Time)) *http.Client {
	return inspectHTTPResponse(client, func(r *http.Response) {
		if r.StatusCode != http.StatusTooManyRequests {
			return
		}
		var deadline time.Time
		captureAuthRetryAfter(&deadline)(r)
		if !deadline.After(time.Now()) {
			deadline = time.Now().Add(time.Minute)
		}
		observed(deadline)
	})
}
