package capensis

import (
	"context"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

const listCollection = palustre.ListCollection
const itemCollection = palustre.ItemCollection

type listTarget struct {
	*session
	pds            string
	rescanInterval time.Duration
	target         *xrpc.Client
	listURI        string
}

var recordKey = palustre.RecordKey

func (api *listTarget) ensureList(ctx context.Context) error {
	return api.EnsureList(ctx, api.listURI, listName(api.pds))
}

func (api *listTarget) refreshListIcon(ctx context.Context) {
	iconCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	icon, err := api.Favicon(iconCtx, api.pds)
	if err == nil {
		err = api.UpdateListIcon(iconCtx, api.listURI, palustre.ListTitle(api.listURI), icon)
	}
	if err != nil {
		slog.Warn("list icon refresh failed", "pds", api.pds, "error", err)
	}
}

func (api *listTarget) belongs(ctx context.Context, did string, resolved resolution) (bool, error) {
	doc, resolveErr := resolved.doc, resolved.err
	if resolveErr == nil && doc.PDS != api.pds {
		return false, nil
	}
	// A former member's PDS can confirm deletion when its DID no longer resolves.
	status, err := atproto.SyncGetRepoStatus(ctx, api.target, did)
	if xrpcCode(err, "RepoNotFound") || xrpcCode(err, "RepoDeactivated") || xrpcCode(err, "RepoTakendown") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !status.Active {
		return false, nil
	}
	if resolveErr != nil {
		return false, resolveErr
	}
	return true, nil
}

type subjectState struct {
	did      string
	excluded bool
	resolved resolution
}

func (api *listTarget) reconcile(ctx context.Context, work job, db *store) error {
	subject, err := api.subject(ctx, db, work.DID)
	if err != nil {
		return err
	}
	return api.reconcileSubject(ctx, subject, db)
}
func (api *listTarget) reconcileSubject(ctx context.Context, subject subjectState, db *store) error {
	var err error
	belongs := false
	if !subject.excluded {
		belongs, err = api.belongs(ctx, subject.did, subject.resolved)
		if err != nil {
			return err
		}
	}

	return api.Reconcile(ctx, db, palustre.Membership{Target: api.pds, ListURI: api.listURI, DID: subject.did, Include: belongs})
}
