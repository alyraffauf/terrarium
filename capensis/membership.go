package capensis

import (
	"context"
	"errors"
	"io/fs"

	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/palustre"
)

const listCollection = palustre.ListCollection
const itemCollection = palustre.ItemCollection

type listTarget struct {
	checkRetry time.Duration
	*session
	pds            string
	rescanInterval time.Duration
	target         *xrpc.Client
	listURI        string
	paused         atomic.Bool
}

var recordKey = palustre.RecordKey

func (api *listTarget) ensureList(ctx context.Context) error {
	return api.EnsureList(ctx, api.listURI, listName(api.pds))
}

func (api *listTarget) checkList(ctx context.Context, db *store) error {
	api.paused.Store(true)
	if err := api.ensureList(ctx); err != nil {
		return err
	}
	if _, err := db.bindList(ctx, Target{PDS: api.pds, ListURI: api.listURI}, api.Client.Auth.Did); err != nil {
		return err
	}
	api.paused.Store(false)
	return nil
}

func (api *listTarget) refreshListIcon(ctx context.Context) {
	iconCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	icon, err := api.Favicon(iconCtx, api.pds)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
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
	active, err := palustre.RepoActive(ctx, api.target, did)
	if xrpcCode(err, "RepoNotFound") || xrpcCode(err, "RepoDeactivated") || xrpcCode(err, "RepoTakendown") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !active {
		return false, nil
	}
	if resolveErr != nil {
		return false, resolveErr
	}
	return true, nil
}

type subjectState struct {
	did         string
	excluded    bool
	beforeWrite func(context.Context) error
	resolved    resolution
}

func (api *listTarget) reconcileSubject(ctx context.Context, subject subjectState, db *store) error {
	if api.paused.Load() {
		return nil
	}
	var err error
	belongs := false
	if !subject.excluded {
		belongs, err = api.belongs(ctx, subject.did, subject.resolved)
		if err != nil {
			return err
		}
	}

	return api.Reconcile(ctx, db, palustre.Membership{Target: api.pds, ListURI: api.listURI, DID: subject.did, Include: belongs, BeforeWrite: subject.beforeWrite})
}
