package helix

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/palustre"
)

const listCollection = palustre.ListCollection
const itemCollection = palustre.ItemCollection

type listTarget struct {
	suspended atomic.Bool
	*session
	actor          string
	rescanInterval time.Duration
	listURI        string
	name           string
	avatar         string
	db             *store
}

var recordKey = palustre.RecordKey

func (api *listTarget) ensureList(ctx context.Context) error {
	client := &xrpc.Client{Host: api.cfg.AppView, Client: api.HTTP}
	profile, err := bsky.ActorGetProfile(ctx, client, api.actor)
	if err != nil {
		if palustre.XRPCCode(err, "AccountTakedown") || palustre.XRPCCode(err, "AccountDeactivated") {
			if !api.suspended.Swap(true) {
				slog.Warn("target unavailable; pausing list updates", "account", api.actor, "error", err)
			}
			return nil
		}
		return err
	}
	if profile.Did != api.actor {
		return fmt.Errorf("profile returned a different target DID")
	}
	handle, err := syntax.ParseHandle(profile.Handle)
	if err != nil || handle.String() == "handle.invalid" {
		return fmt.Errorf("target has no valid handle")
	}
	name := listName(handle.String())
	legacy := name
	if len(legacy) > 64 {
		legacy = legacy[:61] + "..."
	}
	key := "list-name:" + api.listURI
	previous, err := api.db.queries.GetBinding(ctx, key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := api.EnsureList(ctx, api.listURI, name, previous, legacy, listName(api.actor)); err != nil {
		return err
	}
	api.name = palustre.ListTitle(api.listURI)
	if err := api.db.queries.SaveListName(ctx, storage.SaveListNameParams{Key: key, Value: name}); err != nil {
		return err
	}
	if _, err := api.db.bindList(ctx, Target{Actor: api.actor, ListURI: api.listURI}, api.Client.Auth.Did); err != nil {
		return err
	}
	api.avatar = ""
	if profile.Avatar != nil {
		api.avatar = *profile.Avatar
	}
	api.suspended.Store(false)
	return nil
}

func (api *listTarget) refreshListIcon(ctx context.Context) {
	if api.avatar == "" {
		return
	}
	iconCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	icon, err := api.FetchIcon(iconCtx, api.avatar)
	if err == nil {
		err = api.UpdateListIcon(iconCtx, api.listURI, api.name, icon)
	}
	if err != nil {
		slog.Warn("list icon refresh failed", "account", api.actor, "error", err)
	}
}

type subjectState struct {
	did         string
	excluded    bool
	beforeWrite func(context.Context) error
	follows     map[string]bool
}

func (api *listTarget) reconcile(ctx context.Context, work job, db *store) error {
	subject, err := api.subject(ctx, db, work.DID, api.actor)
	if err != nil {
		return err
	}
	return api.reconcileSubject(ctx, subject, db)
}
func (api *listTarget) reconcileSubject(ctx context.Context, subject subjectState, db *store) error {
	if api.suspended.Load() {
		return nil
	}
	belongs := !subject.excluded && subject.follows[api.actor]

	return api.Reconcile(ctx, db, palustre.Membership{Target: api.actor, ListURI: api.listURI, DID: subject.did, Include: belongs, BeforeWrite: subject.beforeWrite})
}
