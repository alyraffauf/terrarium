package helix

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/alyraffauf/terrarium/helix/storage"
	"github.com/alyraffauf/terrarium/palustre"
)

const listCollection = palustre.ListCollection
const itemCollection = palustre.ItemCollection

type listTarget struct {
	*session
	actor          string
	rescanInterval time.Duration
	listURI        string
	name           string
	db             *store
}

var recordKey = palustre.RecordKey

func (api *listTarget) ensureList(ctx context.Context) error {
	var profile struct {
		DID    string `json:"did"`
		Handle string `json:"handle"`
	}
	client := &xrpc.Client{Host: api.cfg.AppView, Client: api.HTTP}
	if err := client.LexDo(ctx, lexutil.Query, "", "app.bsky.actor.getProfile", map[string]any{"actor": api.actor}, nil, &profile); err != nil {
		return err
	}
	if profile.DID != api.actor {
		return fmt.Errorf("profile returned a different target DID")
	}
	handle, err := syntax.ParseHandle(profile.Handle)
	if err != nil || handle.String() == "handle.invalid" {
		return fmt.Errorf("target has no valid handle")
	}
	name := listName(handle.String())
	if len(name) > 64 {
		name = name[:61] + "..."
	}
	key := "list-name:" + api.listURI
	previous, err := api.db.queries.GetBinding(ctx, key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := api.EnsureList(ctx, api.listURI, name, previous, listName(api.actor)); err != nil {
		return err
	}
	api.name = name
	return api.db.queries.SaveListName(ctx, storage.SaveListNameParams{Key: key, Value: name})
}

func (api *listTarget) refreshListIcon(ctx context.Context) {
	iconCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var profile struct {
		Avatar *string `json:"avatar"`
	}
	client := &xrpc.Client{Host: api.cfg.AppView, Client: api.HTTP}
	err := client.LexDo(iconCtx, lexutil.Query, "", "app.bsky.actor.getProfile", map[string]any{"actor": api.actor}, nil, &profile)
	if err == nil && profile.Avatar != nil && *profile.Avatar != "" {
		var icon []byte
		icon, err = api.FetchIcon(iconCtx, *profile.Avatar)
		if err == nil {
			err = api.UpdateListIcon(iconCtx, api.listURI, api.name, icon)
		}
	}
	if err != nil {
		slog.Warn("list icon refresh failed", "account", api.actor, "error", err)
	}
}

type subjectState struct {
	did      string
	excluded bool
	follows  map[string]bool
}

func (api *listTarget) reconcile(ctx context.Context, work job, db *store) error {
	subject, err := api.subject(ctx, db, work.DID, api.actor)
	if err != nil {
		return err
	}
	return api.reconcileSubject(ctx, subject, db)
}
func (api *listTarget) reconcileSubject(ctx context.Context, subject subjectState, db *store) error {
	belongs := !subject.excluded && subject.follows[api.actor]

	return api.Reconcile(ctx, db, palustre.Membership{Target: api.actor, ListURI: api.listURI, DID: subject.did, Include: belongs})
}
