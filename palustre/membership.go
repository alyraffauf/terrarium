package palustre

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/alyraffauf/terrarium/palustre/listlex"
)

const ListCollection = "app.bsky.graph.list"
const ItemCollection = "app.bsky.graph.listitem"

// Stable, valid TID keys make writes safe to replay after ambiguous HTTP failures.
func RecordKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return syntax.NewTIDFromInteger(binary.BigEndian.Uint64(digest[:8]) & ((1 << 63) - 1)).String()
}

func (api *Session) EnsureList(ctx context.Context, listURI, name string, previousNames ...string) error {
	return api.ensureList(ctx, listURI, name, nil, previousNames)
}

func (api *Session) EnsurePostList(ctx context.Context, listURI, name, post string) error {
	return api.ensureList(ctx, listURI, name, &post, nil)
}

func (api *Session) ensureList(ctx context.Context, listURI, name string, description *string, previousNames []string) error {
	uri, err := syntax.ParseATURI(listURI)
	if err != nil {
		return err
	}
	key := uri.RecordKey().String()
	existing, err := atproto.RepoGetRecord(ctx, api.Client, "", ListCollection, api.Client.Auth.Did, key)
	if err == nil {
		if existing.Value == nil {
			return fmt.Errorf("managed list has no record value")
		}
		list, ok := existing.Value.Val.(*listlex.GraphList)
		if !ok {
			return fmt.Errorf("unexpected record at managed list URI")
		}
		if list.Purpose != nil && *list.Purpose == "app.bsky.graph.defs#modlist" {
			sameDescription := description == nil || (list.Description != nil && *list.Description == *description)
			if list.Name == name && sameDescription {
				return nil
			}
			if description != nil && sameDescription {
				previousNames = append(previousNames, list.Name)
			}
			for _, previous := range previousNames {
				if list.Name != previous {
					continue
				}
				if existing.Cid == nil || *existing.Cid == "" {
					return fmt.Errorf("managed list has no CID")
				}
				list.Name = name
				if err := api.WaitWrite(ctx); err != nil {
					return err
				}
				_, err := atproto.RepoPutRecord(ctx, api.Client, &atproto.RepoPutRecord_Input{Repo: api.Client.Auth.Did, Collection: ListCollection, Rkey: key, Record: &lexutil.LexiconTypeDecoder{Val: list}, SwapRecord: existing.Cid})
				api.ObserveWriteError(err)
				return err
			}
		}
		return fmt.Errorf("refusing to adopt list %s: expected name %q and moderation purpose", listURI, name)
	}
	if !XRPCCode(err, "RecordNotFound") {
		return err
	}
	purpose := "app.bsky.graph.defs#modlist"
	list := &listlex.GraphList{LexiconTypeID: ListCollection, Name: name, Description: description, Purpose: &purpose, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	return api.Put(ctx, ListCollection, key, &lexutil.LexiconTypeDecoder{Val: list})
}

type Membership struct {
	Target, ListURI, DID string
	Include              bool
}

// MembershipStore persists write intent before network mutations and forgets only confirmed removals.
type MembershipStore interface {
	MemberKeys(context.Context, string, string) ([]string, error)
	RememberMember(context.Context, string, string, string) error
	ForgetMember(context.Context, string, string) error
}

func (api *Session) Reconcile(ctx context.Context, db MembershipStore, member Membership) error {
	keys, err := db.MemberKeys(ctx, member.Target, member.DID)
	if err != nil {
		return err
	}
	kept := false
	for _, key := range keys {
		record, err := atproto.RepoGetRecord(ctx, api.Client, "", ItemCollection, api.Client.Auth.Did, key)
		if err != nil && !XRPCCode(err, "RecordNotFound") {
			return err
		}
		matches := false
		if err == nil && record.Value != nil {
			item, ok := record.Value.Val.(*listlex.GraphListitem)
			matches = ok && item.List == member.ListURI && item.Subject == member.DID
		}
		if matches && member.Include && !kept {
			kept = true
			continue
		}
		if matches {
			if record.Cid == nil || *record.Cid == "" {
				return fmt.Errorf("list item has no CID")
			}
			if err := api.WaitWrite(ctx); err != nil {
				return err
			}
			_, err = atproto.RepoDeleteRecord(ctx, api.Client, &atproto.RepoDeleteRecord_Input{Repo: api.Client.Auth.Did, Collection: ItemCollection, Rkey: key, SwapRecord: record.Cid})
			api.ObserveWriteError(err)
			if err != nil && !XRPCCode(err, "RecordNotFound") {
				return err
			}
			if err == nil {
				api.MembersRemoved.Add(1)
				reason := "not_eligible"
				if member.Include {
					reason = "duplicate"
				}
				slog.InfoContext(ctx, "list item removed", "did", member.DID, "target", member.Target, "list", member.ListURI, "rkey", key, "reason", reason)
			}
		}
		if err := db.ForgetMember(ctx, member.Target, key); err != nil {
			return err
		}
	}
	if member.Include && !kept {
		key := RecordKey(member.ListURI + "\n" + member.DID)
		existing, err := atproto.RepoGetRecord(ctx, api.Client, "", ItemCollection, api.Client.Auth.Did, key)
		if err == nil {
			if existing.Value != nil {
				if item, ok := existing.Value.Val.(*listlex.GraphListitem); ok && item.List == member.ListURI && item.Subject == member.DID {
					return db.RememberMember(ctx, member.Target, member.DID, key)
				}
			}
			return fmt.Errorf("list item key %s is occupied by another record", key)
		}
		if !XRPCCode(err, "RecordNotFound") {
			return err
		}
		if err := db.RememberMember(ctx, member.Target, member.DID, key); err != nil {
			return err
		}
		item := &listlex.GraphListitem{LexiconTypeID: ItemCollection, Subject: member.DID, List: member.ListURI, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := api.Put(ctx, ItemCollection, key, &lexutil.LexiconTypeDecoder{Val: item}); err != nil {
			return err
		}
		api.MembersAdded.Add(1)
		slog.InfoContext(ctx, "list item added", "did", member.DID, "target", member.Target, "list", member.ListURI, "rkey", key)
	}
	return nil
}
