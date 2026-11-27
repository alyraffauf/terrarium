package palustre

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/rivo/uniseg"
)

const ListCollection = "app.bsky.graph.list"
const ItemCollection = "app.bsky.graph.listitem"
const moderationListPurpose = "app.bsky.graph.defs#modlist"

// ErrUnrelatedList reports a list that does not match the configured identity.
var ErrUnrelatedList = errors.New("unrelated list")

// ErrListUpdate reports a metadata update failure after checking list ownership.
var ErrListUpdate = errors.New("list metadata update failed")

// Stable, valid TID keys make writes safe to replay after ambiguous HTTP failures.
func RecordKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return syntax.NewTIDFromInteger(binary.BigEndian.Uint64(digest[:8]) & ((1 << 63) - 1)).String()
}

// ListTitle derives a stable neutral name from the permanent list URI.
func ListTitle(listURI string) string {
	adjectives := [...]string{
		"Amber", "Bright", "Clear", "Dewy", "Golden", "Green", "Mellow", "Misty",
		"Dusky", "Quiet", "Silver", "Soft", "Still", "Sunny", "Warm", "Snowy",
		"Azure", "Breezy", "Calm", "Cloudy", "Cool", "Copper", "Crisp", "Dappled",
		"Pale", "Fresh", "Frosty", "Gentle", "Lunar", "Rosy", "Shady", "Wavy",
	}
	nouns := [...]string{
		"Birch", "Brook", "Birchwood", "Clover", "Fern", "Garden", "Grove", "Lake",
		"Maple", "Meadow", "Moss", "Pebble", "Pond", "Reed", "Spruce", "Willow",
		"Aspen", "Bay", "Cedar", "Bloom", "Cypress", "Dune", "Elm", "Field",
		"Glade", "Hill", "Lagoon", "Laurel", "Linden", "Oak", "Pine", "River",
	}
	hash := sha256.Sum256([]byte(listURI))
	return adjectives[int(hash[0])%len(adjectives)] + " " + nouns[int(hash[1])%len(nouns)]
}

func (api *Session) EnsureList(ctx context.Context, listURI, description string, previousNames ...string) error {
	return api.ensureList(ctx, listURI, description, listIdentity{previousNames: previousNames})
}

func (api *Session) EnsurePostList(ctx context.Context, listURI, post string) error {
	return api.ensureList(ctx, listURI, post, listIdentity{post: true})
}

// EnsureClusterList also recognizes old managed descriptions when the local history is unavailable.
func (api *Session) EnsureClusterList(ctx context.Context, listURI, description string, isClusterDescription func(string) bool, previousNames ...string) error {
	return api.ensureList(ctx, listURI, description, listIdentity{previousNames: previousNames, isPreviousDescription: isClusterDescription})
}

type listIdentity struct {
	post                  bool
	previousNames         []string
	isPreviousDescription func(string) bool
}

func (api *Session) ensureList(ctx context.Context, listURI, description string, identity listIdentity) error {
	key, err := api.listRecordKey(listURI)
	if err != nil {
		return err
	}
	title := ListTitle(listURI)
	owner := api.Client.Auth.Did
	existing, err := atproto.RepoGetRecord(ctx, api.Client, "", ListCollection, owner, key)
	purpose := moderationListPurpose
	if XRPCCode(err, "RecordNotFound") {
		if err := validateListDescription(description); err != nil {
			return err
		}
		list := &bsky.GraphList{LexiconTypeID: ListCollection, Name: title, Description: &description, Purpose: &purpose, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := api.WaitWrite(ctx); err != nil {
			return err
		}
		createErr := api.createRecord(ctx, ListCollection, key, &lexutil.LexiconTypeDecoder{Val: list})
		if createErr == nil || ctx.Err() != nil {
			return createErr
		}
		existing, err = atproto.RepoGetRecord(ctx, api.Client, "", ListCollection, owner, key)
		if err != nil {
			return createErr
		}
	}
	if err != nil {
		return err
	}
	if existing.Value == nil {
		return fmt.Errorf("managed list has no record value")
	}
	list, ok := existing.Value.Val.(*bsky.GraphList)
	if !ok || list.Purpose == nil || *list.Purpose != purpose {
		return fmt.Errorf("%w: expected moderation list at %s", ErrUnrelatedList, listURI)
	}
	text := ""
	if list.Description != nil {
		text = *list.Description
	}
	lines := strings.Split(text, "\n")
	last := lines[len(lines)-1]
	legacy := list.Name == description || slices.Contains(identity.previousNames, list.Name)
	if identity.post {
		legacy = text == description
	}
	if list.Name == title && (last == description || (identity.post && lines[0] == description)) {
		return nil
	}
	previousDescription := slices.Contains(identity.previousNames, last)
	if list.Name == title && identity.isPreviousDescription != nil && identity.isPreviousDescription(last) {
		previousDescription = true
	}
	if !legacy && !(list.Name == title && previousDescription && last != "") {
		return fmt.Errorf("%w: refusing to adopt %s", ErrUnrelatedList, listURI)
	}
	// Keep existing prose and its facets at the same byte offsets.
	if last == description {
		if identity.post && list.Name != title && list.Name != description {
			lines = append(lines, list.Name)
		}
	} else if last == "" || previousDescription {
		lastStart := int64(len(text) - len(last))
		list.DescriptionFacets = slices.DeleteFunc(list.DescriptionFacets, func(facet *bsky.RichtextFacet) bool {
			return facet == nil || facet.Index == nil || facet.Index.ByteEnd > lastStart
		})
		lines[len(lines)-1] = description
	} else {
		lines = append(lines, description)
	}
	text = strings.Join(lines, "\n")
	if err := validateListDescription(text); err != nil {
		return fmt.Errorf("%w for %s: %w", ErrListUpdate, listURI, err)
	}
	if existing.Cid == nil || *existing.Cid == "" {
		return fmt.Errorf("managed list has no CID")
	}
	list.Name = title
	list.Description = &text
	if err := api.WaitWrite(ctx); err != nil {
		return err
	}
	_, err = atproto.RepoPutRecord(ctx, api.Client, &atproto.RepoPutRecord_Input{Repo: owner, Collection: ListCollection, Rkey: key, Record: &lexutil.LexiconTypeDecoder{Val: list}, SwapRecord: existing.Cid})
	api.ObserveWriteError(err)
	if err != nil && ctx.Err() == nil && !XRPCCode(err, "InvalidSwap") && !XRPCCode(err, "RecordNotFound") && !XRPCCode(err, "RepoNotFound") {
		return fmt.Errorf("%w for %s: %w", ErrListUpdate, listURI, err)
	}
	return err
}

func (api *Session) listRecordKey(listURI string) (string, error) {
	uri, err := syntax.ParseATURI(listURI)
	if err != nil {
		return "", err
	}
	key := uri.RecordKey().String()
	if uri.Authority().String() != api.Client.Auth.Did || uri.Collection().String() != ListCollection || key == "" {
		return "", fmt.Errorf("%w: %s is not a list URI in the authenticated repository", ErrUnrelatedList, listURI)
	}
	return key, nil
}

func validateListDescription(description string) error {
	if len(description) > 3000 || uniseg.GraphemeClusterCount(description) > 300 {
		return fmt.Errorf("list description exceeds 3000 bytes or 300 graphemes")
	}
	return nil
}

type Membership struct {
	Target, ListURI, DID string
	Include              bool
	// BeforeWrite rechecks application state after pacing and before the mutation.
	BeforeWrite func(context.Context) error
}

func (member Membership) waitWrite(ctx context.Context, api *Session) error {
	if err := api.WaitWrite(ctx); err != nil {
		return err
	}
	if member.BeforeWrite != nil {
		return member.BeforeWrite(ctx)
	}
	return nil
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
			item, ok := record.Value.Val.(*bsky.GraphListitem)
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
			if err := member.waitWrite(ctx, api); err != nil {
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
		if XRPCCode(err, "RecordNotFound") {
			if err := db.RememberMember(ctx, member.Target, member.DID, key); err != nil {
				return err
			}
			if err := member.waitWrite(ctx, api); err != nil {
				return err
			}
			item := &bsky.GraphListitem{LexiconTypeID: ItemCollection, Subject: member.DID, List: member.ListURI, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			createErr := api.createRecord(ctx, ItemCollection, key, &lexutil.LexiconTypeDecoder{Val: item})
			if createErr == nil {
				api.MembersAdded.Add(1)
				slog.InfoContext(ctx, "list item added", "did", member.DID, "target", member.Target, "list", member.ListURI, "rkey", key)
				return nil
			}
			if ctx.Err() != nil {
				return createErr
			}
			existing, err = atproto.RepoGetRecord(ctx, api.Client, "", ItemCollection, api.Client.Auth.Did, key)
			if err != nil {
				return createErr
			}
		}
		if err != nil {
			return err
		}
		if existing.Value != nil {
			item, ok := existing.Value.Val.(*bsky.GraphListitem)
			if ok && item.List == member.ListURI && item.Subject == member.DID {
				return db.RememberMember(ctx, member.Target, member.DID, key)
			}
		}
		return fmt.Errorf("list item key %s is occupied by another record", key)
	}
	return nil
}
