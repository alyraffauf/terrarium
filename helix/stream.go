package helix

import (
	"context"

	"github.com/alyraffauf/terrarium/palustre"
)

func (db *store) selectCandidate(watched map[string]bool) func(context.Context, palustre.Event) (bool, error) {
	return func(ctx context.Context, event palustre.Event) (bool, error) {
		switch event.Kind {
		case "sync":
			return true, nil
		case "identity", "account":
			return db.knownCandidate(ctx, event.Payload.Did)
		case "commit":
			if event.Payload.Collection != palustre.FollowCollection {
				return false, nil
			}
			if event.MalformedFollow || watched[event.FollowSubject] {
				return true, nil
			}
			// A new follow to an unrelated target cannot change membership.
			// Updates and deletes can remove a previously qualifying follow.
			if event.Payload.Operation == "create" {
				return false, nil
			}
			return db.knownCandidate(ctx, event.Payload.Did)
		default:
			return false, nil
		}
	}
}
