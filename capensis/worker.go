package capensis

import (
	"context"
	"errors"
	"fmt"

	"github.com/alyraffauf/terrarium/capensis/storage"
	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
)

type membershipCheck struct {
	target  *listTarget
	include bool
	err     error
}

type checkedAccount struct {
	work        job
	followed    bool
	memberships []membershipCheck
	err         error
}

// Workers only use public clients. The owner DID is captured before starting
// them because the writer may replace its authenticated client during login.
func checkAccount(ctx context.Context, targets []*listTarget, owner string, work job, db *store) checkedAccount {
	followed, err := db.isFollowed(ctx, work.DID)
	result := checkedAccount{work: work, followed: followed, err: err}
	if err != nil {
		return result
	}
	var resolved resolution
	if !followed && work.DID != owner {
		resolved.doc, resolved.err = targets[0].Resolve(ctx, work.DID)
	}
	for _, target := range targets {
		if target.paused.Load() {
			continue
		}
		check := membershipCheck{target: target}
		if !followed && work.DID != owner {
			check.include, check.err = target.belongs(ctx, work.DID, resolved)
		}
		result.memberships = append(result.memberships, check)
	}
	return result
}

func (result checkedAccount) apply(ctx context.Context, db *store) error {
	if result.err != nil {
		return result.err
	}
	did := result.work.DID
	current := storage.JobStillCurrentParams{Did: did, Generation: result.work.Generation}
	if result.followed {
		current.Followed = 1
	}
	beforeWrite := func(ctx context.Context) error {
		ok, err := db.queries.JobStillCurrent(ctx, current)
		if err != nil {
			return err
		}
		if !ok {
			return followers.ErrSnapshotSuperseded
		}
		return nil
	}
	if err := beforeWrite(ctx); err != nil {
		return err
	}
	var failures []error
	for _, check := range result.memberships {
		target := check.target
		if target.paused.Load() {
			continue
		}
		err := check.err
		if err == nil {
			err = target.Reconcile(ctx, db, palustre.Membership{Target: target.pds, ListURI: target.listURI, DID: did, Include: check.include, BeforeWrite: beforeWrite})
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", target.pds, err))
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return db.finish(ctx, result.work)
}
