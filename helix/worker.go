package helix

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
)

// The caller selects at most eight jobs. Public reads overlap, while the caller
// retains sole ownership of authentication, exclusions, and list writes.
func processBatch(ctx context.Context, targets []*listTarget, jobs []job, db *store) error {
	actors := make([]string, 0, len(targets))
	for _, target := range targets {
		if !target.suspended.Load() {
			actors = append(actors, target.actor)
		}
	}
	if len(actors) == 0 {
		// Keep live candidates queued while the worker waits for a list to recover.
		palustre.Pause(ctx, time.Second)
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	type checked struct {
		work    job
		subject subjectState
		err     error
	}
	results := make(chan checked, len(jobs))
	api := targets[0].session
	for _, work := range jobs {
		workers.Add(1)
		go func() {
			defer workers.Done()
			subject, err := api.subject(ctx, db, work.DID, actors...)
			results <- checked{work, subject, err}
		}()
	}
	for range jobs {
		var result checked
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result = <-results:
		}
		err := result.err
		if err == nil {
			// A follow event may have arrived while a candidate's public reads ran.
			// Never apply a stale exclusion snapshot to prefetched work.
			if err := followers.RefreshSnapshot(ctx, db, api.Client, false); err != nil {
				slog.Error("follow refresh failed", "error", err)
				if !palustre.Pause(ctx, 10*time.Second) {
					return ctx.Err()
				}
				continue
			}
			followed, readErr := db.isFollowed(ctx, result.work.DID)
			if readErr != nil {
				return readErr
			}
			if result.subject.excluded && !followed && result.work.DID != api.Client.Auth.Did {
				// An unfollow invalidated the early exclusion check. Leave the job queued.
				continue
			}
			result.subject.excluded = followed || result.work.DID == api.Client.Auth.Did
			result.subject.beforeWrite = func(ctx context.Context) error { return followers.CheckSnapshot(ctx, db) }
			err = applySubject(ctx, targets, result.work, result.subject, db)
		}
		if errors.Is(err, followers.ErrSnapshotSuperseded) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("membership update failed", "did", result.work.DID, "error", err)
			if err := db.retry(ctx, result.work); err != nil {
				return err
			}
			api.CheckRetries.Add(1)
		} else {
			api.ChecksCompleted.Add(1)
		}
	}
	return nil
}

func logProgress(ctx context.Context, api *session, db *store) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			unique := db.completions.count(time.Now())
			pending, err := db.queries.PendingCount(ctx)
			var age int64
			if err == nil {
				age, err = db.queries.OldestReadyAgeSeconds(ctx)
			}
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("progress query failed", "error", err)
				}
				continue
			}
			slog.Info("membership progress", "pending", pending, "oldest_ready_age_seconds", age, "unique_completed_last_minute", unique, "completed", api.ChecksCompleted.Load(), "added", api.MembersAdded.Load(), "removed", api.MembersRemoved.Load(), "retries", api.CheckRetries.Load())
		}
	}
}
