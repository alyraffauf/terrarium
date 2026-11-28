package helix

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"

	"time"

	"github.com/alyraffauf/terrarium/helix/storage"

	"github.com/alyraffauf/terrarium/internal/followers"
	"github.com/alyraffauf/terrarium/palustre"
)

const readConcurrency = 8

type checked struct {
	work    job
	actors  []string
	subject subjectState
	err     error
}

type readQueue struct {
	*followers.ReadPool[checked]
	sinceOldest int
}

func activeActors(targets []*listTarget) []string {
	var actors []string
	for _, target := range targets {
		if !target.suspended.Load() {
			actors = append(actors, target.actor)
		}
	}
	return actors
}

func (q *readQueue) fill(ctx context.Context, targets []*listTarget, db *store) error {
	actors := activeActors(targets)
	free := q.Free()
	if len(actors) == 0 || free == 0 {
		return nil
	}
	busy, err := json.Marshal(q.BusyDIDs())
	if err != nil {
		return err
	}
	var oldest int64
	if q.sinceOldest+free >= readConcurrency {
		oldest = 1
	}
	rows, err := db.queries.NextJobs(ctx, storage.NextJobsParams{Busy: string(busy), Now: time.Now().Unix(), PrioritySlots: int64(free) - oldest, OldestSlot: oldest})
	if err != nil {
		return err
	}
	for _, row := range rows {
		work := job{DID: row.Did, Generation: row.Generation, Attempts: int(row.Attempts)}
		q.start(targets[0].session, db, work, actors)
	}
	q.sinceOldest = (q.sinceOldest + len(rows)) % readConcurrency
	return nil
}

func (q *readQueue) start(api *session, db *store, work job, actors []string) {
	q.Start(work.DID, func(ctx context.Context) checked {
		subject, err := api.subject(ctx, db, work.DID, actors...)
		return checked{work, actors, subject, err}
	})
}

var errJobSuperseded = errors.New("membership check superseded")

func checkGeneration(ctx context.Context, db *store, work job) error {
	current, err := db.queries.CurrentJob(ctx, storage.CurrentJobParams{Did: work.DID, Generation: work.Generation})
	if err != nil {
		return err
	}
	if !current {
		return errJobSuperseded
	}
	return nil
}

// Only the owner goroutine refreshes auth and exclusions or publishes records.
func completeCheck(ctx context.Context, targets []*listTarget, db *store, result checked) error {
	api := targets[0].session
	if !slices.Equal(result.actors, activeActors(targets)) {
		// A recovered list was absent from this read, or a list was paused.
		// Leave the job queued for a check against the current target set.
		return nil
	}
	if err := checkGeneration(ctx, db, result.work); err != nil {
		if errors.Is(err, errJobSuperseded) {
			return nil
		}
		return err
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
			return nil
		}
		followed, readErr := db.isFollowed(ctx, result.work.DID)
		if readErr != nil {
			return readErr
		}
		if result.subject.excluded && !followed && result.work.DID != api.Client.Auth.Did {
			// An unfollow invalidated the early exclusion check. Leave the job queued.
			return nil
		}
		result.subject.excluded = followed || result.work.DID == api.Client.Auth.Did
		result.subject.beforeWrite = func(ctx context.Context) error {
			if err := checkGeneration(ctx, db, result.work); err != nil {
				return err
			}
			return followers.CheckSnapshot(ctx, db)
		}
		err = applySubject(ctx, targets, result.work, result.subject, db)
	}
	if errors.Is(err, followers.ErrSnapshotSuperseded) || errors.Is(err, errJobSuperseded) {
		return nil
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
