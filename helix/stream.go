package helix

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
)

const streamPath = palustre.StreamPath

var errStreamGap = palustre.ErrStreamGap

func collect(ctx context.Context, cfg Config, db *store, watched map[string]bool) error {
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := stream(ctx, cfg.Jetstream, db, watched)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errStreamGap) {
			if err := db.resetStream(ctx); err != nil {
				return err
			}
		}
		slog.Warn("Jetstream disconnected", "error", err, "retry_in", delay)
		if !pause(ctx, delay) {
			return nil
		}
		if time.Since(started) > time.Minute {
			delay = time.Second
		} else {
			delay = min(delay*2, time.Minute)
		}
	}
	return nil
}

func stream(ctx context.Context, endpoint string, db *store, watched map[string]bool) error {
	owner, err := db.owner(ctx)
	if err != nil {
		return err
	}
	cursor, err := db.resumeCursor(ctx, endpoint)
	if err != nil {
		return err
	}
	lastCheckpoint := time.Time{}
	return palustre.Stream(ctx, endpoint, cursor, func(payload []byte) error {
		saveUnrelated := time.Since(lastCheckpoint) >= time.Second
		if err := ingestFrame(ctx, db, endpoint, owner, payload, watched, saveUnrelated); err != nil {
			return err
		}
		if saveUnrelated {
			lastCheckpoint = time.Now()
		}

		return nil
	})
}

func ingestFrame(ctx context.Context, db *store, endpoint, owner string, payload []byte, watched map[string]bool, saveUnrelated bool) error {
	event, err := palustre.ClassifyFrame(payload)
	if err != nil {
		return err
	}
	if event.Recover {
		return recoverFrame(ctx, db, endpoint, event.Payload.Seq, payload)
	}
	if event.Kind == "info" {
		return nil
	}
	update := event.Payload
	did := ""
	known, err := db.knownCandidate(ctx, update.Did)
	if err != nil {
		return err
	}
	switch event.Kind {
	case "identity", "account":
		if known {
			did = update.Did
		}
	case "sync":
		did = update.Did
	case "commit":
		if update.Collection == palustre.FollowCollection && (event.MalformedFollow || watched[event.FollowSubject] || known) {
			did = update.Did
		}
	}
	followChanged := update.Did == owner && (event.Kind == "sync" || event.Kind == "commit" && update.Collection == palustre.FollowCollection)
	if did == "" && !followChanged && !saveUnrelated {
		return nil
	}

	return db.ingest(ctx, did, checkpoint{TimeUS: event.Witnessed.UnixMicro(), Seq: update.Seq, Endpoint: endpoint}, followChanged)
}

var pause = palustre.Pause

func recoverFrame(ctx context.Context, db *store, endpoint string, seq int64, payload []byte) error {
	detail := string(payload[:min(len(payload), 2048)])
	slog.Warn("skipping invalid Jetstream event; requesting reconciliation", "frame", detail)
	// Preserve the last trusted timestamp while advancing this instance's sequence.
	// Dirty follows and request durable backfill before accepting later events.
	return db.recoverStream(ctx, endpoint, seq, detail)
}
