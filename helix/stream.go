package helix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"github.com/alyraffauf/terrarium/palustre"
)

const streamPath = palustre.StreamPath
const eventPrefix = palustre.EventPrefix

var errStreamGap = palustre.ErrStreamGap

type streamFrame = palustre.Frame

func collect(ctx context.Context, cfg Config, db *store, rescan chan<- struct{}, watched map[string]bool) error {
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
			select {
			case rescan <- struct{}{}:
			default:
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
	var frame streamFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return fmt.Errorf("%w: malformed JSON", errStreamGap)
	}
	if frame.Type == "error" {
		if frame.Error == "CursorTooOld" {
			return errStreamGap
		}
		return fmt.Errorf("Jetstream %s: %s", frame.Error, frame.Message)
	}
	if frame.Type != "message" || !strings.HasPrefix(frame.Payload.Type, eventPrefix) {
		return recoverFrame(ctx, db, endpoint, frame, payload)
	}
	update := frame.Payload
	kind := strings.TrimPrefix(update.Type, eventPrefix)
	if kind == "info" {
		if update.Name == "OutdatedCursor" || update.Name == "FutureCursor" {
			return errStreamGap
		}
		return nil
	}
	if update.Seq <= 0 {
		return fmt.Errorf("%w: invalid sequence", errStreamGap)
	}
	timestamp := update.WitnessedAt
	if timestamp == "" {
		timestamp = update.Time
	}
	witnessed, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return recoverFrame(ctx, db, endpoint, frame, payload)
	}
	if _, err := syntax.ParseDID(update.DID); err != nil {
		return recoverFrame(ctx, db, endpoint, frame, payload)
	}
	did := ""
	known, err := db.knownCandidate(ctx, update.DID)
	if err != nil {
		return err
	}
	switch kind {
	case "identity", "account":
		if known {
			did = update.DID
		}
	case "sync":
		did = update.DID
	case "commit":
		if update.Collection == "app.bsky.graph.follow" {
			switch update.Operation {
			case "create", "update":
				var record struct {
					Subject string `json:"subject"`
				}
				if err := json.Unmarshal(update.Record, &record); err != nil {
					return recoverFrame(ctx, db, endpoint, frame, payload)
				}
				if watched[record.Subject] || known {
					did = update.DID
				}
			case "delete":
				if known {
					did = update.DID
				}
			default:
				return recoverFrame(ctx, db, endpoint, frame, payload)
			}
		}
	default:
		return recoverFrame(ctx, db, endpoint, frame, payload)
	}
	followChanged := update.DID == owner && (kind == "sync" || kind == "commit" && update.Collection == "app.bsky.graph.follow")
	if did == "" && !followChanged && !saveUnrelated {
		return nil
	}

	return db.ingest(ctx, did, checkpoint{TimeUS: witnessed.UnixMicro(), Seq: update.Seq, Endpoint: endpoint}, followChanged)
}

var pause = palustre.Pause

func recoverFrame(ctx context.Context, db *store, endpoint string, frame streamFrame, payload []byte) error {
	slog.Warn("skipping invalid Jetstream event; requesting reconciliation", "frame", string(payload[:min(len(payload), 2048)]))
	if frame.Payload.Seq <= 0 {
		return errStreamGap
	}
	// Preserve the last trusted timestamp while advancing this instance's sequence.
	// Dirty follows and request durable backfill before accepting later events.
	return db.recoverStream(ctx, endpoint, frame.Payload.Seq, string(payload[:min(len(payload), 2048)]))
}
