package followers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
)

type Checkpoint struct {
	TimeUS, Seq int64
	Endpoint    string
}

type StreamStore interface {
	Owner(context.Context) (string, error)
	ResumeCursor(context.Context, string) (int64, error)
	ResetStream(context.Context) error
	RecoverStream(ctx context.Context, endpoint string, seq int64, detail string) error
	// Ingest commits the candidate, owner-follow invalidation, and checkpoint together.
	Ingest(ctx context.Context, candidate string, position Checkpoint, followChanged bool) error
}

type Collector struct {
	Store           StreamStore
	Endpoint        string
	SelectCandidate func(context.Context, palustre.Event) (bool, error)
}

func (c Collector) Run(ctx context.Context) error {
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.stream(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, palustre.ErrStreamGap) {
			if err := c.Store.ResetStream(ctx); err != nil {
				return err
			}
		}
		slog.Warn("Jetstream disconnected", "error", err, "retry_in", delay)
		if !palustre.Pause(ctx, delay) {
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

func (c Collector) stream(ctx context.Context) error {
	owner, err := c.Store.Owner(ctx)
	if err != nil {
		return err
	}
	cursor, err := c.Store.ResumeCursor(ctx, c.Endpoint)
	if err != nil {
		return err
	}
	var lastCheckpoint time.Time
	return palustre.Stream(ctx, c.Endpoint, cursor, func(payload []byte) error {
		saveUnrelated := time.Since(lastCheckpoint) >= time.Second
		if err := c.IngestFrame(ctx, owner, payload, saveUnrelated); err != nil {
			return err
		}
		if saveUnrelated {
			lastCheckpoint = time.Now()
		}
		return nil
	})
}

func (c Collector) IngestFrame(ctx context.Context, owner string, payload []byte, saveUnrelated bool) error {
	event, err := palustre.ClassifyFrame(payload)
	if err != nil {
		return err
	}
	update := event.Payload
	if event.Recover {
		// Advance this endpoint's sequence while preserving the last trusted timestamp.
		detail := string(payload[:min(len(payload), 2048)])
		slog.Warn("skipping invalid Jetstream event; requesting reconciliation", "frame", detail)
		return c.Store.RecoverStream(ctx, c.Endpoint, update.Seq, detail)
	}
	if event.Kind == "info" {
		return nil
	}
	selected, err := c.SelectCandidate(ctx, event)
	if err != nil {
		return err
	}
	followChanged := update.Did == owner && (event.Kind == "sync" || event.Kind == "commit" && update.Collection == palustre.FollowCollection)
	if !selected && !followChanged && !saveUnrelated {
		return nil
	}
	var did string
	if selected {
		did = update.Did
	}
	return c.Store.Ingest(ctx, did, Checkpoint{TimeUS: event.Witnessed.UnixMicro(), Seq: update.Seq, Endpoint: c.Endpoint}, followChanged)
}
