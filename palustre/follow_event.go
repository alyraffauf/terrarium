package palustre

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

const FollowCollection = "app.bsky.graph.follow"

// Event separates a bad follow record, which can be recovered by scanning its
// actor, from a bad envelope, whose effects may span multiple actors.
type Event struct {
	Frame
	Kind            string
	Witnessed       time.Time
	FollowSubject   string
	MalformedFollow bool
	Recover         bool
}

func ClassifyFrame(raw []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(raw, &event.Frame); err != nil {
		return event, fmt.Errorf("%w: malformed JSON", ErrStreamGap)
	}
	if event.Type == "error" {
		switch event.ErrStr {
		case "CursorTooOld", "OutdatedCursor", "FutureCursor":
			return event, ErrStreamGap
		default:
			return event, fmt.Errorf("Jetstream %s: %s", event.ErrStr, event.Message)
		}
	}

	payload := event.Payload
	event.Kind = strings.TrimPrefix(payload.Type, EventPrefix)
	validEnvelope := event.Type == "message" && strings.HasPrefix(payload.Type, EventPrefix)
	if validEnvelope && event.Kind == "info" {
		if payload.Name == "OutdatedCursor" || payload.Name == "FutureCursor" {
			return event, ErrStreamGap
		}
		return event, nil
	}
	if payload.Seq <= 0 {
		return event, fmt.Errorf("%w: invalid sequence", ErrStreamGap)
	}
	switch event.Kind {
	case "identity", "account", "sync", "commit":
	default:
		validEnvelope = false
	}
	timestamp := payload.Time
	if payload.WitnessedAt != nil && *payload.WitnessedAt != "" {
		timestamp = *payload.WitnessedAt
	}
	var err error
	event.Witnessed, err = time.Parse(time.RFC3339Nano, timestamp)
	_, didErr := syntax.ParseDID(payload.Did)
	if !validEnvelope || err != nil || didErr != nil {
		event.Recover = true
		return event, nil
	}
	if event.Kind != "commit" || payload.Collection != FollowCollection {
		return event, nil
	}
	if _, err := syntax.ParseRecordKey(payload.Rkey); err != nil {
		event.MalformedFollow = true
		return event, nil
	}
	switch payload.Operation {
	case "delete":
	case "create", "update":
		var follow bsky.GraphFollow
		err := json.Unmarshal(payload.Record, &follow)
		_, didErr := syntax.ParseDID(follow.Subject)
		if err != nil || didErr != nil || follow.LexiconTypeID != FollowCollection {
			event.MalformedFollow = true
		} else {
			event.FollowSubject = follow.Subject
		}
	default:
		event.MalformedFollow = true
	}
	return event, nil
}
