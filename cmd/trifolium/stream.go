package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

func (a *service) prepareStream(ctx context.Context) error {
	return a.transaction(ctx, func(tx *sql.Tx) error {
		var active string
		err := tx.QueryRow("SELECT endpoint FROM stream_progress WHERE id=1").Scan(&active)
		if errors.Is(err, sql.ErrNoRows) {
			// The previous schema stored a sequence per endpoint without saying
			// which endpoint was active or recording a portable timestamp.
			var missed bool
			if err = tx.QueryRow("SELECT (EXISTS(SELECT 1 FROM cursor) OR EXISTS(SELECT 1 FROM edges)) AND NOT EXISTS(SELECT 1 FROM cursor WHERE endpoint=?)", a.stream).Scan(&missed); err != nil {
				return err
			}
			if _, err = tx.Exec("INSERT INTO stream_progress(id,endpoint,cursor_us) VALUES(1,?,?)", a.stream, time.Now().Add(-time.Minute).UnixMicro()); err != nil {
				return err
			}
			if missed {
				return a.scheduleStreamRecovery(tx)
			}
		} else if err != nil {
			return err
		} else if active != a.stream {
			if _, err := tx.Exec("UPDATE stream_progress SET endpoint=? WHERE id=1", a.stream); err != nil {
				return err
			}
			return a.scheduleStreamRecovery(tx)
		}
		_, err = tx.Exec(dirty, a.owner)
		return err
	})
}

func (a *service) resumeStream(ctx context.Context) (int64, error) {
	var cursor, timestamp int64
	err := a.QueryRowContext(ctx, `SELECT coalesce((SELECT seq FROM cursor WHERE endpoint=?),0),cursor_us FROM stream_progress WHERE id=1`, a.stream).Scan(&cursor, &timestamp)
	if err != nil || cursor > 0 {
		return cursor, err
	}
	if timestamp > 0 {
		return timestamp - 5_000_000, nil
	}
	return time.Now().Add(-time.Minute).UnixMicro(), nil
}

func (a *service) scheduleStreamRecovery(tx *sql.Tx) error {
	if _, err := tx.Exec(`
DELETE FROM cursor;
UPDATE scans SET version=version+1,due=0;
INSERT OR IGNORE INTO scans(did) SELECT DISTINCT actor FROM edges;
INSERT OR IGNORE INTO scans(did) SELECT DISTINCT did FROM hints;
DELETE FROM hints;
UPDATE discovery_schedule SET due=0,generation=generation+1,cursor='',turn=0,retrying=0;
DELETE FROM discovery_cursors;
UPDATE inventory_schedule SET due=0 WHERE id=1;
`); err != nil {
		return err
	}
	_, err := tx.Exec(dirty, a.owner)
	return err
}

func (a *service) resetStream(ctx context.Context) error {
	return a.transaction(ctx, func(tx *sql.Tx) error {
		if err := a.scheduleStreamRecovery(tx); err != nil {
			return err
		}
		_, err := tx.Exec("UPDATE stream_progress SET cursor_us=? WHERE id=1", time.Now().Add(-time.Minute).UnixMicro())
		return err
	})
}

func (a *service) signalBackfill() {
	select {
	case a.wakeBackfill <- struct{}{}:
	default:
	}
}

func (a *service) listen(ctx context.Context) {
	for ctx.Err() == nil {
		seq, err := a.resumeStream(ctx)
		if err == nil {
			err = palustre.Stream(ctx, a.stream, seq, func(raw []byte) error { return a.ingest(ctx, raw) })
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, palustre.ErrStreamGap) {
			recovery := a.resetStream(ctx)
			if recovery == nil {
				a.signalBackfill()
			} else {
				err = errors.Join(err, recovery)
			}
		}
		slog.Warn("reconnecting Jetstream", "error", err)
		palustre.Pause(ctx, 2*time.Second)
	}
}

func (a *service) ingest(ctx context.Context, raw []byte) error {
	event, err := palustre.ClassifyFrame(raw)
	if err != nil {
		return err
	}
	if event.Kind == "info" && !event.Recover {
		return nil
	}
	p := event.Payload
	recoveryScheduled := false
	err = a.transaction(ctx, func(tx *sql.Tx) error {
		var previous int64
		if err := tx.QueryRow("SELECT coalesce((SELECT seq FROM cursor WHERE endpoint=?),0)", a.stream).Scan(&previous); err != nil {
			return err
		}
		if p.Seq <= previous {
			return nil
		}

		switch {
		case event.Recover:
			if err := a.scheduleStreamRecovery(tx); err != nil {
				return err
			}
			recoveryScheduled = true
		case event.MalformedFollow:
			if err := a.recoverFollowEvent(tx, event); err != nil {
				return err
			}
		case event.Kind == "commit" && p.Collection == followCollection:
			if err := a.applyFollowEvent(tx, event); err != nil {
				return err
			}
		case event.Kind == "sync":
			if _, err := tx.Exec(dirty, p.Did); err != nil {
				return err
			}
		case event.Kind == "account" || event.Kind == "identity":
			known, err := a.knownActor(tx, p.Did)
			if err != nil {
				return err
			}
			if known {
				if _, err := tx.Exec(dirty, p.Did); err != nil {
					return err
				}
			}
		}
		if !event.Recover {
			if _, err := tx.Exec(`INSERT INTO stream_progress(id,endpoint,cursor_us) VALUES(1,?,?)
ON CONFLICT(id) DO UPDATE SET cursor_us=CASE WHEN ?=0 THEN excluded.cursor_us ELSE max(cursor_us,excluded.cursor_us) END`,
				a.stream, event.Witnessed.UnixMicro(), previous); err != nil {
				return err
			}
		}
		_, err := tx.Exec("INSERT OR REPLACE INTO cursor VALUES(?,?)", a.stream, p.Seq)
		return err
	})
	if err == nil && recoveryScheduled {
		a.signalBackfill()
	}
	return err
}

func (a *service) knownActor(tx *sql.Tx, did string) (bool, error) {
	if did == a.owner {
		return true, nil
	}
	var known bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM edges WHERE actor=?1)
OR EXISTS(SELECT 1 FROM hints WHERE did=?1)
OR EXISTS(SELECT 1 FROM scans WHERE did=?1)`, did).Scan(&known)
	return known, err
}

func (a *service) recoverFollowEvent(tx *sql.Tx, event palustre.Event) error {
	p := event.Payload
	known, err := a.knownActor(tx, p.Did)
	if err != nil || !known {
		return err
	}
	if _, err := syntax.ParseRecordKey(p.Rkey); err == nil {
		if _, err := tx.Exec("DELETE FROM hints WHERE did=? AND rkey=?", p.Did, p.Rkey); err != nil {
			return err
		}
	} else if _, err := tx.Exec("DELETE FROM hints WHERE did=?", p.Did); err != nil {
		return err
	}
	_, err = tx.Exec(dirty, p.Did)
	return err
}

func (a *service) applyFollowEvent(tx *sql.Tx, event palustre.Event) error {
	p := event.Payload
	if p.Operation == "create" && !a.targets[event.FollowSubject] && p.Did != a.owner {
		return nil
	}
	if _, err := tx.Exec("DELETE FROM hints WHERE did=? AND rkey=?", p.Did, p.Rkey); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM edges WHERE actor=? AND rkey=?", p.Did, p.Rkey); err != nil {
		return err
	}
	if p.Operation != "delete" && (a.targets[event.FollowSubject] || p.Did == a.owner) {
		if _, err := tx.Exec("INSERT INTO edges VALUES(?,?,?)", p.Did, p.Rkey, event.FollowSubject); err != nil {
			return err
		}
	}
	// Invalidate any inventory started before this record changed.
	_, err := tx.Exec("UPDATE scans SET version=version+1,due=0 WHERE did=?", p.Did)
	return err
}
