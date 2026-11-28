package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

const oldDescription = "Accounts meeting the configured follower crossover threshold."
const ranking = `
 WITH follows AS (SELECT DISTINCT actor,target FROM qualifying_follows JOIN cluster_targets USING(target) WHERE cluster=?),
 shared AS (SELECT actor FROM follows GROUP BY actor HAVING count(*)>1)
 SELECT target FROM follows JOIN shared USING(actor)
 GROUP BY target ORDER BY count(*) DESC,target LIMIT 3`

func clusterDescription(handles []string) string {
	for len(handles) > 0 {
		var names string
		switch len(handles) {
		case 1:
			names = handles[0]
		case 2:
			names = strings.Join(handles, " and ")
		default:
			names = strings.Join(handles[:2], ", ") + ", and " + handles[2]
		}
		description := "Accounts clustered around " + names + "."
		if len(description) <= 300 {
			return description
		}
		handles = handles[:len(handles)-1]
	}
	return "Accounts with overlapping follows."
}

// A restored database may predate the last published ranking. Recognize only
// the exact final line produced by clusterDescription at this list's stable URI.
func isClusterDescription(line string) bool {
	if line == clusterDescription(nil) {
		return true
	}
	names, prefixed := strings.CutPrefix(line, "Accounts clustered around ")
	names, suffixed := strings.CutSuffix(names, ".")
	if !prefixed || !suffixed {
		return false
	}
	names = strings.NewReplacer(", and ", ", ", " and ", ", ").Replace(names)
	handles := strings.Split(names, ", ")
	if len(handles) > 3 {
		return false
	}
	for _, name := range handles {
		raw, ok := strings.CutPrefix(name, "@")
		if !ok {
			return false
		}
		if _, err := syntax.ParseHandle(raw); err != nil {
			return false
		}
	}
	return line == clusterDescription(handles)
}

func (a *cluster) desiredDescription(ctx context.Context) (string, error) {
	rows, err := a.QueryContext(ctx, ranking, a.id)
	if err != nil {
		return "", err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return "", err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	var handles []string
	for _, id := range ids {
		doc, _, err := a.api.CachedDID(ctx, id)
		if err != nil {
			return "", err
		}
		if doc.Handle != "" {
			handles = append(handles, "@"+doc.Handle)
		}
	}
	return clusterDescription(handles), nil
}

func (a *cluster) ensureDescription(ctx context.Context, description string) error {
	// Remember attempted texts before writing so a lost response is safe to retry.
	if _, err := a.ExecContext(ctx, "INSERT OR IGNORE INTO descriptions VALUES(?,?)", a.id, description); err != nil {
		return err
	}
	previous := []string{oldDescription}
	rows, err := a.QueryContext(ctx, "SELECT text FROM descriptions WHERE cluster=?", a.id)
	if err != nil {
		return err
	}
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			rows.Close()
			return err
		}
		previous = append(previous, line)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err = a.api.Refresh(ctx); err != nil {
		return err
	}
	if err = a.api.EnsureClusterList(ctx, a.list, description, isClusterDescription, previous...); err != nil {
		return fmt.Errorf("update cluster description: %w", err)
	}
	_, err = a.ExecContext(ctx, "DELETE FROM descriptions WHERE cluster=? AND text<>?", a.id, description)
	return err
}
func (a *cluster) publishWithDescription(ctx context.Context) error {
	if !time.Now().Before(a.nextDescription) {
		const interval = 5 * time.Minute
		description, err := a.desiredDescription(ctx)
		lookupFailed := err != nil
		a.descriptionRetry = palustre.ScanRetryDelay(a.descriptionRetry, interval)
		a.nextDescription = time.Now().Add(a.descriptionRetry)
		if err != nil {
			slog.Warn("cluster description lookup", "cluster", a.id, "error", err)
			// Validate ownership even during a resolver outage. Reuse a known
			// description so a transient failure does not rename an existing list.
			err = a.QueryRowContext(ctx, "SELECT text FROM descriptions WHERE cluster=? ORDER BY rowid DESC LIMIT 1", a.id).Scan(&description)
			if errors.Is(err, sql.ErrNoRows) {
				description = clusterDescription(nil)
			} else if err != nil {
				return err
			}
		}
		if err = a.ensureDescription(ctx, description); err != nil {
			if !errors.Is(err, palustre.ErrListUpdate) {
				a.listValidated = false
				return err
			}
			slog.Warn("cluster description update", "cluster", a.id, "error", err)
		} else if !lookupFailed {
			a.descriptionRetry = 0
			a.nextDescription = time.Now().Add(interval)
		}
		a.listValidated = true
	}
	if !a.listValidated {
		return sql.ErrNoRows
	}
	return a.publish(ctx)
}
