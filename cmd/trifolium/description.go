package main

import (
	"context"
	"fmt"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lex "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"strings"
	"time"
)

const oldDescription = "Accounts meeting the configured follower crossover threshold."
const ranking = `
 WITH follows AS (SELECT DISTINCT actor,target FROM edges JOIN watched USING(target)),
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
func (a *service) refreshDescription(ctx context.Context) error {
	rows, err := a.QueryContext(ctx, ranking)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var handles []string
	if len(ids) > 0 {
		var profiles struct {
			Profiles []struct{ Did, Handle string }
		}
		client := &xrpc.Client{Host: "https://public.api.bsky.app", Client: a.http}
		err := client.LexDo(ctx, lex.Query, "", "app.bsky.actor.getProfiles", map[string]any{"actors": ids}, nil, &profiles)
		if err != nil {
			return err
		}
		byID := map[string]string{}
		for _, p := range profiles.Profiles {
			if h, e := syntax.ParseHandle(p.Handle); e == nil && p.Handle != "handle.invalid" {
				byID[p.Did] = "@" + h.String()
			}
		}
		for _, id := range ids {
			if h := byID[id]; h != "" {
				handles = append(handles, h)
			}
		}
	}
	description := clusterDescription(handles)
	// Remember attempted texts before writing so a lost response is safe to retry.
	if _, err = a.ExecContext(ctx, "INSERT OR IGNORE INTO descriptions VALUES(?)", description); err != nil {
		return err
	}
	previous := []string{oldDescription}
	rows, err = a.QueryContext(ctx, "SELECT text FROM descriptions")
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
	if err = a.api.EnsureList(ctx, a.list, description, previous...); err != nil {
		return fmt.Errorf("update cluster description: %w", err)
	}
	_, err = a.ExecContext(ctx, "DELETE FROM descriptions WHERE text<>?", description)
	return err
}
func (a *service) publishWithDescription(ctx context.Context) error {
	if !time.Now().Before(a.nextDescription) {
		a.nextDescription = time.Now().Add(5 * time.Minute)
		if err := a.refreshDescription(ctx); err != nil {
			return err
		}
	}
	return a.publish(ctx)
}
