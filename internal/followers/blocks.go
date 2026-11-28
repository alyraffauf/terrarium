package followers

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/xrpc"
)

// RefreshBlocks replaces each target's snapshot only after its last page.
// Changes and any queued list repairs commit together.
func RefreshBlocks(ctx context.Context, db *sql.DB, client *xrpc.Client, targets []string, changed func(context.Context, *sql.Tx, string) error) {
	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		if err := refreshBlocks(ctx, db, client, target, changed); err != nil && ctx.Err() == nil {
			slog.Warn("refresh follower blocks", "target", target, "error", err)
		}
	}
}

func refreshBlocks(ctx context.Context, db *sql.DB, client *xrpc.Client, target string, changed func(context.Context, *sql.Tx, string) error) error {
	blockers := map[string]bool{}
	err := palustre.WalkPages(func(cursor string) (*string, error) {
		rows, next, err := palustre.BacklinksPage(ctx, client, target, "app.bsky.graph.block", "subject", cursor)
		for _, row := range rows {
			blockers[row.Did] = true
		}
		return next, err
	})
	if err != nil {
		return err
	}
	raw, err := json.Marshal(blockers)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		"DELETE FROM follower_blocks WHERE target=?1 AND actor NOT IN (SELECT key FROM json_each(?2)) RETURNING actor",
		"INSERT OR IGNORE INTO follower_blocks(target,actor) SELECT ?1,key FROM json_each(?2) RETURNING actor",
	} {
		rows, err := tx.QueryContext(ctx, query, target, string(raw))
		if err != nil {
			return err
		}
		var actors []string
		for rows.Next() {
			var actor string
			if err := rows.Scan(&actor); err != nil {
				rows.Close()
				return err
			}
			actors = append(actors, actor)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if changed != nil {
			for _, actor := range actors {
				if err := changed(ctx, tx, actor); err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit()
}

func PollBlocks(ctx context.Context, refresh func(context.Context)) {
	for palustre.Pause(ctx, 5*time.Minute) {
		refresh(ctx)
	}
}
