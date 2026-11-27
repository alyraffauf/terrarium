package main

import (
	"context"
	"database/sql"

	"github.com/alyraffauf/terrarium/palustre"
)

// Only the publisher uses the authenticated session or changes list items.
// Collect a full remote snapshot before changing any local membership state.
func (a *service) auditMembers(ctx context.Context, clusters []*cluster) error {
	if err := a.api.Refresh(ctx); err != nil {
		return err
	}
	byURI := map[string]string{}
	for _, c := range clusters {
		byURI[c.list] = c.id
	}
	type item struct{ cluster, did, key string }
	var items []item
	err := palustre.WalkListItems(ctx, a.api.Client, func(list, did, key string) error {
		if id, ok := byURI[list]; ok {
			items = append(items, item{id, did, key})
		}
		return nil
	})
	if err != nil {
		return err
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec("CREATE TEMP TABLE observed_items(cluster TEXT,did TEXT,rkey TEXT,PRIMARY KEY(cluster,rkey)); CREATE INDEX observed_by_did ON observed_items(cluster,did)"); err != nil {
			return err
		}
		stmt, err := tx.Prepare("INSERT INTO observed_items VALUES(?,?,?)")
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, v := range items {
			if _, err := stmt.Exec(v.cluster, v.did, v.key); err != nil {
				return err
			}
		}
		for _, c := range clusters {
			// A restored database can lack follow evidence for existing members.
			// Verify those actors before deciding whether to remove them.
			for _, query := range []string{
				`INSERT OR IGNORE INTO audit_wait
				 SELECT DISTINCT cluster,did FROM observed_items WHERE cluster=?
				 EXCEPT SELECT cluster,did FROM published
				 EXCEPT SELECT cluster,did FROM pending`,
				`INSERT OR IGNORE INTO scans(did)
				 SELECT w.did FROM audit_wait w WHERE w.cluster=?
				 AND NOT EXISTS (SELECT 1 FROM pending p WHERE p.cluster=w.cluster AND p.did=w.did)`,
				`INSERT OR IGNORE INTO pending
				 SELECT o.cluster,o.did FROM observed_items o WHERE o.cluster=? AND
				 (NOT EXISTS (SELECT 1 FROM items i WHERE i.cluster=o.cluster AND i.rkey=o.rkey AND i.did=o.did)
				 OR NOT EXISTS (SELECT 1 FROM published p WHERE p.cluster=o.cluster AND p.did=o.did))`,
				`INSERT OR IGNORE INTO pending
				 SELECT i.cluster,i.did FROM items i WHERE i.cluster=? AND NOT EXISTS
				 (SELECT 1 FROM observed_items o WHERE o.cluster=i.cluster AND o.rkey=i.rkey AND o.did=i.did)`,
				`INSERT OR IGNORE INTO pending
				 SELECT cluster,did FROM published WHERE cluster=? EXCEPT SELECT cluster,did FROM observed_items`,
				`DELETE FROM items WHERE cluster=? AND NOT EXISTS
				 (SELECT 1 FROM observed_items o WHERE o.cluster=items.cluster AND o.rkey=items.rkey AND o.did=items.did)`,
				`INSERT OR IGNORE INTO items(cluster,did,rkey) SELECT cluster,did,rkey FROM observed_items WHERE cluster=?`,
			} {
				if _, err := tx.Exec(query, c.id); err != nil {
					return err
				}
			}
		}
		_, err = tx.Exec("DROP TABLE observed_items")
		return err
	})
}
