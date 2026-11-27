package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

const defaultCluster = "default"

type clusterConfig struct {
	Accounts  []string `json:"accounts"`
	Threshold int      `json:"threshold"`
}
type cluster struct {
	*service
	id, list        string
	threshold       int
	nextDescription time.Time
	listValidated   bool
}

func listURI(owner, id string) string {
	seed := "trifolium:" + owner + ":" + id
	if id == defaultCluster {
		seed = "crossover:" + owner
	}
	return fmt.Sprintf("at://%s/%s/%s", owner, palustre.ListCollection, palustre.RecordKey(seed))
}

func parseClusters(raw, accounts string, threshold int) (map[string]clusterConfig, error) {
	configs := map[string]clusterConfig{}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &configs); err != nil {
			return nil, err
		}
	}
	if configs == nil {
		return nil, fmt.Errorf("clusters must be an object")
	}
	if accounts != "" {
		if _, ok := configs[defaultCluster]; ok {
			return nil, fmt.Errorf("default cluster specified twice")
		}
		configs[defaultCluster] = clusterConfig{strings.Split(accounts, ","), threshold}
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("provide accounts or clusters")
	}
	for id, cfg := range configs {
		if strings.TrimSpace(id) != id || id == "" || len(cfg.Accounts) == 0 || cfg.Threshold < 1 {
			return nil, fmt.Errorf("cluster %q requires an ID, accounts and a positive threshold", id)
		}
		for _, actor := range cfg.Accounts {
			if strings.TrimSpace(actor) == "" {
				return nil, fmt.Errorf("empty account in cluster %q", id)
			}
		}
	}
	return configs, nil
}

func (a *service) includeLegacyCluster(ctx context.Context, configs map[string]clusterConfig, threshold int) (map[string]clusterConfig, error) {
	if _, explicit := configs[defaultCluster]; explicit {
		return configs, nil
	}
	var version int
	if err := a.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version != 0 {
		return configs, nil
	}
	rows, err := a.QueryContext(ctx, "SELECT target FROM watched")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	legacy := clusterConfig{Threshold: threshold}
	for rows.Next() {
		var target string
		if err := rows.Scan(&target); err != nil {
			return nil, err
		}
		legacy.Accounts = append(legacy.Accounts, target)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(legacy.Accounts) == 0 {
		return configs, nil
	}
	withLegacy := make(map[string]clusterConfig, len(configs)+1)
	for id, cfg := range configs {
		withLegacy[id] = cfg
	}
	withLegacy[defaultCluster] = legacy
	return withLegacy, nil
}

func (a *service) setupClusters(ctx context.Context, configs map[string]clusterConfig, legacyThreshold int) ([]*cluster, error) {
	configs, err := a.includeLegacyCluster(ctx, configs, legacyThreshold)
	if err != nil {
		return nil, err
	}

	var ids []string
	for id := range configs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	groups := map[string]map[string]bool{}
	targets := map[string]bool{}
	resolved := map[string]string{}
	var lists []*cluster
	for _, id := range ids {
		groups[id] = map[string]bool{}
		for _, actor := range configs[id].Accounts {
			actor = strings.TrimSpace(actor)
			did := resolved[actor]
			if did == "" {
				if parsed, e := syntax.ParseDID(actor); e == nil {
					did = parsed.String()
				} else {
					doc, err := a.api.Resolve(ctx, actor)
					if err != nil {
						return nil, err
					}
					did = doc.DID
				}
				resolved[actor] = did
			}
			groups[id][did] = true
			targets[did] = true
		}
		if configs[id].Threshold > len(groups[id]) {
			slog.Warn("threshold exceeds distinct watched accounts", "cluster", id, "threshold", configs[id].Threshold, "accounts", len(groups[id]))
		}
		lists = append(lists, &cluster{service: a, id: id, list: listURI(a.owner, id), threshold: configs[id].Threshold})
	}
	err = a.transaction(ctx, func(tx *sql.Tx) error {
		if err := migrateClusters(tx); err != nil {
			return err
		}
		for id, targets := range groups {
			if err := reverifyClusterMembers(tx, id, targets); err != nil {
				return err
			}
		}
		if err := configureStore(tx, a.owner, targets); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM cluster_targets"); err != nil {
			return err
		}
		for id, targets := range groups {
			for target := range targets {
				if _, err := tx.Exec("INSERT INTO cluster_targets VALUES(?,?)", id, target); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	a.targets = targets
	return lists, nil
}

// Published actors may already follow a newly watched target. Verify those
// actors before removing members based on the currently incomplete edges.
func reverifyClusterMembers(tx *sql.Tx, id string, targets map[string]bool) error {
	for target := range targets {
		var alreadyWatched bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM cluster_targets WHERE cluster=? AND target=?)", id, target).Scan(&alreadyWatched); err != nil {
			return err
		}
		if alreadyWatched {
			continue
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO audit_wait SELECT cluster,did FROM published WHERE cluster=?", id); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO scans(did) SELECT did FROM published WHERE cluster=?
ON CONFLICT(did) DO UPDATE SET version=version+1,due=0`, id)
		return err
	}
	return nil
}

func migrateClusters(tx *sql.Tx) error {
	var version int
	if err := tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch version {
	case 2:
		return nil
	case 1:
		_, err := tx.Exec(`
ALTER TABLE items RENAME TO items_by_did;
CREATE TABLE items(cluster TEXT NOT NULL,did TEXT NOT NULL,rkey TEXT NOT NULL,PRIMARY KEY(cluster,rkey));
INSERT INTO items SELECT cluster,did,rkey FROM items_by_did;
DROP TABLE items_by_did;
CREATE INDEX items_by_did ON items(cluster,did);
PRAGMA user_version=2;`)
		return err
	case 0:
		return migrateSingleList(tx)
	default:
		return fmt.Errorf("unknown Trifolium database version %d", version)
	}
}

func migrateSingleList(tx *sql.Tx) error {
	_, err := tx.Exec(`
ALTER TABLE items RENAME TO legacy_items;
ALTER TABLE pending RENAME TO legacy_pending;
ALTER TABLE published RENAME TO legacy_published;
ALTER TABLE retries RENAME TO legacy_retries;
ALTER TABLE descriptions RENAME TO legacy_descriptions;
` + membershipSchema + `
INSERT INTO items SELECT 'default',did,rkey FROM legacy_items;
INSERT INTO pending SELECT 'default',did FROM legacy_pending;
INSERT INTO published SELECT 'default',did FROM legacy_published;
INSERT INTO retries SELECT 'default',did,due FROM legacy_retries;
INSERT INTO descriptions SELECT 'default',text FROM legacy_descriptions;
INSERT INTO cluster_targets SELECT 'default',target FROM watched;
DROP TABLE legacy_items;
DROP TABLE legacy_pending;
DROP TABLE legacy_published;
DROP TABLE legacy_retries;
DROP TABLE legacy_descriptions;
PRAGMA user_version=2;`)
	return err
}
