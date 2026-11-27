package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

func (a *service) setupClusters(ctx context.Context, configs map[string]clusterConfig, legacyThreshold int) ([]*cluster, error) {
	// On a legacy volume, retain the original list even when only new clusters
	// are supplied. The old schema persisted its targets but not its threshold.
	if _, explicit := configs[defaultCluster]; !explicit {
		var version int
		if err := a.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
			return nil, err
		}
		query := "SELECT target FROM cluster_targets WHERE cluster='default'"
		if version == 0 {
			query = "SELECT target FROM watched"
		}
		rows, err := a.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		cfg := clusterConfig{Threshold: legacyThreshold}
		for rows.Next() {
			var target string
			if err = rows.Scan(&target); err != nil {
				rows.Close()
				return nil, err
			}
			cfg.Accounts = append(cfg.Accounts, target)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if len(cfg.Accounts) > 0 {
			if version == 1 {
				if err := a.QueryRowContext(ctx, "SELECT threshold FROM cluster_settings WHERE cluster='default'").Scan(&cfg.Threshold); err != nil {
					return nil, err
				}
			}
			configs[defaultCluster] = cfg
		}
	}
	var ids []string
	for id := range configs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	groups := map[string][]string{}
	resolved := map[string]string{}
	var lists []*cluster
	for _, id := range ids {
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
			groups[id] = append(groups[id], did)
			a.targets[did] = true
		}
		lists = append(lists, &cluster{service: a, id: id, list: listURI(a.owner, id), threshold: configs[id].Threshold})
	}
	err := a.transaction(ctx, func(tx *sql.Tx) error {
		var version int
		if err := tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
			return err
		}
		if version == 0 {
			// Legacy discovery did not record per-target completion. Reuse its
			// existing queue and let the next regular audit revisit these targets.
			if _, err := tx.Exec("INSERT OR IGNORE INTO discovered SELECT target FROM watched WHERE EXISTS(SELECT 1 FROM edges)"); err != nil {
				return err
			}
		}
		if err := configureStore(tx, a.owner, a.targets); err != nil {
			return err
		}
		if err := migrateClusters(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM cluster_targets"); err != nil {
			return err
		}
		for id, targets := range groups {
			if _, err := tx.Exec("INSERT OR REPLACE INTO cluster_settings VALUES(?,?)", id, configs[id].Threshold); err != nil {
				return err
			}
			for _, target := range targets {
				if _, err := tx.Exec("INSERT OR IGNORE INTO cluster_targets VALUES(?,?)", id, target); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return lists, err
}

func migrateClusters(tx *sql.Tx) error {
	var version int
	if err := tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == 1 {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("unknown Trifolium database version %d", version)
	}
	for _, table := range []struct{ name, columns, key, fields string }{
		{"items", "did TEXT,rkey TEXT", "did", "did,rkey"},
		{"pending", "did TEXT", "did", "did"},
		{"published", "did TEXT", "did", "did"},
		{"retries", "did TEXT,due INTEGER", "did", "did,due"},
		{"descriptions", "text TEXT", "text", "text"},
	} {
		query := fmt.Sprintf(`ALTER TABLE %[1]s RENAME TO legacy_%[1]s;
 CREATE TABLE %[1]s(cluster TEXT NOT NULL,%[2]s,PRIMARY KEY(cluster,%[3]s));
 INSERT INTO %[1]s SELECT 'default',%[4]s FROM legacy_%[1]s;
 DROP TABLE legacy_%[1]s;`, table.name, table.columns, table.key, table.fields)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}
	_, err := tx.Exec("CREATE UNIQUE INDEX item_keys ON items(cluster,rkey); PRAGMA user_version=1")
	return err
}
