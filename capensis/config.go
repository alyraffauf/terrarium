package capensis

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"github.com/alyraffauf/terrarium/palustre"
)

type Config struct {
	MetricsAddress                                                      string
	PDS, Identifier, Password, LoginPDS, Slingshot, Jetstream, Database string
	Targets                                                             []Target
	WriteInterval                                                       time.Duration
	RescanInterval                                                      time.Duration
}

type Target struct {
	PDS     string
	ListURI string
}

func ParseTargets(pdses, bindings []string) ([]Target, error) {
	var targets []Target
	indexes := make(map[string]int)
	for _, value := range pdses {
		for _, entry := range strings.Split(value, ",") {
			if strings.TrimSpace(entry) == "" {
				continue
			}
			pds, err := normalizePDS(strings.TrimSpace(entry))
			if err != nil {
				return nil, err
			}
			if _, exists := indexes[pds]; exists {
				return nil, fmt.Errorf("duplicate PDS %s", pds)
			}
			indexes[pds] = len(targets)
			targets = append(targets, Target{PDS: pds})
		}
	}
	for _, value := range bindings {
		for _, entry := range strings.Split(value, ",") {
			pds, uri, ok := strings.Cut(strings.TrimSpace(entry), "=")
			pds, uri = strings.TrimSpace(pds), strings.TrimSpace(uri)
			if !ok || uri == "" {
				return nil, fmt.Errorf("--list expects PDS=at://DID/app.bsky.graph.list/record-key")
			}
			pds, err := normalizePDS(pds)
			if err != nil {
				return nil, err
			}
			i, exists := indexes[pds]
			if !exists {
				return nil, fmt.Errorf("list binding refers to unwatched PDS %s", pds)
			}
			if targets[i].ListURI != "" {
				return nil, fmt.Errorf("duplicate list binding for %s", pds)
			}
			targets[i].ListURI = uri
		}
	}
	return targets, nil
}

var normalizePDS = palustre.NormalizePDS

func (cfg *Config) validate() error {
	if len(cfg.Targets) == 0 && cfg.PDS != "" {
		cfg.Targets = []Target{{PDS: cfg.PDS}}
	}
	if len(cfg.Targets) == 0 || cfg.Identifier == "" || cfg.Password == "" {
		return fmt.Errorf("pds, identifier, and app-password are required (flags or CAPENSIS_ environment variables)")
	}
	cfg.Targets = slices.Clone(cfg.Targets)
	var err error
	seen := make(map[string]bool)
	for i := range cfg.Targets {
		target := &cfg.Targets[i]
		target.PDS, err = normalizePDS(target.PDS)
		if err != nil {
			return err
		}
		if seen[target.PDS] {
			return fmt.Errorf("duplicate PDS %s", target.PDS)
		}
		seen[target.PDS] = true
		if target.ListURI != "" {
			uri, err := syntax.ParseATURI(target.ListURI)
			if err != nil || uri.Collection().String() != listCollection || uri.RecordKey() == "" || !strings.HasPrefix(uri.Authority().String(), "did:") {
				return fmt.Errorf("list must be an at://DID/app.bsky.graph.list/record-key URI")
			}
		}

	}
	cfg.Slingshot, err = normalizePDS(cfg.Slingshot)
	if err != nil {
		return err
	}
	if cfg.LoginPDS != "" {
		cfg.LoginPDS, err = normalizePDS(cfg.LoginPDS)
		if err != nil {
			return err
		}
	}
	cfg.Jetstream, err = palustre.NormalizeJetstream(cfg.Jetstream)
	if err != nil {
		return err
	}
	if cfg.WriteInterval < 0 {
		return fmt.Errorf("write-interval cannot be negative")
	}
	if cfg.RescanInterval <= 0 {
		return fmt.Errorf("rescan-interval must be positive")
	}
	if cfg.Database == "" {
		return fmt.Errorf("database must be a file path")
	}
	return nil
}

func listName(pds string) string {
	endpoint, _ := url.Parse(pds)
	return "Users on " + endpoint.Hostname()
}
