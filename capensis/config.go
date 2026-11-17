package capensis

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"github.com/alyraffauf/terrarium/palustre"
)

type Config struct {
	MetricsAddress                                                      string
	PDS, Identifier, Password, LoginPDS, Slingshot, Jetstream, Database string
	Targets                                                             []Target
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
		targetCfg := *cfg
		targetCfg.PDS = target.PDS
		if len(targetCfg.listName()) > 64 {
			return fmt.Errorf("list name %q exceeds the lexicon's 64-byte limit", targetCfg.listName())
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
	cfg.Jetstream = strings.Replace(strings.Replace(cfg.Jetstream, "https://", "wss://", 1), "http://", "ws://", 1)
	stream, err := url.Parse(cfg.Jetstream)
	if err != nil || stream.Host == "" || (stream.Scheme != "ws" && stream.Scheme != "wss") || stream.User != nil {
		return fmt.Errorf("jetstream must be a ws:// or wss:// URL")
	}
	if stream.Path == "" || stream.Path == "/" {
		stream.Path = streamPath
	}
	if stream.Path != streamPath || stream.RawQuery != "" || stream.Fragment != "" {
		return fmt.Errorf("jetstream must be a base URL or the v2 subscribeEvents URL without query parameters")
	}
	cfg.Jetstream = stream.String()
	if cfg.RescanInterval <= 0 {
		return fmt.Errorf("rescan-interval must be positive")
	}
	if cfg.Database == "" {
		return fmt.Errorf("database must be a file path")
	}
	return nil
}

func (cfg Config) listName() string {
	return listName(cfg.PDS)
}

func listName(pds string) string {
	endpoint, _ := url.Parse(pds)
	return "Users on " + endpoint.Hostname()
}
