package helix

import (
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"github.com/alyraffauf/terrarium/palustre"
)

type Config struct {
	MetricsAddress                                                          string
	AppView, Identifier, Password, LoginPDS, Slingshot, Jetstream, Database string
	Targets                                                                 []Target
	WriteInterval                                                           time.Duration
	RescanInterval                                                          time.Duration
}

type Target struct {
	Actor   string
	ListURI string
}

func ParseTargets(accounts, bindings []string) ([]Target, error) {
	var targets []Target
	indexes := make(map[string]int)
	for _, value := range accounts {
		for _, entry := range strings.Split(value, ",") {
			if strings.TrimSpace(entry) == "" {
				continue
			}
			actor, err := normalizeActor(strings.TrimSpace(entry))
			if err != nil {
				return nil, err
			}
			if _, exists := indexes[actor]; exists {
				return nil, fmt.Errorf("duplicate account %s", actor)
			}
			indexes[actor] = len(targets)
			targets = append(targets, Target{Actor: actor})
		}
	}
	for _, value := range bindings {
		for _, entry := range strings.Split(value, ",") {
			actor, uri, ok := strings.Cut(strings.TrimSpace(entry), "=")
			actor, uri = strings.TrimSpace(actor), strings.TrimSpace(uri)
			if !ok || uri == "" {
				return nil, fmt.Errorf("--list expects ACCOUNT=at://DID/app.bsky.graph.list/record-key")
			}
			actor, err := normalizeActor(actor)
			if err != nil {
				return nil, err
			}
			i, exists := indexes[actor]
			if !exists {
				return nil, fmt.Errorf("list binding refers to unwatched account %s", actor)
			}
			if targets[i].ListURI != "" {
				return nil, fmt.Errorf("duplicate list binding for %s", actor)
			}
			targets[i].ListURI = uri
		}
	}
	return targets, nil
}

var normalizePDS = palustre.NormalizePDS

func (cfg *Config) validate() error {
	if len(cfg.Targets) == 0 || cfg.Identifier == "" || cfg.Password == "" {
		return fmt.Errorf("account, identifier, and app-password are required (flags or HELIX_ environment variables)")
	}
	var err error
	seen := make(map[string]bool)
	for i := range cfg.Targets {
		target := &cfg.Targets[i]
		target.Actor, err = normalizeActor(target.Actor)
		if err != nil {
			return err
		}
		if seen[target.Actor] {
			return fmt.Errorf("duplicate account %s", target.Actor)
		}
		seen[target.Actor] = true
		if target.ListURI != "" {
			uri, err := syntax.ParseATURI(target.ListURI)
			if err != nil || uri.Collection().String() != listCollection || uri.RecordKey() == "" || !strings.HasPrefix(uri.Authority().String(), "did:") {
				return fmt.Errorf("list must be an at://DID/app.bsky.graph.list/record-key URI")
			}
		}

	}
	cfg.AppView, err = normalizePDS(cfg.AppView)
	if err != nil {
		return err
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

func listName(actor string) string { return "Followers of " + actor }
func normalizeActor(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "did:") {
		did, err := syntax.ParseDID(raw)
		return did.String(), err
	}
	handle, err := syntax.ParseHandle(strings.TrimPrefix(strings.ToLower(raw), "@"))
	return handle.String(), err
}
