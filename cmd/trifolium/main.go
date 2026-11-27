package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/gofrs/flock"
)

const nextChange = `
 WITH eligible AS (SELECT actor AS did FROM counts WHERE total>=?),
 changes AS (
 SELECT did FROM (SELECT did FROM published EXCEPT SELECT did FROM eligible)
 UNION ALL SELECT did FROM (SELECT did FROM eligible EXCEPT SELECT did FROM published)
 UNION ALL SELECT did FROM pending)
 SELECT did,did IN (SELECT did FROM eligible) AS include FROM changes
 WHERE did NOT IN (SELECT did FROM retries WHERE due>unixepoch())
 AND NOT EXISTS (SELECT 1 FROM scans WHERE did=?)
 ORDER BY include,did LIMIT 1`

func (a *service) publish(ctx context.Context) error {
	var did string
	var include bool
	err := a.QueryRowContext(ctx, nextChange, a.threshold, a.owner).Scan(&did, &include)
	if err != nil {
		return err
	}
	if _, err = a.ExecContext(ctx, "INSERT OR IGNORE INTO pending VALUES(?)", did); err != nil {
		return err
	}
	if err = a.api.Refresh(ctx); err == nil {
		err = a.api.Reconcile(ctx, a.store, palustre.Membership{Target: a.list, ListURI: a.list, DID: did, Include: include})
	}
	if err != nil {
		_, retryErr := a.ExecContext(ctx, "INSERT OR REPLACE INTO retries VALUES(?,unixepoch()+60)", did)
		return errors.Join(err, retryErr)
	}
	return a.transaction(ctx, func(tx *sql.Tx) error {
		query := "DELETE FROM published WHERE did=?"
		if include {
			query = "INSERT OR IGNORE INTO published VALUES(?)"
		}
		if _, err := tx.Exec(query, did); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM retries WHERE did=?", did); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM pending WHERE did=?", did)
		return err
	})
}
func work(ctx context.Context, name string, next func(context.Context) error) {
	for ctx.Err() == nil {
		if err := next(ctx); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Warn(name, "error", err)
			}
			palustre.Pause(ctx, time.Second)
		}
	}
}
func run(ctx context.Context) error {
	accounts := flag.String("accounts", "", "comma-separated watched handles or DIDs")
	threshold := flag.Int("threshold", 2, "minimum distinct watched accounts followed")
	interval := flag.Duration("rescan-interval", 24*time.Hour, "interval between full discovery passes")
	path := flag.String("database", "trifolium.sqlite", "SQLite state file")
	endpoint := flag.String("jetstream", "wss://jetstream.waow.tech", "Jetstream v2 endpoint")
	identifier := flag.String("identifier", os.Getenv("TRIFOLIUM_IDENTIFIER"), "list owner")
	slingshot := flag.String("slingshot", "https://slingshot.aly.town", "identity resolver")
	pace := flag.Duration("write-interval", palustre.DefaultFollowerWriteInterval, "minimum interval between list writes")
	flag.Parse()
	password := os.Getenv("TRIFOLIUM_APP_PASSWORD")
	if *threshold < 1 || *interval <= 0 || *pace <= 0 || *identifier == "" || password == "" || strings.TrimSpace(*accounts) == "" {
		return errors.New("set accounts, identifier, TRIFOLIUM_APP_PASSWORD, and positive threshold and intervals")
	}
	stream, err := palustre.NormalizeJetstream(*endpoint)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	api := &palustre.Session{HTTP: client, Config: palustre.Config{Identifier: *identifier, Password: password, Slingshot: *slingshot, WriteInterval: *pace}}
	targets := map[string]bool{}
	for _, account := range strings.Split(*accounts, ",") {
		doc, err := api.Resolve(ctx, strings.TrimSpace(account))
		if err != nil {
			return err
		}
		targets[doc.DID] = true
	}
	lock := flock.New(*path + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("database already in use")
	}
	defer lock.Close()
	db, err := sql.Open("sqlite", *path)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, schema); err != nil {
		return err
	}
	if err = api.Login(ctx); err != nil {
		return err
	}
	owner := api.Client.Auth.Did
	// Keep the list URI stable across service renames.
	list := fmt.Sprintf("at://%s/%s/%s", owner, palustre.ListCollection, palustre.RecordKey("crossover:"+owner))
	a := service{wakeBackfill: make(chan struct{}, 1), store: store{db}, api: api, owner: owner, list: list, stream: stream, targets: targets, threshold: *threshold, interval: *interval, http: client}
	if err = a.configure(ctx, owner, targets); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, dirty, owner); err != nil {
		return err
	}
	if err = a.refreshDescription(ctx); err != nil {
		return err
	}
	slog.Info("maintaining moderation list", "list", list, "threshold", *threshold)
	var wg sync.WaitGroup
	wg.Go(func() { a.listen(ctx) })
	wg.Go(func() { a.backfill(ctx) })
	for i := range 4 {
		wg.Go(func() {
			work(ctx, "follow verification", func(ctx context.Context) error { return a.verify(ctx, i != 0) })
		})
	}
	wg.Go(func() { work(ctx, "list reconciliation", a.publishWithDescription) })
	wg.Wait()
	return nil
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		slog.Error("trifolium", "error", err)
		os.Exit(1)
	}
}
