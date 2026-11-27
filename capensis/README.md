# Capensis

Capensis maintains a Bluesky moderation list for each watched PDS (personal data server). Each list has a title derived from its AT URI and the description `Users on <pds hostname>`. Lists exclude the owner and accounts the owner follows.

Capensis sets each list's icon from the PDS's favicon at startup and every `--rescan-interval`. If no supported icon loads, it keeps the current icon. Icon links and redirects must resolve to public addresses, except for the configured PDS host. Icon requests bypass HTTP proxies.

## Run

You need Nix and an app password for the list owner. Run these commands from the repository root in Bash.

```bash
export CAPENSIS_IDENTIFIER=you.example.com
read -rsp 'Bluesky app password: ' CAPENSIS_APP_PASSWORD; echo
export CAPENSIS_APP_PASSWORD
nix run .#capensis -- first.example.com second.example.com
```

This Bash prompt hides password input. Capensis runs in the foreground until stopped. The startup log prints each list's AT URI. Subscribe to the list in Bluesky to use its moderation actions.

Use `nix run .#capensis -- --help` for all options. To set a flag through the environment, uppercase its name, replace hyphens with underscores, and add the prefix `CAPENSIS_`. Flags override environment variables. Positional PDS arguments replace `--pds` and `CAPENSIS_PDS` values. For environment-only configuration, set `CAPENSIS_PDS='first.example.com,second.example.com'`.

The defaults use `https://slingshot.aly.town` and `wss://jetstream.waow.tech`. Override them with `--slingshot` and `--jetstream`.

## Keep the database and list bindings

Keep `capensis.sqlite` on persistent storage, or choose a path with `--database`. The database stores PDS-to-list bindings, pending work, and the stream cursor. Each database belongs to one list owner. Use a separate database when switching owners. Run one process per database.

Capensis applies embedded Goose migrations on startup. Back up the database before upgrading. Stop Capensis before copying the database, or use SQLite's online backup command. Restore the pre-upgrade backup before running an older version.

When an upgrade normalizes saved PDS endpoints, Capensis keeps their list URIs. The migration stops if two entries for the same normalized endpoint have different list URIs or conflicting member record keys.

## Bind an existing list

Use `--list` to choose a list when you first add a target.

```sh
nix run .#capensis -- first.example.com \
	--list 'first.example.com=at://did:plc:YOUR_DID/app.bsky.graph.list/LIST_KEY'
```

The list must belong to the owner, have the managed title and description or the legacy name `Users on <pds hostname>`, and have moderation purpose. Capensis saves a new binding after it verifies or creates the list. Capensis exits if a saved binding points to a different list. If a list check fails, Capensis pauses writes to that list and retries the check after one minute. Other lists keep syncing. A successful check after a pause triggers a new PDS backfill and list-item scan. Capensis recreates deleted lists at the same URI. Removing a PDS from the configuration stops its updates without deleting its list.

## Membership updates

Capensis backfills accounts from each PDS at startup and every `--rescan-interval`, which defaults to six hours. Jetstream account, identity, and sync events queue accounts for checks. Accounts confirmed inactive by their PDS leave the list. Owner follow and unfollow events refresh exclusions, with a five-minute fallback. Failed exclusion fetches pause membership changes until a complete snapshot succeeds.

Restarts resume the saved stream cursor. A stream gap triggers a new backfill.

Successful writes log `list item added` or `list item removed` with a timestamp, account DID, target, list URI, and record key. Removal `reason` distinguishes `not_eligible` from `duplicate` cleanup. Keep these logs to retain membership history.

By default, Capensis waits at least 10 seconds between writes across all its lists. Set `--write-interval`, `CAPENSIS_WRITE_INTERVAL`, or the Helm value `writeInterval` to change the interval. Each process has its own timer. Running one Helix and one Capensis for the same owner allows up to about 16 writes per minute combined at the defaults.

## Metrics

Set `--metrics-address=:9090` to serve Prometheus metrics at `/metrics`. The listener is disabled by default and has no authentication. Bind to `127.0.0.1:9090` for local-only access.

The endpoint exports `terrarium_pending_accounts`, `terrarium_checks_completed_total`, `terrarium_check_retries_total`, `terrarium_members_added_total`, and `terrarium_members_removed_total`. Counters reset on restart. Removals include duplicate cleanup. Metrics contain no account identifiers.

For Helm, set `metrics.enabled=true`. The chart adds a metrics Service and pod scrape annotations on port 9090. Configure your scraper to discover those annotations or the Service. Metrics remain available while membership checks wait on remote servers.

## Deploy

Set `CAPENSIS_IDENTIFIER` and `CAPENSIS_APP_PASSWORD` in your shell first.

```sh
docker build -f capensis/Dockerfile -t capensis .
docker run --rm \
	-e CAPENSIS_IDENTIFIER -e CAPENSIS_APP_PASSWORD \
	-v capensis-data:/data \
	capensis first.example.com second.example.com
```

If you mount a host directory at `/data`, make it writable by UID 10001.

For Kubernetes, create a Secret named `capensis` with an `app-password` key in the release namespace. Install the chart from the repository root.

```sh
helm upgrade --install capensis ./capensis/charts/capensis \
	--set identifier=you.example.com \
	--set 'pds[0]=first.example.com' \
	--set existingSecret=capensis
```

The [chart values](charts/capensis/values.yaml) cover endpoints, list bindings, image tags, and storage. The chart uses one replica, `Recreate` rollouts, and a PVC retained on uninstall. Set `persistence.existingClaim` to reuse a volume. Pin a `sha-<commit>` image tag for repeatable deployments.

See the [build and test commands](../README.md#build-and-test).
