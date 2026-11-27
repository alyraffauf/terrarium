# Helix

Helix maintains a Bluesky moderation list for each watched account. Each list has a title derived from its AT URI and the description `Followers of <current handle>`. Lists exclude the owner and accounts the owner follows.

Helix uses the target account's profile picture as the list icon. Icons refresh at startup and every `--rescan-interval`. Missing or unsupported images leave the existing icon unchanged. Profile-picture URLs and redirects must resolve to public addresses. Icon requests bypass HTTP proxies.

## Run

You need Nix and an app password for the list owner. Run these commands from the repository root in Bash.

```bash
export HELIX_IDENTIFIER=you.example.com
read -rsp 'Bluesky app password: ' HELIX_APP_PASSWORD; echo
export HELIX_APP_PASSWORD
nix run .#helix -- first.example.com second.example.com
```

This Bash prompt hides password input. Targets accept handles or DIDs. Helix runs in the foreground until stopped. The startup log prints each list's AT URI. Subscribe to the list in Bluesky to use its moderation actions.

Use `nix run .#helix -- --help` for all options. To set a flag through the environment, uppercase its name, replace hyphens with underscores, and add the prefix `HELIX_`. Flags override environment variables. Positional accounts replace `--account` and `HELIX_ACCOUNT` values. For environment-only configuration, set `HELIX_ACCOUNT='first.example.com,second.example.com'`.

The defaults use `https://slingshot.aly.town`, `wss://jetstream.waow.tech`, and `https://public.api.bsky.app`. Override them with `--slingshot`, `--jetstream`, and `--appview`.

## Keep the database and list bindings

Keep `helix.sqlite` on persistent storage, or choose a path with `--database`. The database stores target-DID-to-list bindings, pending work, and the stream cursor. Each database belongs to one list owner. Use separate databases for different owners and for Capensis. Run one process per database.

Each list stays bound to a target DID. Helix resolves configured handles at startup. Use DIDs to keep tracking the same account if a handle is reassigned before a restart. The description follows handle changes at startup and every `--rescan-interval`.

Helix applies embedded Goose migrations on startup. Back up the database before upgrading. Stop Helix before copying the database, or use SQLite's online backup command. Restore the pre-upgrade backup before running an older version.

## Bind an existing list

Use `--list` to choose a list when you first add a target.

```sh
nix run .#helix -- first.example.com \
	--list 'first.example.com=at://did:plc:YOUR_DID/app.bsky.graph.list/LIST_KEY'
```

An existing list must belong to the owner, have the managed title and description or the legacy name `Followers of <current handle>`, and have moderation purpose. Helix saves a new binding after it verifies or creates the list. Conflicting saved bindings stop startup. If a target is suspended or its list check fails, Helix pauses writes to that list and retries the check after one minute. Other lists keep syncing. A successful check after a pause triggers a new follower backfill and list-item scan. Helix recreates deleted lists at the same URI. Removing a target from configuration stops its updates without deleting the list or its saved binding.

## Membership updates

Helix backfills followers from the AppView at startup and every `--rescan-interval`, which defaults to six hours. Jetstream follow and unfollow events trigger checks against candidates' current PDSes. Account events recheck known candidates. Accounts confirmed inactive leave the lists.

Owner follow and unfollow events refresh exclusions, with a five-minute fallback. Failed exclusion fetches pause membership changes until a complete snapshot succeeds. Restarts resume the saved stream cursor. A stream gap triggers a new backfill. Backfill coverage depends on the AppView's follower index.

Helix checks up to eight accounts concurrently and stops reading a candidate's follows once all target accounts are found. List writes remain serialized. Once a minute, `membership progress` logs the pending count and process totals for completed checks, additions, removals, and retries. Pending includes in-flight checks and delayed retries.

Successful writes log `list item added` or `list item removed` with a timestamp, account DID, target, list URI, and record key. Removal `reason` distinguishes `not_eligible` from `duplicate` cleanup. Keep these logs to retain membership history.

By default, Helix waits at least 6 seconds between writes across all its lists. Set `--write-interval`, `HELIX_WRITE_INTERVAL`, or the Helm value `writeInterval` to change the interval. Each process has its own timer. Running one Helix and one Capensis for the same owner allows up to about 16 writes per minute combined at the defaults.

## Metrics

Set `--metrics-address=:9090` to serve Prometheus metrics at `/metrics`. The listener is disabled by default and has no authentication. Bind to `127.0.0.1:9090` for local-only access.

The endpoint exports `terrarium_pending_accounts`, `terrarium_checks_completed_total`, `terrarium_check_retries_total`, `terrarium_members_added_total`, and `terrarium_members_removed_total`. Counters reset on restart. Removals include duplicate cleanup. Metrics contain no account identifiers.

For Helm, set `metrics.enabled=true`. The chart adds a metrics Service and pod scrape annotations on port 9090. Configure your scraper to discover those annotations or the Service. Metrics remain available while membership checks wait on remote servers.

## Deploy

Set `HELIX_IDENTIFIER` and `HELIX_APP_PASSWORD` in your shell first.

```sh
docker build -f helix/Dockerfile -t helix .
docker run --rm \
	-e HELIX_IDENTIFIER -e HELIX_APP_PASSWORD \
	-v helix-data:/data \
	helix first.example.com second.example.com
```

If you mount a host directory at `/data`, make it writable by UID 10001.

For Kubernetes, create a Secret named `helix` with an `app-password` key in the release namespace. Install the chart from the repository root.

```sh
helm upgrade --install helix ./helix/charts/helix \
	--set identifier=you.example.com \
	--set 'accounts[0]=first.example.com' \
	--set existingSecret=helix
```

The [chart values](charts/helix/values.yaml) cover endpoints, list bindings, image tags, and storage. The chart uses one replica, `Recreate` rollouts, and a PVC retained on uninstall. Set `persistence.existingClaim` to reuse a volume. Pin a `sha-<commit>` image tag for repeatable deployments.

See the [build and test commands](../README.md#build-and-test).
