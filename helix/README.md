# Helix

Maintain a Bluesky moderation list named `Followers of <current handle>` for each target account. Lists exclude the owner and accounts the owner follows.

Helix uses the target account's profile picture as the list icon. Icons refresh at startup and every `--rescan-interval`. Missing or unsupported images leave the existing icon unchanged.

## Run

From the repository root, using Bash:

```bash
export HELIX_IDENTIFIER=you.example.com
read -rsp 'Bluesky app password: ' HELIX_APP_PASSWORD; echo
export HELIX_APP_PASSWORD
nix run .#helix -- first.example.com second.example.com
```

This Bash prompt hides password input. Targets accept handles or DIDs. Helix runs in the foreground until stopped. The startup log prints each list's AT URI. Subscribe to the list in Bluesky if you want its moderation actions.

Use `nix run .#helix -- --help` for all options. Flags accept `HELIX_` environment variables, with hyphens replaced by underscores. Flags override environment variables. Positional accounts replace `--account` and `HELIX_ACCOUNT` values. For environment-only configuration, set `HELIX_ACCOUNT='first.example.com,second.example.com'`.

The defaults use `https://slingshot.aly.town`, `wss://jetstream.waow.tech`, and `https://public.api.bsky.app`. Override them with `--slingshot`, `--jetstream`, and `--appview`.

## Keep the database and list bindings

Keep `helix.sqlite` on persistent storage, or choose a path with `--database`. The database stores target-DID-to-list bindings, pending work, and the stream cursor. Each database belongs to one list owner. Use separate databases for different owners and for Capensis. Run one process per database. Back up the database before upgrades.

Each list stays bound to a target DID. Helix resolves configured handles at startup. Use DIDs to keep tracking the same account if a handle is reassigned before a restart. List names follow the current handle at startup and every `--rescan-interval`. Long handles are truncated to fit the name limit.

To bind an account to a specific list on its first run:

```sh
nix run .#helix -- first.example.com \
	--list 'first.example.com=at://did:plc:YOUR_DID/app.bsky.graph.list/LIST_KEY'
```

An existing list must belong to the owner, be named `Followers of <current handle>`, and have moderation purpose. Helix manages all membership in that list. Conflicting saved bindings fail. Deleted lists are recreated at the same URI. Removing a target from configuration stops its updates without deleting the list or its saved binding.

Helix backfills followers from the AppView at startup and every `--rescan-interval`, which defaults to six hours. Jetstream follow and unfollow events trigger checks against candidates' current PDSes. Account events recheck known candidates. Accounts confirmed inactive leave the lists.

Owner follow and unfollow events refresh exclusions, with a five-minute fallback. Failed exclusion fetches pause membership changes until a complete snapshot succeeds. Restarts resume the saved stream cursor, and periodic backfills recover missed changes. Backfill completeness depends on the AppView's follower index.

Helix checks up to eight accounts concurrently and stops reading a candidate's follows once all target accounts are found. List writes remain serialized. Once a minute, `membership progress` logs the pending count and process totals for completed checks, additions, removals, and retries. Pending includes in-flight checks and delayed retries.

To back up the database, stop Helix before copying it or use SQLite's online backup command.

Successful deletions log `list item removed` with a timestamp, account DID, target, list URI, and record key. The `reason` distinguishes `not_eligible` from `duplicate` cleanup. Keep these logs to retain removal history; they do not recover past removals.

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

For Kubernetes, create a Secret named `helix` with an `app-password` key in the release namespace, then install:

```sh
helm upgrade --install helix ./helix/charts/helix \
	--set identifier=you.example.com \
	--set 'accounts[0]=first.example.com' \
	--set existingSecret=helix
```

The [chart values](charts/helix/values.yaml) cover endpoints, list bindings, image tags, and storage. The chart uses one replica, `Recreate` rollouts, and a PVC retained on uninstall. Set `persistence.existingClaim` to reuse a volume. Pin a `sha-<commit>` image tag for repeatable deployments.

See the [development commands](../README.md#build-and-develop).
