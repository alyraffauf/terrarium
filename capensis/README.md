# Capensis

Maintain a Bluesky moderation list named `Users on <pds hostname>` for each watched PDS. Lists exclude the owner and accounts the owner follows.

Capensis sets each list's icon from the PDS's favicon at startup and every `--rescan-interval`. If no supported icon loads, it keeps the current icon.

## Run

You need Nix and an app password for the account that owns the lists. From the repository root, using Bash:

```bash
export CAPENSIS_IDENTIFIER=you.example.com
read -rsp 'Bluesky app password: ' CAPENSIS_APP_PASSWORD; echo
export CAPENSIS_APP_PASSWORD
nix run .#capensis -- first.example.com second.example.com
```

This Bash prompt hides password input. Capensis runs in the foreground until stopped. The startup log prints each list's AT URI. Subscribe to the list in Bluesky if you want its moderation actions.

Use `nix run .#capensis -- --help` for all options. Flags accept `CAPENSIS_` environment variables, with hyphens replaced by underscores. Flags override environment variables. Positional PDS arguments replace `--pds` values. For environment-only configuration, set `CAPENSIS_PDS='first.example.com,second.example.com'`.

The defaults use `https://slingshot.aly.town` and `wss://jetstream.waow.tech`. Override them with `--slingshot` and `--jetstream`.

## Keep the database and list bindings

Keep `capensis.sqlite` on persistent storage, or choose a path with `--database`. The database stores PDS-to-list bindings, pending work, and the stream cursor. Each database belongs to one list owner. Use a separate database when switching owners. Run one process per database and back it up before upgrades. Restore the matching backup if you roll back a schema upgrade.

To bind a PDS to a specific list on its first run:

```sh
nix run .#capensis -- first.example.com \
	--list 'first.example.com=at://did:plc:YOUR_DID/app.bsky.graph.list/LIST_KEY'
```

The list must belong to the owner, be named `Users on <pds hostname>`, and have moderation purpose. Capensis manages all membership in that list. Capensis exits if a saved binding points to a different list. Deleted lists are recreated at the same URI. Removing a PDS from the configuration stops its updates without deleting its list.

Capensis backfills accounts from each PDS at startup and every `--rescan-interval`, which defaults to six hours. Jetstream events trigger checks for account creation, migration, and deletion. Accounts confirmed inactive by their PDS leave the list. Owner follow and unfollow events refresh exclusions, with a five-minute fallback. Failed exclusion fetches pause membership changes until a complete snapshot succeeds.

Restarts resume the saved stream cursor. Expired cursors trigger a new backfill.

To back up the database, stop Capensis before copying it or use SQLite's online backup command.

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

For Kubernetes, create a Secret named `capensis` with an `app-password` key in the release namespace, then install:

```sh
helm upgrade --install capensis ./capensis/charts/capensis \
	--set identifier=you.example.com \
	--set 'pds[0]=first.example.com' \
	--set existingSecret=capensis
```

The [chart values](charts/capensis/values.yaml) cover endpoints, list bindings, image tags, and storage. The chart uses one replica, `Recreate` rollouts, and a PVC retained on uninstall. Set `persistence.existingClaim` to reuse a volume. Pin a `sha-<commit>` image tag for repeatable deployments.

See the [development commands](../README.md#build-and-develop).
