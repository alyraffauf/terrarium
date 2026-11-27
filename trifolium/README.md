# Trifolium

Trifolium maintains one Bluesky moderation list per cluster. To qualify for a list, an account must follow at least the configured number of distinct accounts in that cluster. Trifolium excludes the list owner and accounts the owner follows.

## Run one cluster

Set `TRIFOLIUM_IDENTIFIER` and `TRIFOLIUM_APP_PASSWORD` for the list owner. Run Trifolium from the repository root.

```sh
go run ./cmd/trifolium --accounts=one.example,two.example --threshold=2 --database=trifolium.sqlite
```

Use handles or DIDs for watched accounts. Change the accounts or threshold and restart with the same database to update the existing list. The default threshold is 2. A threshold of 1 accepts followers of any watched account. A threshold above the number of distinct watched accounts produces an empty list and logs a warning.

The `maintaining moderation list` startup log shows each list's AT URI. Subscribe to the list in Bluesky to use its moderation actions. Run `go run ./cmd/trifolium --help` for all flags.

## Run multiple clusters

Set stable cluster IDs in the Helm `clusters` value.

```yaml
clusters:
  garden:
    accounts: [one.example, two.example, three.example]
    threshold: 2
  orchard:
    accounts: [two.example, four.example]
    threshold: 1
```

Pass the same configuration to the CLI as JSON.

```sh
go run ./cmd/trifolium --clusters='{"garden":{"accounts":["one.example","two.example","three.example"],"threshold":2},"orchard":{"accounts":["two.example","four.example"],"threshold":1}}'
```

Use the Helm `accounts` and `threshold` values or the CLI `--accounts` and `--threshold` flags to configure the `default` cluster alongside named clusters. If you define `default` in `clusters`, leave the separate `accounts` setting empty.

Each cluster ID has a stable list URI for the list owner. Removing an ID from the configuration pauses updates to its list. The published list keeps its members while paused. Adding the same ID resumes updates. Trifolium rediscovers followers of any watched account that no other cluster kept watching.

## Keep and upgrade the database

Keep `trifolium.sqlite` on persistent storage, or choose a path with `--database`. Use one process per database and a separate database for each list owner.

Trifolium applies embedded Goose migrations on startup and upgrades existing single-list and multi-cluster databases in place. The upgrade keeps list URIs, list item keys, pending writes, verified follows, scan queues, and stream cursors.

Back up the database before upgrading. Stop Trifolium before copying the database, or use SQLite's online backup command. Restore the pre-upgrade backup before running an older version.

If you supply only named clusters when upgrading a single-list database, the first successful cluster setup carries over the old watched accounts as the `default` cluster. Set its original threshold through `--threshold` or the Helm `threshold` value that first time. Include the `default` cluster explicitly on later starts to keep managing it.

## Discovery and reconciliation

Bluesky and Constellation supply follower candidates. Trifolium checks follow records directly on each account's PDS. Constellation backlinks include record keys, which allow workers to check individual records. Four workers share the verification queue, with up to two concurrent reads per PDS.

Full inventories save their cursor in SQLite after each page. Confirmed follows count immediately. An inventory removes missing follows only after its final page succeeds. Discovery runs every 24 hours by default. Discovery reads one page at a time and rotates across targets and sources. SQLite stores the next cursor and deadline for each pair, so a restart resumes the crawl. To change the interval, set `--rescan-interval` or the Helm value `rescanInterval`.

Jetstream follow creates, updates, and deletes change local eligibility. A stream gap or endpoint change schedules rediscovery and follow checks. Follower coverage depends on the two indexes, available Jetstream history, and reachable PDSes.

Trifolium reads the owner's list items at startup and after each rescan interval to reconcile changes made outside the service. Before removing an unfamiliar member from a restored list, it checks that account's follows.

Trifolium waits at least six seconds between writes across all clusters in the process. Set `--write-interval` or the Helm value `writeInterval` to change the interval.

Every five minutes, Trifolium refreshes the description of each list with up to three watched accounts, ranked by how many of their followers also follow another account in that cluster. It breaks ties by DID.

## Deploy to Kubernetes

Create a Secret named `trifolium` with an `app-password` key in the release namespace. Install the chart from the repository root.

```sh
helm upgrade --install trifolium ./trifolium/charts/trifolium \
	--set identifier=you.example.com \
	--set 'accounts[0]=one.example' \
	--set 'accounts[1]=two.example' \
	--set existingSecret=trifolium
```

The [chart values](charts/trifolium/values.yaml) cover cluster settings, endpoints, image tags, and storage. The chart runs one replica, uses the `Recreate` rollout strategy, and retains its PVC on uninstall. Set `persistence.existingClaim` to reuse a volume.
