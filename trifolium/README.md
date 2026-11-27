# Trifolium

Trifolium maintains one Bluesky moderation list per follower cluster. To qualify for a list, an account must follow the configured number of distinct watched accounts in that cluster. Trifolium excludes the list owner and accounts the owner follows.

## Run one cluster

Set `TRIFOLIUM_IDENTIFIER` and `TRIFOLIUM_APP_PASSWORD`, then run:

```sh
go run ./cmd/trifolium --accounts=one.example,two.example --threshold=2 --database=trifolium.sqlite
```

Change the targets or threshold and restart with the same database to update the existing list. The default threshold is 2. A threshold of 1 accepts followers of any watched account. A threshold above the number of distinct targets produces an empty list and logs a warning.

Keep the SQLite file on persistent storage. The Helm chart runs one replica and provides a persistent volume. Set `--write-interval` or the Helm value `writeInterval` to change the minimum time between writes. The default is six seconds across all clusters in the process.

## Run multiple clusters

Set stable cluster IDs in the Helm `clusters` value:

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

You can also set the Helm `accounts` and `threshold` values or the CLI `--accounts` and `--threshold` flags to manage the `default` cluster. Each ID has a stable list URI. Removing an ID from configuration pauses its list. The published list keeps its members while paused. Adding the same ID resumes management. Targets used by no other cluster need a fresh crawl.

The first upgrade from the single-list SQLite schema keeps the original watched accounts as the `default` cluster if you supply only named clusters. The old schema did not store the threshold, so pass the original value through `--threshold` or the Helm `threshold` value on that upgrade. On later starts, include the `default` cluster in the configuration to keep managing it. The migration preserves the original list URI, list item keys, pending writes, verified follows, scan queue, and stream cursor.

## Discovery and reconciliation

Bluesky and Constellation supply follower candidates. Trifolium checks follow records directly on each account's PDS. Constellation backlinks include record keys, which allow workers to check individual records. Four workers share the verification queue, with up to two concurrent reads per PDS.

Full inventories save their cursor in SQLite after each page. Confirmed follows count immediately. An inventory removes missing follows only after its final page succeeds. Discovery runs every 24 hours by default. Each source has its own deadline, which survives restarts. To change the interval, set `--rescan-interval` or the Helm value `rescanInterval`.

Jetstream follow creates, updates, and deletes change local eligibility. A stream gap or endpoint change schedules rediscovery and follow checks. Follower coverage depends on the two indexes, available Jetstream history, and reachable PDSes.

Trifolium reads the owner's list items at startup and periodically afterward to reconcile items that were added, removed, or duplicated outside the service. Before removing an unfamiliar member from a restored list, it checks that account's follows.

Every five minutes, Trifolium refreshes the description of each list with up to three watched accounts, ranked by how many of their followers also follow another account in that cluster. It breaks ties by DID.
