# Trifolium

Trifolium maintains one subscribable moderation list for its owner. A member must follow at least the configured number of distinct watched accounts. The owner and everyone the owner follows are excluded. SQLite retains the underlying follows even when a person is excluded or below threshold.

Set `TRIFOLIUM_IDENTIFIER` and `TRIFOLIUM_APP_PASSWORD` and run:

```sh
go run ./cmd/trifolium --accounts=one.example,two.example --threshold=2 --database=/data/trifolium.sqlite
```

Change the arguments and restart with the same database to change the targets or threshold. Membership is recalculated on the same list. A threshold of 1 includes followers of any target. A threshold above the number of targets produces an empty list. The default is 2.

Bluesky and Constellation both supply follower candidates. Trifolium reads their follow records from their PDS and saves matching record keys locally. An incomplete scan is retried and preserves the previous inventory. A follower missing from both indexes can still be discovered by Jetstream. Coverage depends on the indexes, stream history and reachable PDSes.

Jetstream follow creates, updates and deletes change local eligibility. The publisher retries list writes independently, with a default minimum interval of six seconds. Keep the SQLite volume across restarts. A Helm chart in this directory provides a single replica and persistent volume.

Every five minutes the publisher ranks watched accounts by the number of distinct followers who also follow another watched account. The list description names up to three in descending order, breaking ties by DID. Until there is overlap, it reads “Accounts with overlapping follows.”

Four verification workers share the queue, with at most two concurrent verification reads per PDS. Constellation backlinks carry record keys, so workers can verify individual follows directly. Full inventories yield after each page and save their cursor in SQLite. Confirmed follows count immediately; a full inventory removes missing follows only when its final page succeeds.

### Multiple clusters

Keep the existing Helm `accounts` and `threshold` values for Mellow Laurel. Add stable cluster IDs alongside them:

```yaml
clusters:
  garden:
    accounts: [one.example, two.example, three.example]
    threshold: 2
  orchard:
    accounts: [two.example, four.example]
    threshold: 1
```

The CLI accepts the equivalent JSON object via `--clusters`. Each ID owns one moderation list and its own overlap ranking. Editing a threshold or a target set keeps that list's URI. All clusters share verified follows, owner exclusions, discovery workers and the write interval. Removing a named cluster from configuration pauses management of its list. Reusing its ID resumes it.

On an existing volume, the first multi-list startup migrates Mellow Laurel to the reserved ID `default` in one transaction. Its list URI, list item keys, pending writes, verified follows, queued scans and stream cursor carry forward. The original flags still work. If only new clusters are supplied on upgrade, Trifolium retains the existing watched accounts as the default cluster and uses `--threshold` (2 by default). For a legacy installation with a custom threshold, keep supplying it on that first upgrade. Subsequent starts retain the saved default threshold.
