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
