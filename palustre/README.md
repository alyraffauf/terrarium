# Palustre

Shared authentication, identity resolution, Jetstream reads, and list writes for Terrarium.

`Session` owns authentication, token refresh, identity resolution, list icons, and the write limiter. Call its authenticated methods serially. `Stream` calls the application callback serially and stops on a callback error. Applications own transactions and checkpoints.

`EnsureList` creates missing moderation lists and accepts renames from explicitly supplied previous names. It refuses other name or purpose mismatches. `Reconcile` verifies existing membership, uses CID preconditions for removals, and saves write intent through `MembershipStore`. `FetchFollows` reads every page and skips malformed records.

Applications own membership rules, queues, and migrations. Generated list types live in `listlex/`. See the [generation and test commands](../README.md#build-and-develop).
