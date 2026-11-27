# Palustre

Shared authentication, identity resolution, Jetstream reads, and list writes for Terrarium.

`Session` owns authentication, token refresh, identity resolution, list icons, and the write limiter. Call its authenticated methods serially. `Stream` calls the application callback serially and stops on a callback error. Applications own transactions and checkpoints.

`EnsureList` creates missing moderation lists and accepts renames from explicitly supplied previous names. It refuses other name or purpose mismatches. `Reconcile` verifies existing membership, uses CID preconditions for removals, and saves write intent through `MembershipStore`. `FetchFollows` requires a records array and matching follow record URIs on every page. It skips malformed record values.

Applications own membership rules, queues, and migrations. Palustre uses the generated list and follow types from the pinned Indigo module. See the [build and test commands](../README.md#build-and-develop).
