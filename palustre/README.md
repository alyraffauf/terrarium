# Palustre

Palustre handles AT Protocol sessions, Jetstream events, and list writes for Terrarium.

## Sessions and lists

`Session` handles authentication, token refresh, identity resolution, list icons, and write rate limits. Call its authenticated methods serially.

`EnsureList` creates a missing moderation list and recognizes legacy lists by their names or descriptions. `Reconcile` verifies existing membership and saves write intent through `MembershipStore`. Each removal checks the record's CID before deleting it. `FetchFollows` requires a records array and matching follow record URIs on every page. It skips malformed record values.

## Jetstream events

`Stream` reads the WebSocket and calls the application callback serially. A callback error closes the connection. `ClassifyFrame` reads event fields from generated Jetstream types and decodes follow records with Indigo's `GraphFollow`. Each application owns its transactions, checkpoint, and recovery.

The Jetstream lexicon in `jetstream/lexicons/` comes from Jetstream v0.4.6. To regenerate its Go types from the repository root, run `nix develop -c ./palustre/jetstream/generate.sh`. The script uses `lexgen` and the AT Protocol lexicons from the Indigo revision in `go.mod`.

## SQLite migrations

`MigrateSQLite` runs a daemon's embedded SQL and registered Go migrations with Goose. Each daemon owns its migration versions, membership rules, and queues. See the [Terrarium contributor instructions](../AGENTS.md).
