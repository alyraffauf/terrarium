# Working on Terrarium

## Commits

Use Conventional Commits with an imperative subject. Use a scope when it identifies the affected daemon or shared package, as in `fix(trifolium): preserve members after an interrupted scan`.

Keep each commit independently buildable and revertible. Include the regression test with its fix and generated files with their source changes. Split unrelated behavior changes into separate commits.

## SQLite migrations

Add a new Goose version for each schema change. Use the next version across the application's SQL and Go migrations. Put SQL files in the application's `storage/migrations/` directory. Register Go migrations with `palustre.MigrateSQLite` and use `goose.GoFunc.RunTx`.

Test upgrades from the database versions affected by the change. For changes to tables read by sqlc, regenerate the Capensis or Helix queries using its `scripts/generate-db.sh`.

## Generated clients

After changing the Jetstream or Microcosm lexicons, run the matching `generate.sh` under `palustre/jetstream/` or `palustre/microcosm/`. Both scripts use the Indigo revision in `go.mod`.

## Property tests and fuzzing

Use native Go fuzz targets for behavior that depends on combinations of inputs or sequences of events. For membership and queue logic, build a small independent model using sets or maps. Replay the same operations against the model and the implementation. Compare their observable state and the records each writes or removes.

Generate histories that exercise the behavior being changed. Useful cases include duplicate follows, owner exclusions, threshold changes, interrupted inventories, stale pages, retries, and restarts. Use temporary SQLite databases for persistence tests and local fakes for network calls. Bound the length of each history so the runner can explore many examples and minimize failures.

- Run `go test ./...` to replay the committed seeds and deterministic regressions.
- Run `FUZZTIME=10s FUZZWORKERS=2 ./scripts/fuzz.sh` for a bounded campaign across all targets. The time budget applies to each target.
- To investigate one property, run a focused campaign such as `go test ./cmd/trifolium -run '^$' -fuzz '^FuzzFollowReplay$' -fuzztime=30s -parallel=2`.
- When a campaign finds a defect, preserve the minimized input in `testdata/fuzz` or turn it into a readable deterministic regression. Verify that the case fails before the fix and passes afterward.
- When introducing a model, check that it detects a plausible temporary fault, such as counting a duplicate follow twice. Revert the fault before committing.
