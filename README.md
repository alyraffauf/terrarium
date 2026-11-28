# Terrarium

Terrarium keeps Bluesky moderation lists up to date. Give it a post, an account, a group of accounts, or a personal data server (PDS). The daemon publishes a list in your account’s repository and updates its members as the underlying records change. People can subscribe to the list in Bluesky.

## Choose a daemon

| If you want to list… | Run | What you get |
| --- | --- | --- |
| Everyone who liked a particular post | [Cordata](cordata/README.md) | One list for the post. Cordata checks for new likes and removes accounts whose likes disappear. |
| Followers of individual accounts | [Helix](helix/README.md) | A separate list for each target. |
| Accounts that follow several people in a group | [Trifolium](trifolium/README.md) | One list per group. You choose how many distinct targets an account must follow to qualify. |
| Accounts hosted on particular servers | [Capensis](capensis/README.md) | A separate list for each PDS. |

For example, give Trifolium three accounts and a threshold of two. Someone who follows any two qualifies. If they unfollow one and only one remains, Trifolium removes them. Helix, Trifolium, and Capensis exclude you and anyone you follow. Cordata counts every liker, including you.

Each list keeps its AT URI across restarts. Its title comes from 1,024 adjective-and-noun combinations, such as `Quiet Meadow`. The description tells subscribers what the list tracks. Publishing a list and subscribing to its moderation actions are separate choices.

## Run a list

From the repository root, inspect a daemon’s options without credentials.

```sh
nix run .#helix -- --help
```

To run Helix, set `HELIX_IDENTIFIER` and `HELIX_APP_PASSWORD` for the account that will own the lists. Pass each target as a handle or DID.

```sh
nix run .#helix -- first.example.com second.example.com
```

Helix prints the list URIs at startup. Keep its SQLite file when you restart. Capensis and Trifolium also use SQLite. Cordata uses the owner’s repository for persistence. Each daemon guide above includes its Helm configuration. Container images are published at `ghcr.io/alyraffauf/terrarium/<daemon>` with `latest` and `sha-<commit>` tags.

## Measure verification cost

All four daemons can serve Prometheus metrics at `/metrics`. Set `--metrics-address=:9090` and configure your scraper. The examples assume a scrape target label named `daemon`.

This query shows the 95th percentile duration in seconds by daemon and XRPC method. Each observation covers one candidate PDS request through response decoding, including local connection waits. The `result` label records whether the RPC returned an error. Discovery and list writes use separate clients.

```promql
histogram_quantile(0.95, sum by (daemon, method, le) (
  rate(terrarium_pds_verification_read_duration_seconds_bucket[10m])
))
```

To measure pagination work, divide attempted follow pages by completed follow inventories over the same window.

```promql
sum by (daemon) (rate(terrarium_pds_verification_read_duration_seconds_count{method="com.atproto.repo.listRecords"}[10m]))
/
(sum by (daemon) (rate(terrarium_follow_inventories_completed_total[10m])) > 0)
```

The numerator includes failed page reads and pages from inventories still in progress. Helix counts an inventory when the candidate’s follow scan reaches its last page or finds all requested targets. Trifolium counts a current inventory after committing its final page or confirming the repository is inactive. Single-record checks and account status lookups contribute to their own latency histograms. Capensis and Cordata perform those checks without walking follow inventories.

## Build and test

The four daemons share one Go module. [Palustre](palustre/README.md) holds their AT Protocol clients, publication logic, and SQLite setup. Each daemon owns its membership rules and deployment files.

```sh
nix build .#capensis .#cordata .#helix .#trifolium
nix develop -c go test ./...
nix develop -c go vet ./...
nix develop -c ./scripts/test-charts.sh
FUZZTIME=10s FUZZWORKERS=2 nix develop -c ./scripts/fuzz.sh
```

Ordinary Go tests replay the checked-in fuzz seeds. The fuzz script generates new cases for each target. Give `FUZZTIME` a duration per target or an execution count such as `1000x`. See the [contributor guide](AGENTS.md) for regression tests and commit conventions.

## Change a schema

Capensis, Helix, and Trifolium use Goose for migrations. Keep SQL migrations in the owning daemon’s `storage/migrations/` directory. Register transactional Go migrations alongside them when an upgrade needs to inspect existing data. Capensis and Helix generate their query code with sqlc. Palustre uses Indigo’s AT Protocol and Bluesky types and generates clients and types for the Microcosm and Jetstream lexicons. Regenerate the affected package after a schema or query change.

```sh
nix develop -c ./capensis/scripts/generate-db.sh
nix develop -c ./helix/scripts/generate-db.sh
nix develop -c ./palustre/microcosm/generate.sh
nix develop -c ./palustre/jetstream/generate.sh
```

CI checks the generated files, runs the Go and Nix checks, and builds each container. Successful builds on `main` publish the images. After a Go dependency change, rebuild all four Nix packages and update the shared `vendorHash` in `flake-module.nix` if Nix reports a mismatch.

[MIT license](LICENSE)
