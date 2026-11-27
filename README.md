# Terrarium

AT Protocol list tools, named after plants.

| Package | Purpose |
| --- | --- |
| [Capensis](capensis/) | Moderation lists of accounts hosted on selected PDSes. *Drosera capensis*. |
| [Cordata](cordata/) | Moderation list of accounts that liked a post. |
| [Trifolium](trifolium/) | One moderation list per cluster, with a configurable follower overlap threshold. |
| [Helix](helix/) | Moderation lists of selected accounts' followers. *Hedera helix*. |
| [Palustre](palustre/) | Shared AT Protocol operations. *Sphagnum palustre*. |

Each managed list gets a stable title drawn from 1,024 combinations of neutral adjectives and nature nouns, such as `Quiet Meadow`. Its description identifies the target. On upgrade, the daemons rename existing managed lists in place.

Capensis, Helix, and Trifolium exclude the list owner and accounts the owner follows. Cordata includes everyone who liked the post, including the owner. Creating a list does not subscribe you to its blocking actions.

## Build and develop

Run commands from the repository root:

```sh
nix build .#capensis .#helix .#cordata .#trifolium
nix run .#capensis -- --help
nix run .#helix -- --help
nix develop -c go test ./...
nix develop -c go vet ./...
nix develop -c go build ./cmd/...
```

The repository has one Go module. Its `go` directive sets the minimum supported toolchain. CI tests the Go patch version used by the container builds. The Nix checks also run the tests with the Go version pinned in `flake.lock`. Each daemon keeps its Dockerfile, Helm chart, and Nix definition beside its code. The flake discovers `flake-module.nix` files through import-tree.

After editing SQL queries or Microcosm lexicons, regenerate the committed Go files:

```sh
nix develop -c ./capensis/scripts/generate-db.sh
nix develop -c ./helix/scripts/generate-db.sh
nix develop -c ./palustre/microcosm/generate.sh
```

SQL queries use sqlc. Schema changes need a new Goose migration in the application's `storage/migrations/` directory. AT Protocol and Bluesky operations use Indigo’s generated lexicon types. The [Microcosm clients](palustre/microcosm/) use the same pinned Indigo generator for Slingshot and Constellation.

## CI and dependency updates

CI tests Go code, checks all four Nix packages and generated files, and builds all four containers. Pushes and manual runs on `main` publish `ghcr.io/alyraffauf/terrarium/capensis`, `ghcr.io/alyraffauf/terrarium/helix`, `ghcr.io/alyraffauf/terrarium/cordata`, and `ghcr.io/alyraffauf/terrarium/trifolium` with `latest` and `sha-<commit>` tags. Pull requests build without publishing.

[Renovate](renovate.json) covers Go modules, flake inputs, GitHub Actions, and Docker images. Enable Renovate for the repository to receive update PRs. After Go dependency changes, build all four Nix packages and update the shared `vendorHash` in `flake-module.nix` if Nix reports a mismatch.

[MIT license](LICENSE).

## Property tests and fuzzing

Run `go test ./...` to replay the checked-in seeds. Run `nix develop -c ./scripts/fuzz.sh` to generate new cases for every native Go fuzz target. Set `FUZZTIME=30s` for 30 seconds per target, or `FUZZTIME=1000x` for a fixed number of executions. `FUZZWORKERS` controls parallel workers per target and defaults to two. Run the Helm deployment contracts with `nix develop -c ./scripts/test-charts.sh`.

When Go finds a failure, it writes a minimized input under the affected package's `testdata/fuzz/` directory and prints the command to replay it. Keep confirmed failing cases with the fix.
