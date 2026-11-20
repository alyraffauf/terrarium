# Terrarium

AT Protocol list tools, named after plants.

| Package | Purpose |
| --- | --- |
| [Capensis](capensis/) | Moderation lists of accounts hosted on selected PDSes. *Drosera capensis*. |
| [Cordata](cordata/) | Moderation list of accounts that liked a post. |
| [Helix](helix/) | Moderation lists of selected accounts' followers. *Hedera helix*. |
| [Palustre](palustre/) | Shared AT Protocol operations. *Sphagnum palustre*. |

Each managed list gets a stable title drawn from 1,024 combinations of neutral adjectives and nature nouns, such as `Quiet Meadow`. Its description identifies the target. On upgrade, the daemons rename existing managed lists in place.

Capensis and Helix exclude the list owner and accounts the owner follows. Creating a list does not subscribe you to its blocking actions.

## Build and develop

Run commands from the repository root:

```sh
nix build .#capensis .#helix .#cordata
nix run .#capensis -- --help
nix run .#helix -- --help
nix develop -c go test ./...
nix develop -c go vet ./...
nix develop -c go build ./cmd/...
```

The repository has one Go module. Each daemon keeps its Dockerfile, Helm chart, and Nix definition beside its code. The flake discovers `flake-module.nix` files through import-tree.

After editing SQL queries or lexicons, regenerate the committed Go files:

```sh
nix develop -c ./capensis/scripts/generate-db.sh
nix develop -c ./helix/scripts/generate-db.sh
nix develop -c ./palustre/scripts/generate.sh
```

SQL queries use sqlc. Schema changes need a new Goose migration in the application's `storage/migrations/` directory. Generation reads `palustre/lexicons/` and external definitions from the Indigo revision in `go.mod`.

## CI and dependency updates

CI tests Go code, checks all three Nix packages and generated files, and builds all three containers. Pushes and manual runs on `main` publish `ghcr.io/alyraffauf/terrarium/capensis`, `ghcr.io/alyraffauf/terrarium/helix`, and `ghcr.io/alyraffauf/terrarium/cordata` with `latest` and `sha-<commit>` tags. Pull requests build without publishing.

[Renovate](renovate.json) covers Go modules, flake inputs, GitHub Actions, and Docker images. Enable Renovate for the repository to receive update PRs. After Go dependency changes, build all three Nix packages and update their `vendorHash` values if Nix reports a mismatch.

[MIT license](LICENSE).
