# Terrarium

AT Protocol list tools, named after plants.

| Package | Purpose |
| --- | --- |
| [Capensis](capensis/) | Moderation lists of accounts hosted on selected PDSes. *Drosera capensis*. |
| [Helix](helix/) | Moderation lists of selected accounts' followers. *Hedera helix*. |
| [Palustre](palustre/) | Shared AT Protocol operations. *Sphagnum palustre*. |

Capensis and Helix exclude the list owner and accounts the owner follows. Creating a list does not subscribe you to its blocking actions.

## Build and develop

Run commands from the repository root:

```sh
nix build .#capensis .#helix
nix run .#capensis -- --help
nix run .#helix -- --help
nix develop -c go test ./...
nix develop -c go vet ./...
nix develop -c go build ./cmd/...
```

The repository has one Go module. Capensis and Helix each keep its Dockerfile, Helm chart, and Nix definition beside its code. The flake discovers `flake-module.nix` files through import-tree.

After editing SQL queries or lexicons, regenerate the committed Go files:

```sh
nix develop -c ./capensis/scripts/generate-db.sh
nix develop -c ./helix/scripts/generate-db.sh
nix develop -c ./palustre/scripts/generate.sh
```

SQL queries use sqlc. Schema changes need a new Goose migration in the application's `storage/migrations/` directory. Generation reads `palustre/lexicons/` and external definitions from the Indigo revision in `go.mod`.

## CI and dependency updates

CI tests Go code, checks both Nix packages and generated files, and builds both containers. Pushes and manual runs on `main` publish `ghcr.io/alyraffauf/terrarium/capensis` and `ghcr.io/alyraffauf/terrarium/helix` with `latest` and `sha-<commit>` tags. Pull requests build without publishing.

[Renovate](renovate.json) covers Go modules, flake inputs, GitHub Actions, and Docker images. Enable Renovate for the repository to receive update PRs. After Go dependency changes, build both Nix packages and update their `vendorHash` values if Nix reports a mismatch.

[MIT license](LICENSE).
