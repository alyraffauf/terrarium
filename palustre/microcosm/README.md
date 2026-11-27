# Microcosm client generation

Regenerate the Slingshot and Constellation clients from the repository root.

```sh
nix develop -c ./palustre/microcosm/generate.sh
```

The script uses Indigo's `lexgen` at the revision in `go.mod` and reads the schemas in `lexicons/`.

The schemas come from Microcosm's `microcosm-rs` repository at commit `c89c64d1005c45882a46c235a7b8a709abc92eb2`. [Identity schema](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/lexicons/blue.microcosm/identity/resolveMiniDoc.json), [backlinks schema](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/lexicons/blue.microcosm/links/getBacklinks.json).

The local `getBacklinks.json` adds the optional `cursor` request parameter from Constellation's `GetBacklinksQuery` at that revision. The generated client uses it to fetch subsequent pages. [Constellation handler](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/constellation/src/server/mod.rs).
