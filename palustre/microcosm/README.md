# Microcosm client generation

Regenerate the clients from the repository root.

```sh
./palustre/microcosm/generate.sh
```

The script uses Indigo's `lexgen` at the revision in `go.mod`. It reads the checked-in schemas and requires no access to Microcosm's Git server.

The schemas come from Microcosm's `microcosm-rs` repository at commit `c89c64d1005c45882a46c235a7b8a709abc92eb2`. The vendored `getBacklinks.json` adds the optional request parameter `cursor` defined by Constellation's `GetBacklinksQuery` at the same revision. The generated client needs that parameter to fetch subsequent pages. [Identity schema](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/lexicons/blue.microcosm/identity/resolveMiniDoc.json), [backlinks schema](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/lexicons/blue.microcosm/links/getBacklinks.json), [Constellation handler](https://tangled.org/microcosm.blue/microcosm-rs/blob/c89c64d1005c45882a46c235a7b8a709abc92eb2/constellation/src/server/mod.rs).
