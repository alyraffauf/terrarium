#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/../.."
indigo=$(go list -m -f '{{.Dir}}' github.com/bluesky-social/indigo)
go tool lexgen \
  --build-file palustre/scripts/lexgen.json \
  --external-lexicons "$indigo/lexicons/app/bsky/graph/defs.json" \
  --external-lexicons "$indigo/lexicons/com/atproto/label/defs.json" palustre/lexicons
go run ./palustre/scripts/cborgen
