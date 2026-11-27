#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/../.."
# The subscription lexicon comes from bluesky-social/jetstream v0.4.6.
indigo=$(go list -m -f '{{.Dir}}' github.com/bluesky-social/indigo)
go tool lexgen --build-file palustre/jetstream/lexgen.json \
  --external-lexicons "$indigo/lexicons/com/atproto" palustre/jetstream/lexicons
