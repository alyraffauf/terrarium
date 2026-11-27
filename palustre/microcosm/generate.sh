#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/../.."
go tool lexgen --build-file palustre/microcosm/lexgen.json palustre/microcosm/lexicons
