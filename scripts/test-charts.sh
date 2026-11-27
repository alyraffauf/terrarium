#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec go test -count=1 -tags=helm ./internal/charttest
