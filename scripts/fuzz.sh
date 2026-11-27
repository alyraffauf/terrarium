#!/bin/sh
# FUZZTIME is a Go duration or execution count, applied to each target.
set -eu
cd "$(dirname "$0")/.."
targets=0
packages=$(go list ./...)
for package in $packages; do
    tests=$(go test -run='^$' -list='^Fuzz' "$package")
    for target in $(printf '%s\n' "$tests" | sed -n '/^Fuzz[[:alnum:]_]*$/p'); do
        targets=$((targets + 1))
        printf 'Fuzzing %s/%s\n' "$package" "$target"
        go test "$package" -run='^$' -fuzz="^${target}$" -fuzztime="${FUZZTIME:-10s}" -parallel="${FUZZWORKERS:-2}"
    done
done
if [ "$targets" -eq 0 ]; then
    echo "No fuzz targets found" >&2
    exit 1
fi
