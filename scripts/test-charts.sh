#!/bin/sh
# Executable Helm contract checks for both charts. Requires `helm` (or set
# HELM="nix run nixpkgs#kubernetes-helm --"). Every rendered arg that the
# binary parses must be well-formed for string and numeric value spellings.
set -eu
HELM="${HELM:-helm}"
cd "$(dirname "$0")/.."
fail=0
check_no_garbled_format() {
  # $1 = app, rest = helm args
  app="$1"; shift
  out="$($HELM template rel "./$app/charts/$app" "$@" 2>&1)" || { echo "FAIL render $app: $out"; fail=1; return; }
  if printf '%s' "$out" | grep -q '%!'; then
    echo "FAIL $app rendered a garbled printf verb:"; printf '%s\n' "$out" | grep '%!' ; fail=1
  fi
}
# Numeric rescanInterval / identifier spellings (YAML numbers, not strings).
check_no_garbled_format capensis --set identifier=you.example.com --set existingSecret=x --set 'pds[0]=a.example' --set rescanInterval=6
check_no_garbled_format capensis --set identifier=you.example.com --set existingSecret=x --set 'pds[0]=a.example' --set rescanInterval=6h
check_no_garbled_format helix --set identifier=you.example.com --set existingSecret=x --set 'accounts[0]=a.example' --set rescanInterval=6
check_no_garbled_format helix --set identifier=you.example.com --set existingSecret=x --set 'accounts[0]=a.example' --set metrics.enabled=true --set metrics.port=8080
# Metrics wiring must agree on the port across Service, container, annotation.
rendered="$($HELM template rel ./helix/charts/helix --set identifier=you.example.com --set existingSecret=x --set 'accounts[0]=a.example' --set metrics.enabled=true --set metrics.port=8080)"
for want in 'containerPort: 8080' '--metrics-address=:8080' 'port: 8080' 'prometheus.io/port: "8080"'; do
  printf '%s' "$rendered" | grep -qF -e "$want" || { echo "FAIL helix metrics missing: $want"; fail=1; }
done
rendered="$($HELM template rel ./capensis/charts/capensis --set identifier=you.example.com --set existingSecret=x --set 'pds[0]=a.example' --set metrics.enabled=true)"
for want in 'containerPort: 9090' '--metrics-address=:9090'; do
  printf '%s' "$rendered" | grep -qF -e "$want" || { echo "FAIL capensis metrics missing: $want"; fail=1; }
done
# Missing required values must fail rendering, not produce a broken pod.
if $HELM template rel ./capensis/charts/capensis --set existingSecret=x --set 'pds[0]=a.example' >/dev/null 2>&1; then
  echo "FAIL capensis rendered without identifier"; fail=1
fi
if $HELM template rel ./helix/charts/helix --set identifier=you.example.com --set existingSecret=x >/dev/null 2>&1; then
  echo "FAIL helix rendered without accounts"; fail=1
fi
if $HELM template rel ./capensis/charts/capensis --set identifier=you.example.com --set existingSecret=x >/dev/null 2>&1; then
  echo "FAIL capensis rendered without pds"; fail=1
fi
[ "$fail" -eq 0 ] && echo "helm contract: all checks passed"
exit "$fail"
