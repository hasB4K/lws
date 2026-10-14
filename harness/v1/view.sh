#!/usr/bin/env bash
# The supplied command may customize k9s, but receives our isolation arguments.
set -euo pipefail
RUN_DIR="${1:?run directory required}"
CONTEXT="${2:?kind context required}"
K9S_COMMAND="${3:-k9s}"
export KUBECONFIG="$RUN_DIR/kubeconfig"
export K9S_CONFIG_DIR="$RUN_DIR/k9s-config"
export K9S_LOGS_DIR="$RUN_DIR/k9s-logs"
[[ -s "$KUBECONFIG" && "$CONTEXT" == kind-lws-v1-* ]] || { echo 'Not a v1 harness kubeconfig.' >&2; exit 1; }
k() { kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" "$@"; }
[[ "$(k config get-contexts -o name)" == "$CONTEXT" && "$(k config current-context)" == "$CONTEXT" ]] || {
  echo 'Refusing to open k9s: the private kubeconfig contains another context.' >&2; exit 1;
}
server="$(k config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" =~ ^https://127\.0\.0\.1:[0-9]+$ ]] || { echo 'Refusing a non-local API endpoint.' >&2; exit 1; }
mkdir -p "$K9S_CONFIG_DIR" "$K9S_LOGS_DIR"
printf '%s\n' "$$" > "$RUN_DIR/k9s.pid"
exec "$K9S_COMMAND" --kubeconfig "$KUBECONFIG" --context "$CONTEXT" --namespace v1-harness \
  --command pods --readonly --refresh 1 --request-timeout 10s --logFile "$RUN_DIR/k9s.log"
