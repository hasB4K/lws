#!/usr/bin/env bash
# Dispatch one or all planner visualization scenarios.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECONFIG_FILE="$SCRIPT_DIR/.kube-planner3"
SCENARIO=""
START_K9S=0
INTERACTIVE=1
KEEP_RESOURCES=0
MAX_SURGE_OVERRIDE=""
MAX_UNAVAILABLE_OVERRIDE=""

usage() {
  cat <<'EOF'
Usage: ./run.sh [scenario|all] [--auto] [--keep] [--start-k9s]
                [--max-surge N] [--max-unavailable N]

With no scenario, lists all available scenarios.

  --auto        disable pauses (useful for assertion runs)
  --keep        preserve resources after one scenario
  --start-k9s   open k9s in a new Terminal window before the scenario
  --max-surge N override maxSurge for both roles in scenarios 14 and 15
  --max-unavailable N
                override maxUnavailable for both roles in scenarios 14 and 15
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-k9s) START_K9S=1 ;;
    --auto) INTERACTIVE=0 ;;
    --keep) KEEP_RESOURCES=1 ;;
    --max-surge)
      [[ $# -ge 2 ]] || { echo "error: --max-surge requires a value" >&2; exit 2; }
      MAX_SURGE_OVERRIDE="$2"
      shift
      ;;
    --max-surge=*) MAX_SURGE_OVERRIDE="${1#*=}" ;;
    --max-unavailable)
      [[ $# -ge 2 ]] || { echo "error: --max-unavailable requires a value" >&2; exit 2; }
      MAX_UNAVAILABLE_OVERRIDE="$2"
      shift
      ;;
    --max-unavailable=*) MAX_UNAVAILABLE_OVERRIDE="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "error: unknown option '$1'" >&2; usage >&2; exit 2 ;;
    *)
      if [[ -n "$SCENARIO" ]]; then
        echo "error: only one scenario may be selected" >&2
        exit 2
      fi
      SCENARIO="$1"
      ;;
  esac
  shift
done

for budget in "$MAX_SURGE_OVERRIDE" "$MAX_UNAVAILABLE_OVERRIDE"; do
  if [[ -n "$budget" && ! "$budget" =~ ^(0|[1-9][0-9]*)$ ]]; then
    echo "error: rollout budgets must be non-negative integers" >&2
    exit 2
  fi
done
if [[ "${MAX_SURGE_OVERRIDE:-5}" == "0" && "${MAX_UNAVAILABLE_OVERRIDE:-5}" == "0" ]]; then
  echo "error: maxSurge and maxUnavailable cannot both be zero; the rollout would have no legal first move" >&2
  exit 2
fi

if [[ ! -f "$KUBECONFIG_FILE" ]]; then
  echo "error: $KUBECONFIG_FILE does not exist; run ./install.sh first" >&2
  exit 1
fi
export KUBECONFIG="$KUBECONFIG_FILE"
export INTERACTIVE KEEP_RESOURCES MAX_SURGE_OVERRIDE MAX_UNAVAILABLE_OVERRIDE

# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
require_safe_context

list_scenarios() {
  echo "Scenarios (context: $(kubectl config current-context)):"
  for scenario_dir in "$SCRIPT_DIR"/scenarios/*/; do
    printf "  %s\n" "$(basename "$scenario_dir")"
  done
  echo
  echo "See README.md for the behavior and invariant covered by each scenario."
}

if [[ -z "$SCENARIO" ]]; then
  list_scenarios
  exit 0
fi

if (( START_K9S )); then
  launch_k9s
fi

if [[ "$SCENARIO" == "all" ]]; then
  if (( KEEP_RESOURCES )); then
    echo "error: --keep cannot be combined with 'all'" >&2
    exit 2
  fi
  for scenario_dir in "$SCRIPT_DIR"/scenarios/*/; do
    bash "$scenario_dir/run.sh"
  done
  banner "All planner scenarios passed" "$GREEN"
  exit 0
fi

target=""
for scenario_dir in "$SCRIPT_DIR"/scenarios/*/; do
  scenario_name=$(basename "$scenario_dir")
  case "$scenario_name" in
    "$SCENARIO"|"$SCENARIO"-*|0"$SCENARIO"-*|*"$SCENARIO"*) target="$scenario_dir"; break ;;
  esac
done

if [[ -z "$target" || ! -x "$target/run.sh" ]]; then
  echo "error: no runnable scenario matches '$SCENARIO'" >&2
  list_scenarios >&2
  exit 1
fi

exec bash "$target/run.sh"
