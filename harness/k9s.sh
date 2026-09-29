#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$SCRIPT_DIR/.kube-planner3" ]]; then
  echo "error: run ./install.sh first" >&2
  exit 1
fi

export KUBECONFIG="$SCRIPT_DIR/.kube-planner3"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
require_safe_context
launch_k9s
