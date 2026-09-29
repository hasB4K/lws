#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

MAX_SURGE="${MAX_SURGE_OVERRIDE:-5}"
MAX_UNAVAILABLE="${MAX_UNAVAILABLE_OVERRIDE:-5}"
if (( MAX_SURGE == 0 && MAX_UNAVAILABLE == 0 )); then
  echo "error: maxSurge and maxUnavailable cannot both be zero; the rollout would have no legal first move" >&2
  exit 2
fi

prepare_scenario
trap_cleanup

banner "Scenario 14 · large A → B" "$MAGENTA"
cat <<EOF
  Shape:     50 prefill / 25 decode
  Budget:    maxSurge=$MAX_SURGE, maxUnavailable=$MAX_UNAVAILABLE for both roles
  Timing:    B pods take 12s to become Ready; old pods terminate for 10s

  This is the large ordinary-rollout baseline. The 2:1 role ratio should remain
  inside the 1/25 fractional window while readiness releases work in visible
  batches. Every watcher update shows each revision's live P:D Spec ratio,
  Ready counts, and built/remaining percentage.
EOF

step "Create and stabilize A at 50P/25D"
apply_revision A A 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE" 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 50 25 600

pause_phase "Start the large A → B rollout"
step "Apply B"
reset_observations
apply_revision B B 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE" 12 12 10
REV_B="$LAST_REVISION"
set_bounds 50 25 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE"
set_fraction_check "$REV_B" 50 25
watch_until_complete "$REV_B" 50 25 1200
assert_observed_pending

finish_scenario "large 50P/25D fractional rollout"
