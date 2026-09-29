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

banner "Scenario 15 · large interrupted A → B → C" "$MAGENTA"
cat <<EOF
  Shape:     50 prefill / 25 decode
  Budget:    maxSurge=$MAX_SURGE, maxUnavailable=$MAX_UNAVAILABLE for both roles
  Sequence:  start A→B; request C when B reaches about one-third progress

  The interruption is state-based rather than timed. The harness waits until
  B has at least 17 Prefill and 9 Decode Specs, then immediately applies C.
  A and partial B become separate old revisions whose Ready capacity and drain
  opportunities must continue to be evaluated independently. Every watcher
  update shows the live P:D ratio and built/remaining percentage of A, B, and C.
EOF

step "Create and stabilize A at 50P/25D"
apply_revision A A 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE" 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 50 25 600

pause_phase "Start A → B; C will be applied automatically near one-third progress"
step "Apply B"
reset_observations
apply_revision B B 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE" 12 12 10
REV_B="$LAST_REVISION"
set_bounds 50 25 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE"
set_fraction_check "$REV_B" 50 25
watch_until_revision_progress "$REV_B" 50 25 1 3 1200

step "B reached approximately one-third progress; apply C immediately"
apply_revision C C 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE" 12 12 10
REV_C="$LAST_REVISION"
set_bounds 50 25 50 25 "$MAX_SURGE" "$MAX_UNAVAILABLE" "$MAX_SURGE" "$MAX_UNAVAILABLE"
# With multiple old revisions, C replaces one revision-sized phase at a time.
# Comparing C directly with the final 50P/25D ratio would therefore reject
# valid intermediate phase targets.
clear_fraction_check
watch_until_complete "$REV_C" 50 25 1800
assert_observed_pending
assert_bounded_drained_revisions

finish_scenario "large state-triggered A → B → C rollout"
