#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 07 · zero-surge drain-before-grow" "$MAGENTA"
cat <<EOF
  Shape:     4 prefill / 2 decode
  Budget:    maxSurge=0, maxUnavailable=1
  Timing:    B pods take 16s to become Ready; old pods terminate for 12s

  No extra replica may exist. Every replacement therefore requires an old
  slot to be released first. The watcher asserts total Spec never exceeds
  4P/2D. Planner unit tests assert the 3P/1D decision-time Ready floor.
EOF

step "Create and stabilize A"
apply_revision A A 4 2 0 1 0 1 2 2 12
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 4 2 240

pause_phase "Start the zero-surge A → B rollout"
step "Apply B"
reset_observations
apply_revision B B 4 2 0 1 0 1 16 16 12
REV_B="$LAST_REVISION"
set_bounds 4 2 4 2 0 1 0 1
set_fraction_check "$REV_B" 4 2
watch_until_complete "$REV_B" 4 2 600
assert_observed_pending

finish_scenario "zero-surge drain-before-grow ordering"
