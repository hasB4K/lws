#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 02 · balanced A → B" "$MAGENTA"
cat <<EOF
  Shape:     4 prefill / 4 decode
  Budget:    maxSurge=1, maxUnavailable=1 for both roles
  Timing:    B pods take 18s to become Ready; old pods terminate for 10s

  With equal role sizes, fractional checkpoints map to nearly identical
  replica counts. The API writes are sequential, not atomic, so k9s can still
  briefly show one role one replica ahead.
EOF

step "Create and stabilize A"
apply_revision A A 4 4 1 1 1 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 4 4 180

pause_phase "Start the balanced A → B rollout"
step "Apply B"
reset_observations
apply_revision B B 4 4 1 1 1 1 18 18 10
REV_B="$LAST_REVISION"
set_bounds 4 4 4 4 1 1 1 1
set_fraction_check "$REV_B" 4 4
watch_until_complete "$REV_B" 4 4 360
assert_observed_pending

finish_scenario "balanced fractional lockstep"
