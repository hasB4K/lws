#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 03 · imbalanced 8P/4D A → B" "$MAGENTA"
cat <<EOF
  Shape:     8 prefill / 4 decode (2:1)
  Budget:    maxSurge=2, maxUnavailable=1 for both roles
  Timing:    B pods take 18s to become Ready

  One shared fraction maps to different absolute counts: a 1/4 move is two
  prefill replicas but one decode replica. The watcher enforces that new-side
  progress differs by at most largestReplicaFraction=1/4.
EOF

step "Create and stabilize A"
apply_revision A A 8 4 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 8 4 240

pause_phase "Start the imbalanced A → B rollout"
step "Apply B"
reset_observations
apply_revision B B 8 4 2 1 2 1 18 18 10
REV_B="$LAST_REVISION"
set_bounds 8 4 8 4 2 1 2 1
set_fraction_check "$REV_B" 8 4
watch_until_complete "$REV_B" 8 4 480
assert_observed_pending

finish_scenario "2:1 proportional fractional lockstep"
