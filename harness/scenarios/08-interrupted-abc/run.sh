#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 08 · interrupted A → B → C" "$MAGENTA"
cat <<EOF
  Shape:     6 prefill / 3 decode
  Budget:    maxSurge=2, maxUnavailable=1
  Sequence:  start A→B; while B is still unready, request C

  At C creation, both A and B are old revisions. The executor must consume
  drain budget from B first. The harness asserts that the remaining A counts
  do not move until B reaches zero.

  Every revision records the replica count it was intended to reach. While B
  is active, B's own 6P/3D intended count is its old-side baseline and A stays
  parked. Once B is gone, A's own baseline is used. Their partial Specs are
  never added into one baseline, while parked usable readiness still
  participates in the 5P/2D decision-time availability check.
EOF

step "Create and stabilize A"
apply_revision A A 6 3 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 6 3 240

pause_phase "Start A → B"
step "Apply slow B and observe it for 15 seconds"
apply_revision B B 6 3 2 1 2 1 30 30 10
REV_B="$LAST_REVISION"
set_bounds 6 3 6 3 2 1 2 1
observe_for 15 "$REV_B" 6 3

pause_phase "Interrupt B with revision C"
step "Apply C before B finishes"
reset_observations
apply_revision C C 6 3 2 1 2 1 22 22 10
REV_C="$LAST_REVISION"
set_bounds 6 3 6 3 2 1 2 1
set_drain_order "$REV_B" "$REV_A"
watch_until_complete "$REV_C" 6 3 720

finish_scenario "newest-first retirement with a stable interrupted-rollout baseline"
