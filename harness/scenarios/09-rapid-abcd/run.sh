#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 09 · rapid A → B → C → D" "$MAGENTA"
cat <<EOF
  Shape:     6 prefill / 3 decode
  Budget:    maxSurge=2, maxUnavailable=1
  Sequence:  request B, C, and D ten seconds apart

  This deliberately accumulates interrupted revisions. Once D is desired,
  Executable old revisions should retire newest-first: C, then B, then the
  remaining A. A blocked candidate may be skipped. The harness also checks
  that cleanup never retains more than one fully drained old revision.
EOF

step "Create and stabilize A"
apply_revision A A 6 3 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 6 3 240

pause_phase "Begin the rapid sequence"
step "Apply B"
apply_revision B B 6 3 2 1 2 1 34 34 10
REV_B="$LAST_REVISION"
observe_for 10 "$REV_B" 6 3

step "Apply C while B is in flight"
apply_revision C C 6 3 2 1 2 1 30 30 10
REV_C="$LAST_REVISION"
observe_for 10 "$REV_C" 6 3

step "Apply final revision D while C is in flight"
reset_observations
apply_revision D D 6 3 2 1 2 1 22 22 10
REV_D="$LAST_REVISION"
set_bounds 6 3 6 3 2 1 2 1
set_drain_order "$REV_C" "$REV_B" "$REV_A"
watch_until_complete "$REV_D" 6 3 900
assert_bounded_drained_revisions

finish_scenario "newest-first A → B → C → D retirement"
