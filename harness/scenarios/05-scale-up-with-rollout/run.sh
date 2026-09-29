#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 05 · scale up while changing revision" "$MAGENTA"
cat <<EOF
  Sequence:  A 3P/2D -> B 7P/4D
  Budget:    maxSurge=2, maxUnavailable=1
  Timing:    B pods take 20s to become Ready

  This combines replacement with capacity growth. Hard surge ceilings are
  based on max(initial,target): P<=9 and D<=6. The old side drains while the
  larger B side grows toward its new ratio.
EOF

step "Create and stabilize A at 3P/2D"
apply_revision A A 3 2 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 3 2 180

pause_phase "Change both the template and target size"
step "Apply B at 7P/4D"
reset_observations
apply_revision B B 7 4 2 1 2 1 20 20 10
REV_B="$LAST_REVISION"
set_bounds 3 2 7 4 2 1 2 1
set_fraction_check "$REV_B" 7 4
watch_until_complete "$REV_B" 7 4 600
assert_observed_pending

finish_scenario "scale-up combined with a rollout"
