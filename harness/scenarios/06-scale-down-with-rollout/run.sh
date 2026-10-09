#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 06 · scale down while changing revision" "$MAGENTA"
cat <<EOF
  Sequence:  A 8P/4D -> B 3P/2D
  Budget:    maxSurge=1, maxUnavailable=1
  Timing:    B pods take 18s to become Ready

  This tests replacement and shrink together. The availability floor uses
  min(initial,target), so it is P>=2 and D>=1, while the surge ceiling remains
  based on the larger A side: P<=9 and D<=5.
EOF

step "Create and stabilize A at 8P/4D"
apply_revision A A 8 4 1 1 1 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 8 4 240

pause_phase "Shrink and change the template together"
step "Apply B at 3P/2D"
reset_observations
apply_revision B B 3 2 1 1 1 1 18 18 10
REV_B="$LAST_REVISION"
set_bounds 8 4 3 2 1 1 1 1
set_fraction_check "$REV_B" 3 2
watch_until_complete "$REV_B" 3 2 600
assert_observed_pending

finish_scenario "scale-down combined with a rollout"
