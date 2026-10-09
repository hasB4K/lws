#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 12 · constraint-based progress from a tight state" "$MAGENTA"
cat <<EOF
  Sequence:  A 2P/3D -> B 1P/3D
  Budget:    maxSurge=0; maxUnavailable=2P/1D
  Timing:    B pods take 18s to become Ready

  The interesting state is old=2P/1D and new=0P/1D, with no pending work.
  Revision completeness rules out old=0P/1D. The planner's single constraint
  calculation instead chooses the furthest safe target that leaves at least
  one P and one D in A. Once B has usable capacity, A retires as one unit.

  The watcher permits a brief incomplete observation between two sequential
  API patches, but an old revision must never remain incomplete.
EOF

step "Create and stabilize A at 2P/3D"
apply_revision A A 2 3 0 2 0 1 2 2 12
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 2 3 240

pause_phase "Start the zero-surge rollout and shrink P from two to one"
step "Apply B at 1P/3D"
reset_observations
apply_revision B B 1 3 0 2 0 1 18 18 12
REV_B="$LAST_REVISION"
set_bounds 2 3 1 3 0 2 0 1
watch_until_complete "$REV_B" 1 3 720
assert_no_persistent_incomplete_old

finish_scenario "constraint-based progress from a tight state"
