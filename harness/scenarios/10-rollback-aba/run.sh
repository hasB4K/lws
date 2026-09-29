#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 10 · rollback A → B → A" "$MAGENTA"
cat <<EOF
  Shape:     6 prefill / 3 decode
  Budget:    maxSurge=2, maxUnavailable=1
  Sequence:  start A→B, then restore the exact A template

  Reapplying A produces A's original hash; no third revision is created.
  B becomes the newest old revision and drains while A grows back to target.
EOF

step "Create and stabilize A"
apply_revision A A 6 3 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 6 3 240

pause_phase "Start A → B"
step "Apply slow B and observe it for 16 seconds"
apply_revision B B 6 3 2 1 2 1 28 28 10
REV_B="$LAST_REVISION"
set_bounds 6 3 6 3 2 1 2 1
observe_for 16 "$REV_B" 6 3

pause_phase "Roll back to the exact A template"
current_total_specs
ROLLBACK_INITIAL_P="$CURRENT_SPEC_P"
ROLLBACK_INITIAL_D="$CURRENT_SPEC_D"
step "Reapply A"
reset_observations
apply_manifest A 6 3 2 1 2 1 2 2 10
set_bounds "$ROLLBACK_INITIAL_P" "$ROLLBACK_INITIAL_D" 6 3 2 1 2 1
set_drain_order "$REV_B"
watch_until_complete "$REV_A" 6 3 720
wait_for_revision_gone "$REV_B" 90
assert_eq "$REV_A" "$(list_revisions)" "rollback converged to the original A hash"

finish_scenario "revision reuse during rollback"
