#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 13 · bootstrap a zero-surge singleton role" "$MAGENTA"
cat <<EOF
  Sequence:  A 1P/5D -> B 1P/5D
  Budget:    maxSurge=0; maxUnavailable=1 for both roles
  Timing:    B pods take 18s to become Ready

  The ordinary planner first reaches A=1P/4D and B=0P/1D. It cannot remove
  A's last Prefill without leaving revision A incomplete, and B's Prefill
  cannot start inside the configured zero-surge ceiling.

  With no ordinary move available, the planner permits exactly one bootstrap
  Prefill: A=1P/4D and B=1P/1D. This temporarily makes total Prefill 2 rather
  than 1. It must wait for that replica instead of issuing another exception.
  Once B is usable, normal constrained progress resumes and A retires intact.
EOF

step "Create and stabilize A at 1P/5D"
apply_revision A A 1 5 0 1 0 1 2 2 12
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 1 5 240

pause_phase "Start the zero-surge rollout that needs one bootstrap Prefill"
step "Apply B at 1P/5D"
reset_observations
apply_revision B B 1 5 0 1 0 1 18 18 12
REV_B="$LAST_REVISION"
set_bounds 1 5 1 5 0 1 0 1
allow_bootstrap_surge prefill
watch_until_complete "$REV_B" 1 5 720
assert_observed_bootstrap_surge
assert_observed_pending
assert_no_persistent_incomplete_old

finish_scenario "bounded bootstrap surge unblocked the rollout"
