#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 11 · asymmetric per-role budgets" "$MAGENTA"
cat <<EOF
  Shape:     8 prefill / 4 decode
  Prefill:   maxSurge=3, maxUnavailable=2
  Decode:    maxSurge=0, maxUnavailable=1
  Timing:    B prefill is Ready after 8s; B decode after 20s

  Prefill has much more raw concurrency, while decode must release each slot
  before replacing it. The raw bounds differ per role, but new-side normalized
  progress must still remain within the shared 1/4 skew bound.
EOF

step "Create and stabilize A"
apply_revision A A 8 4 3 2 0 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 8 4 240

pause_phase "Start A → B with asymmetric budgets"
step "Apply B"
reset_observations
apply_revision B B 8 4 3 2 0 1 8 20 10
REV_B="$LAST_REVISION"
set_bounds 8 4 8 4 3 2 0 1
set_fraction_check "$REV_B" 8 4
watch_until_complete "$REV_B" 8 4 720
assert_observed_pending

finish_scenario "per-role limits under fractional coordination"
