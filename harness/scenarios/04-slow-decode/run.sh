#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 04 · decode readiness is much slower" "$MAGENTA"
cat <<EOF
  Shape:     6 prefill / 3 decode (2:1)
  Budget:    maxSurge=2, maxUnavailable=1
  Timing:    B prefill becomes Ready after 6s; B decode after 32s

  Prefill is allowed to lead within the 1/3 fractional skew bound, then must
  wait for decode. Issued-but-unready Decode replicas count as fractional
  progress, while committed readiness limits how much old capacity may drain.
EOF

step "Create and stabilize A"
apply_revision A A 6 3 2 1 2 1 2 2 10
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 6 3 240

pause_phase "Start B with deliberately slow decode readiness"
step "Apply B"
reset_observations
apply_revision B B 6 3 2 1 2 1 6 32 10
REV_B="$LAST_REVISION"
set_bounds 6 3 6 3 2 1 2 1
set_fraction_check "$REV_B" 6 3
watch_until_complete "$REV_B" 6 3 600
assert_observed_pending

finish_scenario "slow-role readiness and bounded skew"
