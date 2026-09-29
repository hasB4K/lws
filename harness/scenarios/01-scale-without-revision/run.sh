#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "$SCRIPT_DIR/../../lib/common.sh"

prepare_scenario
trap_cleanup

banner "Scenario 01 · scale without creating a revision" "$MAGENTA"
cat <<EOF
  Sequence:  A at 2P/2D -> 6P/4D -> 3P/1D
  Timing:    every newly created pod waits 12s before becoming Ready

  This is the comparison case: changing only replicas does not change the
  template hash, so the controller uses its steady-state scaling path. Watch
  Desired and Ready change on the same two A LeaderWorkerSets; no B appears.
EOF

step "Create revision A at 2P/2D"
apply_revision A A 2 2 1 1 1 1 12 12 8
REV_A="$LAST_REVISION"
watch_until_complete "$REV_A" 2 2 180

pause_phase "Scale A up to 6P/4D"
step "Scale up without changing the template"
apply_manifest A 6 4 1 1 1 1 12 12 8
watch_until_complete "$REV_A" 6 4 240
assert_eq "$REV_A" "$(list_revisions)" "scale-up retained revision A"

pause_phase "Scale A down to 3P/1D"
step "Scale down without changing the template"
apply_manifest A 3 1 1 1 1 1 12 12 8
watch_until_complete "$REV_A" 3 1 180
assert_eq "$REV_A" "$(list_revisions)" "scale-down retained revision A"

finish_scenario "steady-state scale up and scale down"
