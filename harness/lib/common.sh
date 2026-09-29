# Shared helpers for the fractional-lockstep rollout scenarios.
# Compatible with the macOS-provided Bash 3.2.

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${NAME:-planner-viz}"
NS="${NS:-default}"
CLUSTER="${CLUSTER:-viz-planner3}"
INTERACTIVE="${INTERACTIVE:-1}"
KEEP_RESOURCES="${KEEP_RESOURCES:-0}"

DS_RESOURCE="disaggregatedsets.disaggregatedset.x-k8s.io"
SET_LABEL="disaggregatedset.x-k8s.io/name"
ROLE_LABEL="disaggregatedset.x-k8s.io/role"
REVISION_LABEL="disaggregatedset.x-k8s.io/revision"
INITIAL_REPLICAS_ANNOTATION="disaggregatedset.x-k8s.io/initial-replicas"

BOLD=$'\033[1m'
RED=$'\033[31m'
GREEN=$'\033[32m'
YELLOW=$'\033[33m'
BLUE=$'\033[34m'
MAGENTA=$'\033[35m'
CYAN=$'\033[36m'
DIM=$'\033[2m'
RESET=$'\033[0m'

REVISION_HASHES=()
REVISION_NAMES=()
DRAIN_HASHES=()
DRAIN_BASE_P=()
DRAIN_BASE_D=()

MAX_SPEC_P=-1
MAX_SPEC_D=-1
BOOTSTRAP_SPEC_P=0
BOOTSTRAP_SPEC_D=0
MIN_READY_P=-1
MIN_READY_D=-1
CHECK_FRACTION=0
FRACTION_REVISION=""
FRACTION_TARGET_P=0
FRACTION_TARGET_D=0
FRACTION_SKEW_STREAK=0
OBSERVED_PENDING=0
OBSERVED_BOOTSTRAP_SURGE=0
OBSERVED_INCOMPLETE_OLD=0
INCOMPLETE_OLD_STREAK=0
MAX_INCOMPLETE_OLD_STREAK=0
MAX_DRAINED_OLD_REVISIONS=0
LAST_REVISION=""
CURRENT_SPEC_P=0
CURRENT_SPEC_D=0
ROLLOUT_TARGET_P=0
ROLLOUT_TARGET_D=0

banner() {
  local message="$1" color="${2:-$BLUE}"
  echo
  echo "${color}${BOLD}════════════════════════════════════════════════════════════${RESET}"
  echo "${color}${BOLD}  $message${RESET}"
  echo "${color}${BOLD}════════════════════════════════════════════════════════════${RESET}"
  echo
}

step() {
  echo
  echo "${CYAN}${BOLD}▶ $1${RESET}"
}

info() {
  echo "  ${DIM}$1${RESET}"
}

pass() {
  echo "  ${GREEN}✓${RESET} $1"
}

fail() {
  echo "  ${RED}✗ $1${RESET}" >&2
  dump_state >&2 || true
  return 1
}

pause_phase() {
  if (( INTERACTIVE )) && [[ -t 0 ]]; then
    echo
    read -r -p "${YELLOW}${BOLD}⏸  ${1:-Press Enter to continue}...${RESET} "
  fi
}

require_safe_context() {
  local context
  context=$(kubectl config current-context 2>/dev/null || true)
  if [[ "$context" != "kind-$CLUSTER" ]]; then
    echo "${RED}error: expected context 'kind-$CLUSTER', got '${context:-<none>}'${RESET}" >&2
    return 1
  fi
}

launch_k9s() {
  command -v k9s >/dev/null 2>&1 || {
    echo "${RED}error: k9s is not installed${RESET}" >&2
    return 1
  }
  local kcfg="${KUBECONFIG:-$HARNESS_DIR/.kube-planner3}"
  local command_line="k9s --kubeconfig '$kcfg' --namespace '$NS' --command lws --readonly --refresh 1"
  case "$(uname -s)" in
    Darwin)
      command -v osascript >/dev/null 2>&1 || {
        echo "${RED}error: osascript is required to open macOS Terminal${RESET}" >&2
        return 1
      }
      osascript <<EOF
tell application "Terminal"
  activate
  set newWindow to do script "$command_line"
  set custom title of front window to "k9s • planner3"
end tell
EOF
      ;;
    Linux)
      command -v x-terminal-emulator >/dev/null 2>&1 || {
        echo "${RED}error: x-terminal-emulator is required to open k9s${RESET}" >&2
        return 1
      }
      x-terminal-emulator -e sh -lc "$command_line" >/dev/null 2>&1 &
      ;;
    *)
      echo "${RED}error: cannot launch a terminal on $(uname -s)${RESET}" >&2
      return 1
      ;;
  esac
  pass "k9s opened on LeaderWorkerSets in namespace $NS"
}

cleanup_scenario() {
  if (( KEEP_RESOURCES )); then
    info "Keeping $DS_RESOURCE/$NAME for inspection."
    return 0
  fi
  kubectl -n "$NS" delete "$DS_RESOURCE" "$NAME" \
    --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "$NS" delete leaderworkersets -l "$SET_LABEL=$NAME" \
    --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
  kubectl -n "$NS" delete pods -l "$SET_LABEL=$NAME" \
    --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
}

prepare_scenario() {
  require_safe_context
  local preserve="$KEEP_RESOURCES"
  KEEP_RESOURCES=0
  cleanup_scenario
  KEEP_RESOURCES="$preserve"
}

trap_cleanup() {
  trap 'scenario_status=$?; cleanup_scenario; exit "$scenario_status"' EXIT
}

apply_manifest() {
  local version="$1" prefill="$2" decode="$3"
  local p_surge="$4" p_unavailable="$5" d_surge="$6" d_unavailable="$7"
  local p_ready_delay="$8" d_ready_delay="$9" termination_delay="${10:-10}"
  local termination_grace=$((termination_delay + 5))

  kubectl apply -f - >/dev/null <<EOF
apiVersion: disaggregatedset.x-k8s.io/v1
kind: DisaggregatedSet
metadata:
  name: $NAME
  namespace: $NS
  labels:
    planner3.lws.x-k8s.io/harness: "true"
spec:
  roles:
  - name: prefill
    spec:
      replicas: $prefill
      rolloutStrategy:
        rollingUpdateConfiguration:
          maxSurge: $p_surge
          maxUnavailable: $p_unavailable
      leaderWorkerTemplate:
        size: 1
        restartPolicy: RecreateGroupOnPodRestart
        workerTemplate:
          metadata:
            labels:
              planner3.lws.x-k8s.io/version: "$version"
              planner3.lws.x-k8s.io/role: prefill
          spec:
            terminationGracePeriodSeconds: $termination_grace
            containers:
            - name: main
              image: mirror.gcr.io/library/busybox:1.36
              imagePullPolicy: IfNotPresent
              env:
              - name: VERSION
                value: "$version"
              - name: ROLE
                value: prefill
              command: ["sh", "-c"]
              args:
              - "echo starting prefill/$version; sleep $p_ready_delay; touch /tmp/ready; exec sleep 2147483647"
              readinessProbe:
                exec:
                  command: ["test", "-f", "/tmp/ready"]
                periodSeconds: 1
              lifecycle:
                preStop:
                  exec:
                    command: ["sh", "-c", "sleep $termination_delay"]
  - name: decode
    spec:
      replicas: $decode
      rolloutStrategy:
        rollingUpdateConfiguration:
          maxSurge: $d_surge
          maxUnavailable: $d_unavailable
      leaderWorkerTemplate:
        size: 1
        restartPolicy: RecreateGroupOnPodRestart
        workerTemplate:
          metadata:
            labels:
              planner3.lws.x-k8s.io/version: "$version"
              planner3.lws.x-k8s.io/role: decode
          spec:
            terminationGracePeriodSeconds: $termination_grace
            containers:
            - name: main
              image: mirror.gcr.io/library/busybox:1.36
              imagePullPolicy: IfNotPresent
              env:
              - name: VERSION
                value: "$version"
              - name: ROLE
                value: decode
              command: ["sh", "-c"]
              args:
              - "echo starting decode/$version; sleep $d_ready_delay; touch /tmp/ready; exec sleep 2147483647"
              readinessProbe:
                exec:
                  command: ["test", "-f", "/tmp/ready"]
                periodSeconds: 1
              lifecycle:
                preStop:
                  exec:
                    command: ["sh", "-c", "sleep $termination_delay"]
EOF
}

list_revisions() {
  kubectl -n "$NS" get leaderworkersets -l "$SET_LABEL=$NAME" -o json 2>/dev/null \
    | jq -r --arg revision "$REVISION_LABEL" \
      '.items[] | .metadata.labels[$revision] // empty' \
    | sort -u
}

wait_for_new_revision() {
  local existing="$1" timeout="${2:-45}" deadline candidate current
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    current=$(list_revisions)
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      if ! grep -Fxq "$candidate" <<<"$existing"; then
        LAST_REVISION="$candidate"
        return 0
      fi
    done <<<"$current"
    sleep 1
  done
  fail "no new revision appeared within ${timeout}s"
}

wait_for_revision_gone() {
  local revision="$1" timeout="${2:-90}" deadline
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if ! list_revisions | grep -Fxq "$revision"; then
      pass "drained revision $(revision_alias "$revision")=$revision was garbage-collected"
      return 0
    fi
    sleep 1
  done
  fail "revision $(revision_alias "$revision")=$revision still exists after ${timeout}s"
}

register_revision() {
  local name="$1" hash="$2" index
  index=${#REVISION_HASHES[@]}
  REVISION_HASHES[$index]="$hash"
  REVISION_NAMES[$index]="$name"
  echo "  ${GREEN}${BOLD}$name = $hash${RESET}"
}

revision_alias() {
  local hash="$1" i
  for ((i = 0; i < ${#REVISION_HASHES[@]}; i++)); do
    if [[ "${REVISION_HASHES[$i]}" == "$hash" ]]; then
      printf '%s' "${REVISION_NAMES[$i]}"
      return
    fi
  done
  printf '%s' "?"
}

apply_revision() {
  local alias="$1"
  shift
  local existing
  existing=$(list_revisions)
  apply_manifest "$@"
  wait_for_new_revision "$existing" 60
  register_revision "$alias" "$LAST_REVISION"
}

rollout_snapshot_json() {
  kubectl -n "$NS" get leaderworkersets -l "$SET_LABEL=$NAME" -o json 2>/dev/null \
    | jq -cS --arg revision "$REVISION_LABEL" --arg role "$ROLE_LABEL" \
      --arg initial "$INITIAL_REPLICAS_ANNOTATION" '
      [.items[]
       | select(.metadata.deletionTimestamp == null)
       | {
           revision: .metadata.labels[$revision],
           role: .metadata.labels[$role],
           initial: ((.metadata.annotations[$initial] // "") as $value
             | if ($value | test("^[0-9]+$")) then ($value | tonumber)
               else (.spec.replicas // 1)
               end),
           spec: (.spec.replicas // 1),
           statusReplicas: (.status.replicas // 0),
           rawReady: (.status.readyReplicas // 0),
           ready: ([
             (.spec.replicas // 1),
             ([0, ((.status.readyReplicas // 0) - ([0, ((.status.replicas // 0) - (.spec.replicas // 1))] | max))] | max)
           ] | min)
         }]
      | sort_by(.revision, .role)
      | group_by(.revision)
      | map(. as $rows | {
          revision: $rows[0].revision,
          prefill: {
            initial: ([$rows[] | select(.role == "prefill") | .initial] | add // 0),
            spec: ([$rows[] | select(.role == "prefill") | .spec] | add // 0),
            statusReplicas: ([$rows[] | select(.role == "prefill") | .statusReplicas] | add // 0),
            rawReady: ([$rows[] | select(.role == "prefill") | .rawReady] | add // 0),
            ready: ([$rows[] | select(.role == "prefill") | .ready] | add // 0)
          },
          decode: {
            initial: ([$rows[] | select(.role == "decode") | .initial] | add // 0),
            spec: ([$rows[] | select(.role == "decode") | .spec] | add // 0),
            statusReplicas: ([$rows[] | select(.role == "decode") | .statusReplicas] | add // 0),
            rawReady: ([$rows[] | select(.role == "decode") | .rawReady] | add // 0),
            ready: ([$rows[] | select(.role == "decode") | .ready] | add // 0)
          }
        })'
}

revision_metric() {
  local snapshot="$1" revision="$2" role="$3" field="$4"
  printf '%s' "$snapshot" | jq -r \
    --arg revision "$revision" --arg role "$role" --arg field "$field" '
      ([.[] | select(.revision == $revision) | .[$role][$field]][0] // 0)'
}

set_rollout_progress_display() {
  ROLLOUT_TARGET_P="$1"
  ROLLOUT_TARGET_D="$2"
}

percent_of() {
  local value="$1" total="$2"
  if (( total > 0 )); then
    printf '%d%%' "$((value * 100 / total))"
  else
    printf '%s' "n/a"
  fi
}

print_snapshot() {
  local snapshot="$1" elapsed="$2" target_revision="${3:-}"
  local revision p_initial p_spec p_raw_ready p_ready
  local d_initial d_spec d_raw_ready d_ready
  local alias note progress_label progress_reference
  local p_percent d_percent p_ready_percent d_ready_percent
  local p_unready_percent d_unready_percent
  local p_denominator d_denominator
  local total_p_spec total_p_status total_p_raw_ready total_p_ready
  local total_d_spec total_d_status total_d_raw_ready total_d_ready
  local usable_p_ready usable_d_ready reserved_p reserved_d wait_reason
  read -r total_p_spec total_p_status total_p_raw_ready total_p_ready total_d_spec total_d_status total_d_raw_ready total_d_ready usable_p_ready usable_d_ready <<<"$(printf '%s' "$snapshot" | jq -r '
    [([.[].prefill.spec] | add // 0),
     ([.[].prefill.statusReplicas] | add // 0),
     ([.[].prefill.rawReady] | add // 0),
     ([.[].prefill.ready] | add // 0),
     ([.[].decode.spec] | add // 0),
     ([.[].decode.statusReplicas] | add // 0),
     ([.[].decode.rawReady] | add // 0),
     ([.[].decode.ready] | add // 0),
     ([.[] | select(.prefill.ready > 0 and .decode.ready > 0) | .prefill.ready] | add // 0),
     ([.[] | select(.prefill.ready > 0 and .decode.ready > 0) | .decode.ready] | add // 0)] | @tsv')"
  echo "  ${MAGENTA}${BOLD}[t=$(printf '%3d' "$elapsed")s]${RESET} total: ${BOLD}Spec P:D=${total_p_spec}:${total_d_spec} · raw Ready P:D=${total_p_raw_ready}:${total_d_raw_ready} · committed Ready P:D=${total_p_ready}:${total_d_ready}${RESET}"
  if (( MIN_READY_P >= 0 && MIN_READY_D >= 0 )); then
    echo "           ${BOLD}planner-usable Ready P:D=${usable_p_ready}:${usable_d_ready} · floor P:D=${MIN_READY_P}:${MIN_READY_D}${RESET}"
    if (( total_p_raw_ready < MIN_READY_P || total_d_raw_ready < MIN_READY_D )); then
      echo "           ${RED}${BOLD}WARNING: raw Ready P:D=${total_p_raw_ready}:${total_d_raw_ready} is below floor P:D=${MIN_READY_P}:${MIN_READY_D}${RESET}"
    elif (( usable_p_ready < MIN_READY_P || usable_d_ready < MIN_READY_D )); then
      reserved_p=$((total_p_raw_ready - usable_p_ready))
      reserved_d=$((total_d_raw_ready - usable_d_ready))
      (( reserved_p < 0 )) && reserved_p=0
      (( reserved_d < 0 )) && reserved_d=0
      wait_reason="an incomplete revision"
      if (( total_p_status > total_p_spec || total_d_status > total_d_spec )); then
        wait_reason="an in-flight deletion"
      fi
      echo "           ${YELLOW}${BOLD}WAITING: P:D=${reserved_p}:${reserved_d} Ready capacity is reserved by ${wait_reason}; no further drain is safe yet${RESET}"
      echo "                    ${YELLOW}planner-usable=${usable_p_ready}:${usable_d_ready} < floor=${MIN_READY_P}:${MIN_READY_D}; raw Ready remains=${total_p_raw_ready}:${total_d_raw_ready}${RESET}"
    fi
  fi
  while IFS=$'\t' read -r revision p_initial p_spec p_raw_ready p_ready d_initial d_spec d_raw_ready d_ready; do
    [[ -n "$revision" ]] || continue
    alias=$(revision_alias "$revision")
    note=""
    if (( (p_spec == 0 && d_spec > 0) || (p_spec > 0 && d_spec == 0) )); then
      note=" ${YELLOW}← incomplete revision${RESET}"
    fi
    p_denominator="$p_initial"
    d_denominator="$d_initial"
    progress_label="remaining"
    progress_reference="baseline"
    if [[ "$revision" == "$target_revision" ]]; then
      p_denominator="$ROLLOUT_TARGET_P"
      d_denominator="$ROLLOUT_TARGET_D"
      progress_label="built"
      progress_reference="target"
    else
      (( p_denominator > 0 )) || p_denominator="$ROLLOUT_TARGET_P"
      (( d_denominator > 0 )) || d_denominator="$ROLLOUT_TARGET_D"
    fi
    p_percent=$(percent_of "$p_spec" "$p_denominator")
    d_percent=$(percent_of "$d_spec" "$d_denominator")
    p_ready_percent=$(percent_of "$p_ready" "$p_denominator")
    d_ready_percent=$(percent_of "$d_ready" "$d_denominator")
    p_unready_percent=$(percent_of "$((p_spec - p_ready))" "$p_denominator")
    d_unready_percent=$(percent_of "$((d_spec - d_ready))" "$d_denominator")
    printf "      %s(%s)  Spec P:D=%d:%d · raw Ready=%d:%d · committed Ready=%d:%d\n" \
      "$alias" "$revision" "$p_spec" "$d_spec" "$p_raw_ready" "$d_raw_ready" "$p_ready" "$d_ready"
    printf "                   %s%s against %s %dP/%dD · Spec P:D=%s:%s · committed Ready=%s:%s · unready=%s:%s%s%b\n" \
      "$DIM" "$progress_label" "$progress_reference" "$p_denominator" "$d_denominator" \
      "$p_percent" "$d_percent" "$p_ready_percent" "$d_ready_percent" \
      "$p_unready_percent" "$d_unready_percent" "$RESET" "$note"
  done < <(printf '%s' "$snapshot" | jq -r \
    '.[] | [.revision,.prefill.initial,.prefill.spec,.prefill.rawReady,.prefill.ready,.decode.initial,.decode.spec,.decode.rawReady,.decode.ready] | @tsv')
}

reset_observations() {
	OBSERVED_PENDING=0
	OBSERVED_BOOTSTRAP_SURGE=0
	OBSERVED_INCOMPLETE_OLD=0
	INCOMPLETE_OLD_STREAK=0
	MAX_INCOMPLETE_OLD_STREAK=0
	MAX_DRAINED_OLD_REVISIONS=0
}

record_observations() {
  local snapshot="$1" target_revision="$2"
  local drained_old_revisions p_spec d_spec
  read -r p_spec d_spec <<<"$(printf '%s' "$snapshot" | jq -r '
    [([.[].prefill.spec] | add // 0), ([.[].decode.spec] | add // 0)] | @tsv')"
  if printf '%s' "$snapshot" | jq -e 'any(.[]; .prefill.spec > .prefill.ready or .decode.spec > .decode.ready)' >/dev/null; then
    OBSERVED_PENDING=1
  fi
  if (( (MAX_SPEC_P >= 0 && p_spec > MAX_SPEC_P) || (MAX_SPEC_D >= 0 && d_spec > MAX_SPEC_D) )); then
    OBSERVED_BOOTSTRAP_SURGE=1
  fi
  if printf '%s' "$snapshot" | jq -e --arg target "$target_revision" '
      any(.[];
        .revision != $target
        and ((.prefill.spec == 0 and .decode.spec > 0)
          or (.prefill.spec > 0 and .decode.spec == 0)))' >/dev/null; then
    OBSERVED_INCOMPLETE_OLD=1
    INCOMPLETE_OLD_STREAK=$((INCOMPLETE_OLD_STREAK + 1))
    if (( INCOMPLETE_OLD_STREAK > MAX_INCOMPLETE_OLD_STREAK )); then
      MAX_INCOMPLETE_OLD_STREAK=$INCOMPLETE_OLD_STREAK
    fi
  else
    INCOMPLETE_OLD_STREAK=0
  fi
  drained_old_revisions=$(printf '%s' "$snapshot" | jq -r --arg target "$target_revision" '
    [.[] | select(.revision != $target and .prefill.spec == 0 and .decode.spec == 0)] | length')
  if (( drained_old_revisions > MAX_DRAINED_OLD_REVISIONS )); then
    MAX_DRAINED_OLD_REVISIONS=$drained_old_revisions
  fi
}

set_bounds() {
  local initial_p="$1" initial_d="$2" target_p="$3" target_d="$4"
  local p_surge="$5" p_unavailable="$6" d_surge="$7" d_unavailable="$8"
  MAX_SPEC_P=$(( (initial_p > target_p ? initial_p : target_p) + p_surge ))
  MAX_SPEC_D=$(( (initial_d > target_d ? initial_d : target_d) + d_surge ))
  BOOTSTRAP_SPEC_P=0
  BOOTSTRAP_SPEC_D=0
  MIN_READY_P=$(( (initial_p < target_p ? initial_p : target_p) - p_unavailable ))
  MIN_READY_D=$(( (initial_d < target_d ? initial_d : target_d) - d_unavailable ))
  (( MIN_READY_P < 0 )) && MIN_READY_P=0
  (( MIN_READY_D < 0 )) && MIN_READY_D=0
  info "hard bounds: P spec<=${MAX_SPEC_P}, decision-time ready>=${MIN_READY_P}; D spec<=${MAX_SPEC_D}, decision-time ready>=${MIN_READY_D}"
}

allow_bootstrap_surge() {
  local role="$1"
  case "$role" in
    prefill) BOOTSTRAP_SPEC_P=1 ;;
    decode) BOOTSTRAP_SPEC_D=1 ;;
    *) fail "unknown bootstrap-surge role '$role'" ;;
  esac
  info "bootstrap exception: $role Spec may temporarily exceed its configured surge ceiling by one"
}

clear_bounds() {
  MAX_SPEC_P=-1
  MAX_SPEC_D=-1
  BOOTSTRAP_SPEC_P=0
  BOOTSTRAP_SPEC_D=0
  MIN_READY_P=-1
  MIN_READY_D=-1
}

set_fraction_check() {
  FRACTION_REVISION="$1"
  FRACTION_TARGET_P="$2"
  FRACTION_TARGET_D="$3"
  CHECK_FRACTION=1
  FRACTION_SKEW_STREAK=0
  local min_target="$FRACTION_TARGET_P"
  (( FRACTION_TARGET_D < min_target )) && min_target="$FRACTION_TARGET_D"
  info "fractional skew bound: <= 1/$min_target between new P and D progress"
}

clear_fraction_check() {
  CHECK_FRACTION=0
  FRACTION_REVISION=""
  FRACTION_SKEW_STREAK=0
}

check_invariants() {
  local snapshot="$1" p_spec p_ready d_spec d_ready
  read -r p_spec p_ready d_spec d_ready <<<"$(printf '%s' "$snapshot" | jq -r '
    [([.[].prefill.spec] | add // 0),
     ([.[].prefill.ready] | add // 0),
     ([.[].decode.spec] | add // 0),
     ([.[].decode.ready] | add // 0)] | @tsv')"

  if (( MAX_SPEC_P >= 0 && p_spec > MAX_SPEC_P + BOOTSTRAP_SPEC_P )); then
    fail "prefill surge ceiling exceeded: $p_spec > $((MAX_SPEC_P + BOOTSTRAP_SPEC_P))"
  fi
  if (( MAX_SPEC_D >= 0 && d_spec > MAX_SPEC_D + BOOTSTRAP_SPEC_D )); then
    fail "decode surge ceiling exceeded: $d_spec > $((MAX_SPEC_D + BOOTSTRAP_SPEC_D))"
  fi
  # Do not assert a Ready floor from an arbitrary API sample. Readiness may
  # independently fall after the controller made its drain decision, and a
  # sample may observe only part of a multi-object update. Deterministic unit
  # tests assert the Ready values used by the planner at decision time. This
  # watcher still enforces Spec ceilings, rollout ordering/completeness, and
  # full Ready state at convergence.

  if (( CHECK_FRACTION )) && (( FRACTION_TARGET_P > 0 && FRACTION_TARGET_D > 0 )); then
    local current_p current_d delta min_target lhs rhs
    current_p=$(revision_metric "$snapshot" "$FRACTION_REVISION" prefill spec)
    current_d=$(revision_metric "$snapshot" "$FRACTION_REVISION" decode spec)
    delta=$((current_p * FRACTION_TARGET_D - current_d * FRACTION_TARGET_P))
    (( delta < 0 )) && delta=$((-delta))
    min_target="$FRACTION_TARGET_P"
    (( FRACTION_TARGET_D < min_target )) && min_target="$FRACTION_TARGET_D"
    lhs=$((delta * min_target))
    rhs=$((FRACTION_TARGET_P * FRACTION_TARGET_D))
    if (( lhs > rhs )); then
      # Role LWS patches are sequential. A one-second sample may therefore see
      # only half of a valid planner step, especially for large rollouts. Fail
      # only if the out-of-window state survives several observations.
      FRACTION_SKEW_STREAK=$((FRACTION_SKEW_STREAK + 1))
      if (( FRACTION_SKEW_STREAK > 3 )); then
        fail "fractional skew persisted for ${FRACTION_SKEW_STREAK}s for $FRACTION_REVISION: P=$current_p/$FRACTION_TARGET_P D=$current_d/$FRACTION_TARGET_D"
      fi
    else
      FRACTION_SKEW_STREAK=0
    fi
  fi

  check_drain_order "$snapshot"
}

rollout_complete() {
  local snapshot="$1" target_revision="$2" target_p="$3" target_d="$4"
  printf '%s' "$snapshot" | jq -e \
    --arg revision "$target_revision" --argjson p "$target_p" --argjson d "$target_d" '
      any(.[];
        .revision == $revision
        and .prefill.spec == $p and .prefill.ready == $p
        and .decode.spec == $d and .decode.ready == $d)
      and all(.[];
        .revision == $revision
        or (.prefill.spec == 0 and .decode.spec == 0))' >/dev/null
}

watch_until_complete() {
  local target_revision="$1" target_p="$2" target_d="$3" timeout="${4:-600}"
  local deadline=$((SECONDS + timeout)) start=$SECONDS last="" snapshot elapsed
  set_rollout_progress_display "$target_p" "$target_d"
  while (( SECONDS < deadline )); do
    snapshot=$(rollout_snapshot_json)
    record_observations "$snapshot" "$target_revision"
    check_invariants "$snapshot"
    if [[ "$snapshot" != "$last" ]]; then
      elapsed=$((SECONDS - start))
      print_snapshot "$snapshot" "$elapsed" "$target_revision"
      last="$snapshot"
    fi
    if rollout_complete "$snapshot" "$target_revision" "$target_p" "$target_d"; then
      pass "converged to $(revision_alias "$target_revision")=$target_revision at ${target_p}P/${target_d}D in $((SECONDS - start))s"
      return 0
    fi
    sleep 1
  done
  fail "rollout did not converge within ${timeout}s"
}

observe_for() {
  local seconds="$1" target_revision="$2" target_p="$3" target_d="$4"
  local deadline=$((SECONDS + seconds)) start=$SECONDS last="" snapshot elapsed
  set_rollout_progress_display "$target_p" "$target_d"
  while (( SECONDS < deadline )); do
    snapshot=$(rollout_snapshot_json)
    record_observations "$snapshot" "$target_revision"
    check_invariants "$snapshot"
    if [[ "$snapshot" != "$last" ]]; then
      elapsed=$((SECONDS - start))
      print_snapshot "$snapshot" "$elapsed" "$target_revision"
      last="$snapshot"
    fi
    sleep 1
  done
}

# Watch a revision until every role has reached the requested fraction of its
# target Spec. This lets interrupted-rollout scenarios trigger the next
# revision from observed progress instead of relying on machine-dependent
# timing.
watch_until_revision_progress() {
  local revision="$1" target_p="$2" target_d="$3"
  local numerator="$4" denominator="$5" timeout="${6:-900}"
  local deadline=$((SECONDS + timeout)) start=$SECONDS last="" snapshot elapsed
  local current_p current_d
  set_rollout_progress_display "$target_p" "$target_d"
  info "waiting for $(revision_alias "$revision") to reach at least ${numerator}/${denominator} of ${target_p}P/${target_d}D"
  while (( SECONDS < deadline )); do
    snapshot=$(rollout_snapshot_json)
    record_observations "$snapshot" "$revision"
    check_invariants "$snapshot"
    if [[ "$snapshot" != "$last" ]]; then
      elapsed=$((SECONDS - start))
      print_snapshot "$snapshot" "$elapsed" "$revision"
      last="$snapshot"
    fi
    current_p=$(revision_metric "$snapshot" "$revision" prefill spec)
    current_d=$(revision_metric "$snapshot" "$revision" decode spec)
    if (( current_p * denominator >= target_p * numerator &&
          current_d * denominator >= target_d * numerator )); then
      pass "$(revision_alias "$revision") reached ${current_p}P/${current_d}D; continuing at approximately ${numerator}/${denominator} progress"
      return 0
    fi
    sleep 1
  done
  fail "revision $(revision_alias "$revision") did not reach ${numerator}/${denominator} progress within ${timeout}s"
}

current_total_specs() {
  local snapshot
  snapshot=$(rollout_snapshot_json)
  read -r CURRENT_SPEC_P CURRENT_SPEC_D <<<"$(printf '%s' "$snapshot" | jq -r '
    [([.[].prefill.spec] | add // 0), ([.[].decode.spec] | add // 0)] | @tsv')"
}

set_drain_order() {
  local snapshot hash index
  snapshot=$(rollout_snapshot_json)
  DRAIN_HASHES=("$@")
  DRAIN_BASE_P=()
  DRAIN_BASE_D=()
  for ((index = 0; index < ${#DRAIN_HASHES[@]}; index++)); do
    hash="${DRAIN_HASHES[$index]}"
    DRAIN_BASE_P[$index]=$(revision_metric "$snapshot" "$hash" prefill spec)
    DRAIN_BASE_D[$index]=$(revision_metric "$snapshot" "$hash" decode spec)
  done
  info "expected old-revision drain order: $(for hash in "${DRAIN_HASHES[@]}"; do printf '%s ' "$(revision_alias "$hash")"; done)"
}

clear_drain_order() {
  DRAIN_HASHES=()
  DRAIN_BASE_P=()
  DRAIN_BASE_D=()
}

check_drain_order() {
  local snapshot="$1" active=-1 index hash p d
  (( ${#DRAIN_HASHES[@]} > 0 )) || return 0
  for ((index = 0; index < ${#DRAIN_HASHES[@]}; index++)); do
    hash="${DRAIN_HASHES[$index]}"
    p=$(revision_metric "$snapshot" "$hash" prefill spec)
    d=$(revision_metric "$snapshot" "$hash" decode spec)
    if (( active < 0 && p + d > 0 )); then
      active=$index
      continue
    fi
    if (( active >= 0 && index > active )); then
      if (( p != DRAIN_BASE_P[index] || d != DRAIN_BASE_D[index] )); then
        fail "$(revision_alias "$hash") drained before newer old revision $(revision_alias "${DRAIN_HASHES[$active]}") reached zero"
      fi
    fi
  done
}

assert_eq() {
  local expected="$1" actual="$2" description="$3"
  if [[ "$expected" != "$actual" ]]; then
    fail "$description: expected '$expected', got '$actual'"
  fi
  pass "$description"
}

assert_observed_pending() {
  (( OBSERVED_PENDING == 1 )) || fail "the watcher never observed issued-but-not-Ready work"
  pass "observed issued work waiting for readiness"
}

assert_observed_bootstrap_surge() {
  (( OBSERVED_BOOTSTRAP_SURGE == 1 )) || fail "the watcher never observed the one-replica bootstrap surge"
  pass "observed exactly the bounded bootstrap-surge allowance"
}

assert_observed_incomplete_old() {
  (( OBSERVED_INCOMPLETE_OLD == 1 )) || fail "the watcher never observed the fallback's incomplete old revision"
  pass "observed the documented incomplete-old-revision fallback"
}

assert_no_persistent_incomplete_old() {
  (( MAX_INCOMPLETE_OLD_STREAK <= 3 )) || fail "an old revision stayed incomplete for ${MAX_INCOMPLETE_OLD_STREAK}s"
  pass "no old revision stayed incomplete (brief sequential API updates are tolerated)"
}

assert_bounded_drained_revisions() {
  (( MAX_DRAINED_OLD_REVISIONS <= 1 )) || fail "observed ${MAX_DRAINED_OLD_REVISIONS} fully drained old revisions at once"
  pass "at most one fully drained old revision was retained"
}

finish_scenario() {
  banner "PASS · $1" "$GREEN"
  pause_phase "Inspect the final state, then press Enter to clean up"
}

dump_state() {
  echo
  echo "--- DisaggregatedSet"
  kubectl -n "$NS" get "$DS_RESOURCE" "$NAME" -o yaml 2>/dev/null || true
  echo "--- LeaderWorkerSets"
  kubectl -n "$NS" get leaderworkersets -l "$SET_LABEL=$NAME" -o wide 2>/dev/null || true
  echo "--- Pods"
  kubectl -n "$NS" get pods -l "$SET_LABEL=$NAME" -L "$ROLE_LABEL,$REVISION_LABEL" 2>/dev/null || true
  echo "--- Controller logs (tail)"
  kubectl -n lws-system logs deployment/lws-controller-manager --tail=100 2>/dev/null || true
}
