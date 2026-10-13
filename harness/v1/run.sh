#!/usr/bin/env bash
# Build the pinned controller, run the lab, and remove only this run's cluster.
# Bash 3.2 compatible; no changes to the user's Kubernetes context.
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY="$(cd "$HARNESS_DIR/../.." && pwd)"
SUITE_ARGS=()
INTERACTIVE=0
OPEN_K9S=1
LIGHTWEIGHT=0
SHOW_HELP=0
for arg in "$@"; do
  case "$arg" in
    --interactive) INTERACTIVE=1; SUITE_ARGS+=("$arg") ;;
    --auto) INTERACTIVE=0; SUITE_ARGS+=("$arg") ;;
    --no-k9s) OPEN_K9S=0 ;;
    --help|-h) LIGHTWEIGHT=1; SHOW_HELP=1; SUITE_ARGS+=("$arg") ;;
    --list|--validate) LIGHTWEIGHT=1; SUITE_ARGS+=("$arg") ;;
    *) SUITE_ARGS+=("$arg") ;;
  esac
done
if (( ${#SUITE_ARGS[@]} == 0 )); then LIGHTWEIGHT=1; SUITE_ARGS=(--list); fi
if (( LIGHTWEIGHT )); then
  python3 "$HARNESS_DIR/suite.py" "${SUITE_ARGS[@]}"
  if (( SHOW_HELP )); then
    printf '\nRuntime: --no-k9s disables the viewer in interactive mode.\n'
    printf 'KIND, KIND_NODE_IMAGE, K9S_COMMAND, TERMINAL_COMMAND are configurable; see README.md.\n'
  fi
  exit 0
fi
python3 "$HARNESS_DIR/suite.py" "${SUITE_ARGS[@]}" --validate
(( INTERACTIVE )) || OPEN_K9S=0

fail() { printf '\033[31;1mERROR: %s\033[0m\n' "$*" >&2; exit 1; }
section() { printf '\n\033[36;1m━━ %s ━━\033[0m\n' "$*"; }
for tool in python3 docker kubectl git go jq tar; do
  command -v "$tool" >/dev/null || fail "Missing $tool. Install it before running the harness."
done
if (( INTERACTIVE )) && [[ ! -t 0 ]]; then fail 'Interactive mode needs a terminal. Use --auto for unattended runs.'; fi
KIND="${KIND:-$(command -v kind || true)}"
[[ -n "$KIND" ]] || fail 'Install kind on PATH, or set KIND=/absolute/path/to/kind.'
command -v "$KIND" >/dev/null || fail "Cannot execute kind: $KIND"
K9S_COMMAND="${K9S_COMMAND:-k9s}"
TERMINAL_COMMAND="${TERMINAL_COMMAND:-}"
if (( OPEN_K9S )); then
  K9S_COMMAND="$(command -v "$K9S_COMMAND")" || fail 'Install k9s, set K9S_COMMAND to an executable wrapper, or use --no-k9s.'
  if [[ -n "$TERMINAL_COMMAND" ]]; then
    TERMINAL_COMMAND="$(command -v "$TERMINAL_COMMAND")" || fail 'TERMINAL_COMMAND must name an executable wrapper.'
  elif [[ "$(uname -s)" != Darwin ]]; then
    fail 'Set TERMINAL_COMMAND to a new-window terminal wrapper, or use --no-k9s. The default viewer is macOS Ghostty.'
  elif ! osascript -e 'id of application "Ghostty"' >/dev/null 2>&1; then
    fail 'Install Ghostty, set TERMINAL_COMMAND, or use --no-k9s.'
  fi
fi
docker info >/dev/null || fail 'Docker is not available.'
COMMIT="$(jq -er '.commit | select(test("^[0-9a-f]{40}$"))' "$HARNESS_DIR/baseline.json")" || fail 'baseline.json must contain a full controller commit SHA.'
git -C "$REPOSITORY" cat-file -e "$COMMIT^{commit}" || fail "Pinned controller $COMMIT is missing. Fetch the harness branch without --depth=1."
[[ -d "$HARNESS_DIR/observer" ]] || fail 'Missing shared-observer helper source.'

mkdir -p "$HARNESS_DIR/.state"
RUN_DIR="$(mktemp -d "$HARNESS_DIR/.state/run.XXXXXX")"
TOKEN="$(basename "$RUN_DIR" | tr '[:upper:].' '[:lower:]-')"
CLUSTER="lws-v1-$TOKEN"
CONTEXT="kind-$CLUSTER"
NODE="$CLUSTER-control-plane"
KCFG="$RUN_DIR/kubeconfig"
IMAGE="lws-v1-harness:$COMMIT"
NODE_ID=''
CLUSTER_OWNED=0
VIEWER_TERMINAL=''
DRIVER_PID=''
RUN_PASSED=0
printf '%s\n' "$TOKEN" > "$RUN_DIR/owner"
printf '%s\n' "$RUN_DIR" > "$HARNESS_DIR/.state/last-run"
k() { kubectl --kubeconfig "$KCFG" --context "$CONTEXT" --request-timeout=10s "$@"; }

owns_node() {
  local id="$1" label="$2" mount="$3"
  [[ "$label" == "$CLUSTER" && ( -z "$NODE_ID" || "$id" == "$NODE_ID" ) ]] || return 1
  [[ "$mount" == "$RUN_DIR/owner" ]] && return 0
  # Docker Desktop reports macOS bind sources through its VM's /host_mnt.
  [[ "$(uname -s)" == Darwin && "$mount" == "/host_mnt$RUN_DIR/owner" ]]
}

cleanup() {
  local status=$? owner='' viewer_pid='' actual_id='' cluster_label='' owner_mount='' deadline
  trap - EXIT
  trap '' INT TERM HUP PIPE
  set +e
  if [[ -n "$DRIVER_PID" ]]; then
    kill -TERM "$DRIVER_PID" 2>/dev/null
    # Allow the suite's finally blocks to restore temporary local settings.
    # Its isolated process group also owns every observer/kubectl subprocess.
    deadline=$((SECONDS + 30))
    while kill -0 -- "-$DRIVER_PID" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.2; done
    kill -KILL -- "-$DRIVER_PID" 2>/dev/null
    wait "$DRIVER_PID" 2>/dev/null
  fi
  if [[ -f "$RUN_DIR/k9s.pid" ]]; then
    read -r viewer_pid < "$RUN_DIR/k9s.pid"
    if [[ "$viewer_pid" =~ ^[0-9]+$ ]] && ps -p "$viewer_pid" -o command= | grep -Fq -- "$KCFG"; then
      kill -TERM "$viewer_pid" 2>/dev/null
    fi
  fi
  if [[ -n "$VIEWER_TERMINAL" ]]; then
    osascript - "$VIEWER_TERMINAL" >/dev/null 2>&1 <<'APPLESCRIPT'
on run argv
  with timeout of 5 seconds
    if application "Ghostty" is running then
      tell application "Ghostty"
        repeat with labTerminal in terminals
          if id of labTerminal is item 1 of argv then close labTerminal
        end repeat
      end tell
    end if
  end timeout
end run
APPLESCRIPT
  fi
  read -r owner < "$RUN_DIR/owner"
  if (( CLUSTER_OWNED )) && [[ "$owner" == "$TOKEN" ]]; then
    actual_id=$(docker inspect --type container --format '{{.Id}}' "$NODE" 2>/dev/null)
    cluster_label=$(docker inspect --type container --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$NODE" 2>/dev/null)
    owner_mount=$(docker inspect --type container --format '{{range .Mounts}}{{if eq .Destination "/var/run/lws-v1-harness-owner"}}{{.Source}}{{end}}{{end}}' "$NODE" 2>/dev/null)
    if [[ -n "$actual_id" ]] && ! owns_node "$actual_id" "$cluster_label" "$owner_mount"; then
      printf 'REFUSING cleanup: %s is no longer this run\047s node.\n' "$NODE" >&2
      status=1
    else
      section "Remove owned kind cluster $CLUSTER"
      if [[ -s "$KCFG" ]]; then
        k -n v1-harness get disaggregatedsets,lws,pods -o yaml > "$RUN_DIR/final-resources.yaml" 2>&1
        k -n v1-harness get events --sort-by=.lastTimestamp > "$RUN_DIR/events.log" 2>&1
        k -n lws-system logs deployment/lws-controller-manager --all-containers --tail=3000 > "$RUN_DIR/controller.log" 2>&1
      fi
      "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KCFG" > "$RUN_DIR/cleanup.log" 2>&1 || status=1
      if docker inspect --type container "$NODE" >/dev/null 2>&1; then
        printf 'Cleanup did not remove %s; inspect %s/cleanup.log.\n' "$NODE" "$RUN_DIR" >&2
        status=1
      fi
    fi
  elif (( CLUSTER_OWNED )); then
    printf 'REFUSING cleanup: run ownership token changed.\n' >&2
    status=1
  fi
  if [[ "$status" == 0 && "$RUN_PASSED" != 1 ]]; then status=1; fi
  jq -n --argjson exitCode "$status" --argjson suitePassed "$RUN_PASSED" \
    --arg cluster "$CLUSTER" '{exitCode:$exitCode,suitePassed:($suitePassed==1),cluster:$cluster}' > "$RUN_DIR/runtime-result.json"
  printf '\nArtifacts retained: %s\n' "$RUN_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
exec > >(tee "$RUN_DIR/transcript.log") 2>&1
export CLICOLOR_FORCE=1
section "LWS v1 regression lab · controller ${COMMIT:0:12}"
printf 'Artifacts: %s\nPrivate context: %s\n' "$RUN_DIR" "$CONTEXT"

case "$(docker info --format '{{.Architecture}}')" in
  arm64|aarch64) ARCH=arm64 ;;
  amd64|x86_64) ARCH=amd64 ;;
  *) fail 'Docker must use arm64 or amd64.' ;;
esac
mkdir "$RUN_DIR/source" "$RUN_DIR/bin" "$RUN_DIR/build"
git -C "$REPOSITORY" archive "$COMMIT" | tar -x -C "$RUN_DIR/source"
section 'Build the observer helper and pinned controller (two Go build jobs)'
(cd "$REPOSITORY"; GOMAXPROCS=2 go build -tags=harness -mod=readonly -p=2 -trimpath -o "$RUN_DIR/bin/observe" ./harness/v1/observer)
if [[ "$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' 2>/dev/null || true)" != "$COMMIT" ||
      "$(docker image inspect "$IMAGE" --format '{{.Architecture}}' 2>/dev/null || true)" != "$ARCH" ]]; then
  (cd "$RUN_DIR/source"
    CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" GOMAXPROCS=2 go build -mod=readonly -p=2 -trimpath \
      -ldflags "-X sigs.k8s.io/lws/pkg/version.GitCommit=$COMMIT" -o "$RUN_DIR/build/manager" ./cmd)
  docker build --platform "linux/$ARCH" --provenance=false --build-arg "REVISION=$COMMIT" \
    -t "$IMAGE" -f - "$RUN_DIR/build" <<'DOCKERFILE'
FROM gcr.io/distroless/static:nonroot
ARG REVISION
LABEL org.opencontainers.image.revision=$REVISION
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
DOCKERFILE
fi

section 'Create a fresh isolated kind cluster'
clusters="$("$KIND" get clusters)" || fail 'Cannot list kind clusters.'
if printf '%s\n' "$clusters" | grep -Fxq "$CLUSTER" || docker inspect --type container "$NODE" >/dev/null 2>&1; then
  fail "The generated cluster name $CLUSTER already exists; refusing to reuse it."
fi
# Claim the unique name before creation, so interrupted creation is cleaned too.
jq -n --arg owner "$RUN_DIR/owner" '{
  kind:"Cluster",apiVersion:"kind.x-k8s.io/v1alpha4",
  nodes:[{role:"control-plane",extraMounts:[{
    hostPath:$owner,containerPath:"/var/run/lws-v1-harness-owner",readOnly:true
  }]}]
}' > "$RUN_DIR/kind-config.json"
CLUSTER_OWNED=1
"$KIND" create cluster --name "$CLUSTER" --config "$RUN_DIR/kind-config.json" \
  --image "${KIND_NODE_IMAGE:-kindest/node:v1.36.1}" --kubeconfig "$KCFG" --wait 2m
NODE_ID="$(docker inspect --type container --format '{{.Id}}' "$NODE")"
chmod 600 "$KCFG"
[[ "$(k config current-context)" == "$CONTEXT" && "$(k config get-contexts -o name)" == "$CONTEXT" ]] || fail 'Private kubeconfig has an unexpected context.'
server="$(k config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" =~ ^https://127\.0\.0\.1:[0-9]+$ ]] || fail 'The harness API endpoint is not localhost.'
k label node "$NODE" "harness.lws.x-k8s.io/run=$TOKEN" >/dev/null
jq -n --arg commit "$COMMIT" --arg cluster "$CLUSTER" --arg node "$NODE" --arg nodeID "$NODE_ID" --arg token "$TOKEN" \
  '{controllerCommit:$commit,cluster:$cluster,node:$node,nodeID:$nodeID,owner:$token}' > "$RUN_DIR/runtime.json"
docker image inspect busybox:1.36 >/dev/null 2>&1 || docker pull busybox:1.36
docker image save "$IMAGE" busybox:1.36 | docker exec --privileged -i "$NODE" \
  ctr --namespace=k8s.io images import --platform "linux/$ARCH" --digests --snapshotter=overlayfs -

section 'Install the pinned CRDs, admission webhooks, and controller'
jq -n --arg tag "$COMMIT" '{
  apiVersion:"kustomize.config.k8s.io/v1beta1", kind:"Kustomization", resources:["source/config/default"],
  images:[{name:"us-central1-docker.pkg.dev/k8s-staging-images/lws/lws",newName:"lws-v1-harness",newTag:$tag}],
  replicas:[{name:"lws-controller-manager",count:1}],
  patches:[{target:{kind:"Deployment",name:"lws-controller-manager"},
    patch:"- op: replace\n  path: /spec/template/spec/containers/0/resources/requests\n  value: {cpu: 100m, memory: 128Mi}\n"}]
}' > "$RUN_DIR/kustomization.yaml"
k apply --server-side -k "$RUN_DIR" > "$RUN_DIR/install.log"
k wait --for=condition=Established --timeout=90s \
  crd/disaggregatedsets.disaggregatedset.x-k8s.io crd/leaderworkersets.leaderworkerset.x-k8s.io
k -n lws-system rollout status deployment/lws-controller-manager --timeout=180s
[[ "$(k -n lws-system get deployment lws-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')" == "$IMAGE" ]] || fail 'Controller image does not match the pinned build.'
k -n kube-system wait pods -l tier=control-plane --for=condition=Ready --timeout=90s
k create namespace v1-harness

if (( OPEN_K9S )); then
  section 'Open read-only k9s in a separate window · :pods / :lws'
  if [[ -n "$TERMINAL_COMMAND" ]]; then
    "$TERMINAL_COMMAND" /bin/bash --noprofile --norc "$HARNESS_DIR/view.sh" "$RUN_DIR" "$CONTEXT" "$K9S_COMMAND" > "$RUN_DIR/terminal.log" 2>&1 &
  else
    if ! VIEWER_TERMINAL="$(osascript "$HARNESS_DIR/ghostty.applescript" "$HARNESS_DIR/view.sh" "$RUN_DIR" "$CONTEXT" "$K9S_COMMAND" 2> "$RUN_DIR/ghostty.log")"; then
      fail "Ghostty launch failed or did not respond within 15s. Use --no-k9s or configure TERMINAL_COMMAND; details: $RUN_DIR/ghostty.log"
    fi
  fi
  deadline=$((SECONDS + 15))
  until [[ -s "$RUN_DIR/k9s.pid" ]]; do
    (( SECONDS < deadline )) || fail 'The k9s viewer did not start. Check its window or terminal.log.'
    sleep 0.2
  done
  read -r viewer_pid < "$RUN_DIR/k9s.pid"
  sleep 1
  [[ "$viewer_pid" =~ ^[0-9]+$ ]] && ps -p "$viewer_pid" -o command= | grep -Fq -- "$KCFG" || fail 'The k9s viewer exited during startup.'
fi

section 'Run scenarios'
# setsid isolates cancellation without taking stdin away from manual checkpoints.
python3 -c 'import os, sys; os.setsid(); os.execv(sys.executable, [sys.executable, *sys.argv[1:]])' \
  "$HARNESS_DIR/suite.py" "${SUITE_ARGS[@]}" \
  --kubeconfig "$KCFG" --context "$CONTEXT" --run-dir "$RUN_DIR" --node "$NODE" \
  --controller-commit "$COMMIT" --observer "$RUN_DIR/bin/observe" <&0 &
DRIVER_PID=$!
wait "$DRIVER_PID"
DRIVER_PID=''
RUN_PASSED=1
