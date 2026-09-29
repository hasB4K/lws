#!/usr/bin/env bash
# Build PR #907 and deploy it into an isolated kind cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LWS_DIR="${LWS_DIR:-$(cd "$SCRIPT_DIR/.." 2>/dev/null && pwd || true)}"
IMG="${IMG:-lws-planner3:test}"
CLUSTER="${CLUSTER:-viz-planner3}"
KUBECONFIG_FILE="$SCRIPT_DIR/.kube-planner3"
RECREATE=0
TEARDOWN_ONLY=0
SKIP_BUILD=0

RED=$'\033[31m'; GREEN=$'\033[32m'; BLUE=$'\033[34m'; DIM=$'\033[2m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

usage() {
  cat <<'EOF'
Usage: ./install.sh [--recreate | --teardown] [--skip-build]

  --recreate    delete and recreate the isolated kind cluster
  --teardown    delete the isolated cluster and kubeconfig, then exit
  --skip-build  reuse an image already loaded into the existing cluster
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --recreate) RECREATE=1 ;;
    --teardown|teardown) TEARDOWN_ONLY=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "${RED}error: unknown argument '$1'${RESET}" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

for required_cmd in kind docker kubectl jq go; do
  command -v "$required_cmd" >/dev/null 2>&1 || {
    echo "${RED}error: '$required_cmd' is not installed${RESET}" >&2
    exit 1
  }
done

teardown() {
  echo "${BLUE}${BOLD}==> Removing kind cluster '$CLUSTER'${RESET}"
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG_FILE"
  echo "${GREEN}✓ cluster and isolated kubeconfig removed${RESET}"
}

if (( TEARDOWN_ONLY )); then
  teardown
  exit 0
fi
if (( RECREATE )); then
  teardown
fi

if kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER"; then
  echo "${DIM}==> kind cluster '$CLUSTER' already exists${RESET}"
else
  echo "${BLUE}${BOLD}==> Creating kind cluster '$CLUSTER'${RESET}"
  kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 2m
fi

kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"
export KUBECONFIG="$KUBECONFIG_FILE"

if [[ -z "$LWS_DIR" || ! -d "$LWS_DIR" ]]; then
  echo "${RED}error: LWS_DIR was not found; set LWS_DIR=/path/to/lws${RESET}" >&2
  exit 1
fi

if (( ! SKIP_BUILD )); then
  docker_arch=$(docker info --format '{{.Architecture}}')
  case "$docker_arch" in
    arm64|aarch64) platform=linux/arm64; goarch=arm64 ;;
    amd64|x86_64) platform=linux/amd64; goarch=amd64 ;;
    *) echo "${RED}error: unsupported Docker architecture '$docker_arch'${RESET}" >&2; exit 1 ;;
  esac

  build_jobs="${BUILD_JOBS:-2}"
  echo "${BLUE}${BOLD}==> Building the Linux controller from $LWS_DIR (${build_jobs} jobs)${RESET}"
  GOMAXPROCS="$build_jobs" GOFLAGS="${GOFLAGS:-} -p=$build_jobs" \
    make -C "$LWS_DIR" build \
      GO_BUILD_ENV="CGO_ENABLED=0 GOOS=linux GOARCH=$goarch"

  echo "${BLUE}${BOLD}==> Packaging $IMG${RESET}"
  docker buildx build \
    --progress=plain \
    --platform="${PLATFORMS:-$platform}" \
    --build-arg BASE_IMAGE="${BASE_IMAGE:-gcr.io/distroless/static:nonroot}" \
    --load \
    -t "$IMG" \
    -f "$SCRIPT_DIR/Dockerfile.controller" \
    "$LWS_DIR"

  echo "${BLUE}${BOLD}==> Loading image into kind${RESET}"
  kind load docker-image "$IMG" --name "$CLUSTER"
fi

echo "${BLUE}${BOLD}==> Deploying CRDs and controller${RESET}"
manager_kustomization="$LWS_DIR/config/manager/kustomization.yaml"
manager_kustomization_backup=$(mktemp)
cp "$manager_kustomization" "$manager_kustomization_backup"
restore_manager_kustomization() {
  cp "$manager_kustomization_backup" "$manager_kustomization"
  rm -f "$manager_kustomization_backup"
}
trap restore_manager_kustomization EXIT
make -C "$LWS_DIR" deploy IMG="$IMG"
restore_manager_kustomization
trap - EXIT
kubectl wait --for=condition=Established crd/disaggregatedsets.disaggregatedset.x-k8s.io --timeout=2m
kubectl -n lws-system rollout restart deployment/lws-controller-manager
kubectl -n lws-system rollout status deployment/lws-controller-manager --timeout=3m

cat <<EOF

${GREEN}${BOLD}✓ Planner lab is ready.${RESET}
  ${DIM}worktree:${RESET}   $LWS_DIR
  ${DIM}kubeconfig:${RESET} $KUBECONFIG_FILE
  ${DIM}context:${RESET}    $(kubectl config current-context)

  Observe:  ./k9s.sh
  Run:      ./run.sh <scenario>
  Teardown: ./install.sh --teardown
EOF
