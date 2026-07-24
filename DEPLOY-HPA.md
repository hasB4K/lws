# Deploying the KEP-849 HPA controller build

This branch (`feature/hpa-impl-deployable`) publishes the DisaggregatedSet
controller with the per-role HPA support (KEP-849) to your own image
registry so you can install it on any cluster without the shared
`k8s-staging` registry.

## How the image gets built

`.github/workflows/publish-hpa-image.yaml` triggers on push to any branch
matching `feature/hpa-**` (also manual via `workflow_dispatch`) and pushes
a multi-arch (`linux/amd64,linux/arm64`) image to:

    ghcr.io/<owner>/lws:<version>-dev_<sha7>

where `<version>` is read from `RELEASE_VERSION` in the `Makefile` with
the leading `v` stripped, and `<sha7>` is the short commit SHA. Every
commit produces a distinct, immutable tag — no rolling tag.

For this branch on hasB4K/lws the current commit becomes:

    ghcr.io/hasb4k/lws:0.9.0-dev_<sha7>

## One-time setup on the fork

1. Push this branch to `hasB4K/lws` (that's what fires the workflow).
2. First successful build creates the package. Newly-created GHCR packages
   default to **private**. To pull it without a pull secret, mark it public:
   go to <https://github.com/hasB4K?tab=packages>, click `lws`, open Package
   settings, "Change package visibility" → Public. This is a one-time step
   per package.

## Deploying on a real cluster

Once the image exists in GHCR:

```shell
# From this branch checkout:
IMG=ghcr.io/hasb4k/lws:0.9.0-dev_<sha7>
make deploy IMG="$IMG"
```

`make deploy` uses `config/default` kustomize output (CRDs + RBAC +
webhook + controller Deployment) with the image ref substituted. It
requires `kubectl` pointing at the target cluster.

If you prefer a self-contained manifest bundle (no kustomize on the
target side):

```shell
make manifests generate
bin/kustomize build config/default \
  | sed 's|us-central1-docker\.pkg\.dev/k8s-staging-images/lws/lws:.*|'"$IMG"'|' \
  > /tmp/lws-hpa.yaml
kubectl apply --server-side --force-conflicts -f /tmp/lws-hpa.yaml
```

## Pulling to a private cluster

If you keep the package private, create an image pull secret in the
target namespace before deploying:

```shell
kubectl -n lws-system create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=<your-gh-user> \
  --docker-password=<gh-classic-token-with-read:packages>
kubectl -n lws-system patch serviceaccount lws-controller-manager \
  -p '{"imagePullSecrets":[{"name":"ghcr-pull"}]}'
```

## Sanity check

```shell
kubectl -n lws-system rollout status deploy/lws-controller-manager
kubectl -n lws-system get pods -l control-plane=controller-manager \
  -o jsonpath='{.items[0].spec.containers[0].image}{"\n"}'
```
