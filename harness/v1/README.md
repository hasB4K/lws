# LWS v1 rollout lab

A small, colorful regression lab for the final v1 rollout changes. It builds one pinned controller, creates its own kind cluster, runs real Pods through the scenarios below, and deletes that cluster when finished or interrupted. No existing cluster or default kubeconfig is used.

The default matrix contains **26 runs across eleven scenario families**. Every family runs with Ordinal and Hash group identity. Scale-down also covers both policies; the small-outage case covers integer and percentage budgets. An optional larger rename adds two runs, for 28 in total.

See [validation results](VALIDATION.md) for the executed matrix, harness fixes, and the Ghostty verification limitation.

## Run it

Requirements: Docker, kind, kubectl, Git, Go matching this repository's `go.mod`, Python 3, jq, Bash, and standard Unix tools. No Python packages are needed. First setup needs network access for Go modules and container images. Docker needs enough memory for a Kubernetes control plane and the demo Pods; approximately 8 GiB is a useful starting point.

```bash
git clone --branch codex/v1-regression-harness https://github.com/hasB4K/lws.git lws-v1-lab
cd lws-v1-lab

./harness/v1/run.sh --list                  # exact selected matrix; no Docker calls
./harness/v1/run.sh all --auto              # all 26 runs, unattended
./harness/v1/run.sh autoscaler-handoff --auto # Static → External, both identities
./harness/v1/run.sh outage --interactive    # pause at events; open k9s in Ghostty
./harness/v1/run.sh rename-outage --interactive # rename with MS=0 / MU=100%, both identities
./harness/v1/run.sh scale-up --identity hash --interactive
./harness/v1/run.sh scale-down --policy Immediate --auto
./harness/v1/run.sh outage --variant integer --identity ordinal --auto
./harness/v1/run.sh ordinary rename --auto  # select several families
./harness/v1/run.sh all --include-large --auto
```

An installed `kind` on PATH is found automatically; no export is needed. Alternatively, set `KIND=/absolute/path/to/kind` to select another binary. `KIND_NODE_IMAGE` optionally overrides the default `kindest/node:v1.36.1`. Clone with history: `baseline.json` points to a controller commit that must be available locally.

Automatic mode is the default. Use `--fail-fast` to stop after the first failed run; otherwise the report records failures and continues with the remaining cases. A failing or incomplete matrix returns a nonzero exit status.

With cached builds/images on the tested laptop, budget about **21 minutes** for `all --auto`, or **23 minutes** with `--include-large`. These are estimates from the earlier full run plus the added handoff and rename-outage timings, not measurements of a fresh expanded matrix. The handoff pair alone took about two minutes including setup. First-time downloads/builds take longer.

### Watch and trigger events

`--interactive` opens read-only k9s in **one new Ghostty window**, never a tab. The colorful scenario dashboard stays in the terminal where you started the script. In k9s, `:pods` shows individual Pods and `:lws` shows physical LeaderWorkerSets.

At a checkpoint:

- **Enter** performs the described event, such as changing replicas or releasing application readiness.
- **a** performs that event and runs the rest of the selected run automatically.
- **q** stops the run and cleans up its cluster.

Waiting for Kubernetes, readiness, or the real scheduler timeout is automatic. The harness does not ask you to simulate those observations. `--interactive --no-k9s` keeps the checkpoints without opening a window.

If Ghostty's scripting endpoint is unavailable, launch fails after a bounded wait and the owned cluster is cleaned up. Retry with `--no-k9s`, or use your own new-window launcher below. Viewer launch errors are saved in `ghostty.log`.

You can customize the viewer without changing the scripts:

```bash
K9S_COMMAND=/path/to/my-k9s-wrapper ./harness/v1/run.sh outage --interactive
TERMINAL_COMMAND=/path/to/new-window-wrapper ./harness/v1/run.sh outage --interactive
```

These variables name **executable files**, not shell command strings. A k9s wrapper should end with `exec k9s "$@"`; it receives the private kubeconfig, context, namespace, and read-only flags. A terminal wrapper receives the full viewer command as separate arguments and must open a new window running those arguments unchanged. For example, a Linux wrapper can use `exec xterm -e "$@"`. Only the default macOS Ghostty integration automatically closes its window; custom wrappers control their own windows. Never remove the isolation flags from a wrapper.

## Scenarios

P means Prefill; D means Decode. **MS** is `maxSurge`, **MU** is `maxUnavailable`. Counts and budgets refer to replica groups, not individual Pods. The scheduler's synthetic slots are equal-size resources; each demo Pod requests one slot.

| Case | What happens and what is checked | Runs |
|---|---|---:|
| `ordinary` | Start at 2P/4D with MS1/MU0, hold the new version's readiness, then release it. Check ordinary asymmetric progress, capacity protection, convergence, and replica-only scaling without a new revision. | 2 |
| `scale-up` | During unequal progress toward 4P/4D, raise desired to 6P/6D with MS1/MU0. Missing capacity must not justify deleting existing Ready capacity or cancelling needed loading groups. Finish at the exact new desired counts. | 2 |
| `scale-down` | During the same mixed rollout, reduce desired from 4 to 2 per role. Compare Deferred and Immediate: prefer safe old drains and retain scheduled new groups needed for the final count. | 4 |
| `autoscaler-handoff` | Start with 3P/2D, two Pods per group, MS1/MU0. Switch Prefill from Static to External without changing templates. Its scaler must inherit three groups; hold off the first scaler write for 15 seconds and verify all ten original Pods stay Ready. Then write Prefill 3→4→2 through `/scale`; Decode stays untouched. | 2 |
| `rename` | Rename both roles at 2P/2D, MS0/MU1, with tight capacity. Released slots must allow the first new P and D to start; the faster role must not monopolize replacement growth. | 2 |
| `rename-outage` | Rename both roles at 2P/3D with MS0/MU100% on both old and new roles, in five slots. All old groups must disappear and all five replacements must schedule while readiness is closed. Hold the permitted full outage, release Prefill first, then Decode. Check zero-surge limits and no emergency allowance. | 2 |
| `restart-rollback` | Interrupt A→B with C, restart the controller, then roll back to B. Check recovery with a fresh informer cache and reuse of the existing revision. MS1/MU1. | 2 |
| `subroles` | Two-Pod groups share one physical LWS. Shrink/relabel, restart the controller, grow, and roll out while checking group-wide labels and independent child targets. MS1/MU0 per child. | 2 |
| `native-restart` | Crash a worker, then a leader, in a two-Pod group. Only the affected group should be recreated; unrelated group UIDs must survive. | 2 |
| `outage` | Fit 1P/2D into exactly three slots with MS0. Prefill MU is either `1` or `100%`; Decode MU is `1`. Check that explicitly permitted Prefill downtime unblocks the rollout without losing Decode's independent protection. | 4 |
| `emergency` | Attach a clearly identified interrupted history at desired 1P/4D, MS1/MU0. Observe one bootstrap replica, then genuine scheduler rejection for at least 60 seconds. Check bounded unavailable fallback, preservation of A, no repeated draining while C loads, and eventual recovery. | 2 |
| `large-rename` — optional | Rename 9P/4D to 8P/3D in thirteen slots, old MU1/new MU4. Exercise asymmetric integer rounding and first-complete-pair growth with a larger workload. | +2 |

Cases use Deferred except `scale-down`, which tests both policies. `--policy` narrows only that scale-down case; it does not change the other scenarios. `--identity ordinal`, `--identity hash`, and `--variant` narrow the matrix. Use `--list` with the same arguments to see exactly what will run.

## Read the dashboard

Magenta headings mark sampled progress, cyan marks the next event, green marks passed checks, and yellow marks warnings or emergency exceptions. The dashboard separates these concepts:

| Field | Meaning |
|---|---|
| Spec | Groups requested from the physical LWS. A request is not proof that a Pod exists or is Ready. |
| Whole-group Ready | Every expected Pod in the group is Ready. A Ready leader alone is insufficient. |
| Retained Ready | Conservative Ready credit from the shared observer, after acknowledgements and already-planned removals. This can be below physically observed Ready while controllers catch up. |
| Serving / individual Ready | Serving credit requires the revision's needed roles together. Individual credit protects each healthy role separately; those are different checks. |
| Availability floor | Ready capacity the planner must protect. Existing capacity below a newly increased floor is not permission to make the deficit worse. |
| Surge ceiling | Normal upper bound on requested groups. It is a ceiling, not a minimum or a promise that terminating Pods have already disappeared. |
| Growth limits / minimum kept | Other active constraints: pending work, fractional replacement, and old/new groups that cannot safely be removed yet. |
| Fractional windows | Relative rollout progress across unequal roles; 1 of 2 Prefill and 2 of 4 Decode represent the same fraction. |
| Emergency events | Actual `BootstrapSurge` or `AvailabilityFallback` events. These are displayed separately from normal budgets, never silently treated as ordinary capacity. |

Detailed bounds and previews come from the **actual planner implementation**, exposed by a read-only diagnostic bridge built only with the `harness` build tag. The controller image does not contain that bridge. Multiple old revisions can have different candidate bounds, so the display prints each candidate instead of guessing one universal ceiling.

For current roles, the availability floor is normally `max(0, desired − MU)`. Removed roles also account for unreplaced historical capacity. Immediate uses `desired + MS` as the surge ceiling; Deferred uses `max(desired, selected old initial count) + MS`. Completeness, pending work, and fractional coordination still constrain an action even if these two numbers appear to permit it.

If a required role's budget permits zero Ready groups, the old revision may temporarily stop serving. Its other roles still keep their own individual availability budgets. The dashboard explicitly labels this permission: zero serving capacity is not automatically a failure in that scenario.

The observer and API samples are **not atomic snapshots or the controller's own decision records**. A sampled drop can reflect a Pod failure or an observation in the middle of several writes. Assertions use explicit stable checkpoints and scenario-specific no-regression checks; this lab does not claim that one sampled counter proves every decision safe.

## Provenance and limits

`baseline.json` is the source of truth for the pinned local integration:

- Upstream main: `b43d3b4ec81ff243435ac2f98c6c0138712adb05`.
- [#1181](https://github.com/kubernetes-sigs/lws/pull/1181): `a0d47fe88c3355e018ddb29c289679a8b4f2c84f`.
- [#1172](https://github.com/kubernetes-sigs/lws/pull/1172): `924c5427e10cb1eb808b22562a0d98b6d3afe02f`.
- Merged controller: `cf833e2421f105e16d4366a2cc17057a1b5bec17`.

The controller is built from a Git archive of that commit, not uncommitted controller files. Its deployment uses the original CRDs/webhooks and feature defaults; the local overlay selects the pinned image, one manager replica, and smaller manager resource requests. The dashboard/helper is built from the harness checkout.

This consolidates the theme of the [#907 harness](https://github.com/hasB4K/lws/pull/3), released-capacity/rename/shared-readiness regressions from #1105, scaling-during-rollout cases, and controller/restart experiments. It deliberately does not carry over every historical scenario or outdated budget assumption.

Important limits:

- The emergency case seeds ownership/revision metadata while the local controller is paused, then resumes real reconciliation. Its Pods, readiness, scale acknowledgements, and scheduler timeout are real; no Pod status or Unschedulable age is forged. Only three admission hooks are temporarily scoped out of the demo namespace and then restored.
- The restart/rollback case tests an actual controller restart and cold-cache recovery. It does not reproduce the old crash harness's artificially delayed API-write fault injection.
- `native-restart` covers merged `RecreateGroupOnPodRestart`, not the unmerged in-place restart prototype.
- Hash subrole deletion preferences are not exact deletion authority. The case checks coherent eventual assignment and recovery, not guaranteed preservation of a particular Hash child group.
- `autoscaler-handoff` tests the handoff and real `/scale` reads/writes, not a running metrics-driven HPA or KEDA. Pod UID checks detect a drain/recreation even between samples; polling does not capture every short-lived API write or Ready condition change. The case covers ordinary roles at steady state, not switching during a rollout or switching a subrole.
- Slots are not GPUs. These tests do not prove schedulability after arbitrary resource-size changes, nor cover real GPU plugins, network partitions, multiple nodes, or every possible interleaving.

## Cleanup and artifacts

Every invocation creates a unique `lws-v1-run-*` cluster and private kubeconfig under `harness/v1/.state/`. Cleanup verifies the Docker node ID, kind cluster label, mounted ownership-token path, and token before deleting that cluster, accounting for Docker Desktop's exact macOS `/host_mnt` translation. It runs after success, failure, Ctrl-C, or normal termination. An ownership mismatch causes refusal, not deletion. Killing the process with `SIGKILL`, losing the Docker daemon, or powering off cannot guarantee cleanup; retained `runtime.json` identifies the exact run for inspection.

On interruption the suite gets up to 30 seconds to close its observer and restore temporary settings, then the runtime stops only its isolated process group before deleting the owned cluster. Existing Ghostty windows, other clusters, and the default kubeconfig are untouched.

The printed artifact directory contains `results.json` (per-case outcome/checks), `runtime-result.json` (exit/cleanup outcome), the colored transcript, controller logs, events, final resources, and per-case samples plus `observer.jsonl`. `.state/last-run` records the latest path. Artifacts and cached Docker images remain for diagnosis; demo clusters do not.

Fast runtime checks, without a cluster:

```bash
python3 -m unittest discover -s harness/v1 -p 'test_*.py' -v
bash -n harness/v1/run.sh harness/v1/view.sh
```
