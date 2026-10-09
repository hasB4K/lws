# DisaggregatedSet fractional-lockstep rollout lab

This is an interactive harness for
[kubernetes-sigs/lws#907](https://github.com/kubernetes-sigs/lws/pull/907). It
runs against an isolated kind cluster, uses deliberately slow readiness and
termination, and prints every observed per-revision Spec and readiness
transition while you watch the same objects in k9s.

The harness builds the controller from the repository containing this directory
by default. It never changes the context in `~/.kube/config`; its kubeconfig is
`./.kube-planner3`.

## Setup

```bash
cd harness
./install.sh
./install.sh --recreate
./install.sh --teardown
```

Override the source worktree with `LWS_DIR=/path/to/lws`. The host build is
memory-bounded to two jobs by default; use `BUILD_JOBS=<n>` to change that.

## Run and observe

```bash
./run.sh                    # list scenarios
./run.sh 02                 # balanced A -> B
./run.sh imbalanced         # match by name
./run.sh 09 --keep          # leave A/B/C/D resources for inspection
./run.sh all --auto         # assertion run without interactive pauses
./run.sh 04 --start-k9s     # open k9s in a new Terminal before running
./run.sh 14 --max-surge 0 --start-k9s
./run.sh 15 --max-unavailable 0 --start-k9s
./k9s.sh                    # open k9s separately
```

Scenarios pause before meaningful changes and once more before cleanup. Use
`--start-k9s` to open k9s in a new Terminal window, or `--auto` to disable
pauses. In k9s, the initial `lws` view exposes Desired and Ready counts; use
`:pods` to inspect individual pods and `/planner-viz` to filter them.

The terminal watcher distinguishes raw LWS Ready from committed Ready. Raw
Ready is the LWS status value and may include replicas already terminating.
Committed Ready conservatively subtracts every replica above Spec because the
controller does not know which pending deletion will survive. The total also
shows planner-usable Ready, which excludes every role of a revision when one of
its required roles has no committed Ready replica. Revision aliases (`A`, `B`,
`C`, `D`) and their eight-character hashes are printed before each transition.

When rollout bounds are active, the watcher prints the availability floor and
prints an explicit `WARNING` whenever raw Ready or conservative planner-usable
Ready is below it. The warning says which value crossed the floor, separating
an actual readiness loss from uncertainty caused by an in-progress scale-down.

Scenarios 14 and 15 additionally show the live Spec P:D ratio of every
revision. The current revision is labelled `built`; old revisions are labelled
`remaining`. Each line shows Spec, committed-Ready, and unready percentages.
Their percentages use `50P/25D` as the full-revision baseline.
Both scenarios default to `maxSurge=5` and `maxUnavailable=5`. Either value can
be overridden, including to zero, with `--max-surge N` or
`--max-unavailable N`. They cannot both be zero because that leaves no legal
first rollout move.

Fractional lockstep means every role stays inside one moving progress window.
It does not mean equal absolute replica changes or atomic API updates. Brief
intermediate differences can therefore be visible in k9s.

## Scenario catalogue

| # | Sequence | Shape | What it demonstrates |
|---|---|---|---|
| 01 | Scale only | `2P/2D -> 6P/4D -> 3P/1D` | The steady-state scaling path. Replica-only edits retain revision A and do not invoke a template rollout. |
| 02 | A -> B | `4P/4D` | Balanced roles. Both roles stay inside the same fractional window; `maxSurge=1`, `maxUnavailable=1`. |
| 03 | A -> B | `8P/4D` | A 2:1 imbalance. Absolute changes differ while normalized progress remains within `largestReplicaFraction=1/4`. |
| 04 | A -> B | `6P/3D`, decode slow | Prefill becomes Ready quickly and decode slowly. Prefill may lead within the skew bound, then waits; pending readiness does not trigger uncoordinated retirement. |
| 05 | A -> B + scale up | `3P/2D -> 7P/4D` | Template rollout and growth together. The hard ceiling is based on `max(initial,target)+maxSurge`. |
| 06 | A -> B + scale down | `8P/4D -> 3P/2D` | Template rollout and shrink together. Availability floors use `min(initial,target)-maxUnavailable`. |
| 07 | A -> B | `4P/2D`, zero surge | Drain-before-grow ordering. Total Spec never exceeds `4P/2D`; each old slot is released before its replacement is requested. |
| 08 | A -> B -> C | `6P/3D` | Interrupt B before it is Ready. B drains before A, and per-revision intended counts keep C's original availability floor stable after B is deleted. |
| 09 | A -> B -> C -> D | `6P/3D` | Three rapid interruptions produce several old revisions. They retire in C, B, A order while the intended baseline stays stable. |
| 10 | A -> B -> A | `6P/3D` | Rollback reuses revision A instead of creating a third hash; B becomes the newest old revision and drains first. |
| 11 | A -> B | `8P/4D`, unequal budgets | Prefill has `3/2` surge/unavailable while decode has `0/1`. Raw limits are per role while fractional coordination still bounds normalized skew. |
| 12 | A -> B + shrink | `2P/3D -> 1P/3D`, zero surge, unequal unavailability | Constraint-based progress from a tight state. The furthest feasible target keeps both old roles present, then retires the old revision together once the target has usable capacity. A brief observation between sequential API patches is tolerated; an incomplete revision may not persist. |
| 13 | A -> B | `1P/5D`, zero surge | The ordinary constraints reach the singleton-role wedge `A=1P/4D, B=0P/1D`. One bootstrap Prefill may temporarily exceed its surge ceiling by one. No second emergency replica is allowed, and A must still retire as a complete revision. |
| 14 | A -> B | `50P/25D` | A large ordinary rollout with configurable budgets (default `5/5`). Slow readiness and live per-revision P:D progress make successive fractional steps visible. |
| 15 | A -> B -> C | `50P/25D` | A large interrupted rollout with configurable budgets (default `5/5`). C is requested automatically once both B roles have reached at least one third of their target Specs (`17P/9D`). |

Every rollout scenario checks the applicable per-role Spec surge ceiling.
Scenario 13 permits and requires the documented one-replica bootstrap exception
for Prefill; its Decode ceiling remains hard.
Scenarios 02–04, 11, and 14 also check the new revision's fractional skew.
Scenarios 08 and 09 assert newest-first retirement rather than merely printing it.
Scenario 08 keeps the original `5P/2D` decision-time availability floor for the
whole A -> B -> C rollout, including after partial revision B is deleted.

The watcher does not treat every sampled Ready value as a controller safety
decision. Readiness can independently fall after a decision, and sequential
Kubernetes updates can be observed partway through. Decision-time
availability-floor enforcement is covered by the deterministic planner tests.
The harness instead asserts observable Spec ceilings, fractional coordination,
revision retirement properties, and full readiness at convergence.

## Cleanup

Each scenario deletes its DisaggregatedSet on exit unless `--keep` is used.
Delete the entire isolated cluster with:

```bash
./install.sh --teardown
```
