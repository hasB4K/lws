# KEP-NNNN: DisaggregatedSet Scaling During Rolling Updates

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Current behavior](#current-behavior)
  - [What PR #907 changes](#what-pr-907-changes)
  - [Goals](#goals)
  - [Non-goals](#non-goals)
- [Proposal](#proposal)
  - [Scale up during a rollout](#scale-up-during-a-rollout)
  - [Scale down during a rollout](#scale-down-during-a-rollout)
  - [If the target changes again](#if-the-target-changes-again)
- [Design details](#design-details)
  - [Safety rules](#safety-rules)
  - [How one reconcile works](#how-one-reconcile-works)
  - [How replica fractions help](#how-replica-fractions-help)
  - [Multiple old revisions](#multiple-old-revisions)
  - [Completion and status](#completion-and-status)
  - [Required code changes](#required-code-changes)
  - [API and compatibility](#api-and-compatibility)
  - [Test plan](#test-plan)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
    - [End-to-end tests](#end-to-end-tests)
  - [Graduation criteria](#graduation-criteria)
- [Risks](#risks)
- [Alternatives](#alternatives)
- [Implementation history](#implementation-history)
<!-- /toc -->

## Summary

Today, changing a role's replica target during a DisaggregatedSet rolling
update may have little immediate effect. Scale-up waits whenever the new
revision is not fully Ready. Scale-down cannot shrink replicas already created
in the new revision.

This KEP makes scaling and rolling updates work together:

- **Scaling decides how many replicas the role needs.**
- **The rolling update decides whether those replicas belong to the old or new
  revision.**

When the target increases, additional replicas are created in the new
revision. When it decreases, old-revision replicas are removed first; the new
revision is reduced only when necessary. Every action remains limited by
`maxSurge`, `maxUnavailable`, and the amount of work already waiting to become
Ready.

This design builds on the Spec/Ready and replica-fraction model introduced by
[PR #907](https://github.com/kubernetes-sigs/lws/pull/907). It requires no API
change.

## Motivation

DisaggregatedSet pods can take minutes to become Ready because they may need to
load a model or wait for accelerators. A rolling update can therefore be in
progress for a long time. During that time, load can still change.

An operator or autoscaler should not have to choose between finishing the
rollout and responding to current demand.

### Current behavior

There are two separate reasons scaling is delayed today.

First, the rolling-update executor on `main` has a global stability check. If
any new-revision role has `Spec.Replicas != Status.ReadyReplicas`, it returns
without running the planner. A scaler update does trigger reconciliation, but
the new target cannot pass this check until all previously created replicas
are Ready.

Second, [KEP-849](/keps/849-DisaggregatedSet-HPA) intentionally prevents the
new revision from shrinking during a rollout:

```text
effective target = max(scaler target, new revision Spec)
```

This avoids changing direction in the middle of a rollout, but a lower target
cannot be reached until every old revision has drained.

### What PR #907 changes

PR #907 removes the global stability check. There is no replacement
`isRevisionStable` function. Instead, it answers three questions separately:

1. How much work has already been requested? Use `Spec`.
2. How much capacity is serving? Use `Ready`.
3. Is another scale-up or scale-down safe? Apply the relevant bound to that
   action.

This allows the controller to create more new replicas while earlier replicas
are still starting. Ready capacity still controls whether old replicas may be
removed.

PR #907 also gives differently sized roles a shared progress scale. This keeps
roles such as `8 prefill / 4 decode` moving through a revision together.

These are the foundations needed by this KEP. PR #907 does not itself support
shrinking the new revision.

### Goals

1. Apply a higher or lower replica target while a rolling update is active.
2. Put scale-up capacity directly in the new revision.
3. Remove old-revision capacity before removing new-revision capacity.
4. Preserve rollout availability, surge, pending-work, and role-coordination
   guarantees.
5. Always converge to the latest target if the target and pod readiness stop
   changing.
6. Remain stateless: controller restarts must not lose scaling progress.

### Non-goals

1. Changing how HPA or KEDA computes a target.
2. Adding another stabilization window. HPA/KEDA remain responsible for
   smoothing their output.
3. Supporting External scaling with `spec.slices > 1`; KEP-849 currently
   excludes that combination.
4. Treating replica changes as new revisions.

## Proposal

On every reconcile, the controller reads the latest target and observes how
many old and new replicas exist and are Ready. It then moves toward that target
without waiting for the current rollout step to become fully Ready.

The basic policy is:

```text
Need more replicas: create them in the new revision.
Need fewer replicas: remove old replicas first, then new replicas.
```

The target may come from `DisaggregatedSetRoleScaler.spec.replicas` for an
External role or from the role's inline replicas for a Static role. Both use
the same reconciliation logic.

### Scale up during a rollout

Suppose a role is rolling from A to B:

```text
old A Spec:  6
new B Spec:  2
new B Ready: 1
target:      8 -> 12
```

The desired fleet has grown by four replicas. Those replicas, as well as the
remaining replacements for A, belong to B. The controller may start creating
them immediately, even though one existing B replica is still starting.

It cannot create all remaining replicas without limits. The proposed new Spec
must fit within:

- the configured surge limit; and
- PR #907's pending allowance, which limits how far Spec may move ahead of
  Ready.

Old A replicas are removed only when enough Ready capacity exists. Raising the
target therefore increases capacity first; it does not sacrifice existing
Ready capacity to make the rollout appear further along.

### Scale down during a rollout

Suppose the state is:

```text
old Spec: 4
new Spec: 5
target:   8 -> 4
maxSurge: 1
```

At the new target, the rollout may temporarily contain at most five replicas.
The current total is nine, so four replicas are excess. Assuming Ready capacity
makes that safe, the controller removes those four replicas from the old
revision.

The resulting new revision still has five replicas. Once the old revision is
gone, the controller reduces the new revision from five to four. The final
state is:

```text
old Spec: 0
new Spec: 4
new Ready: 4
```

If old Spec is not large enough to absorb the required reduction, the
remaining excess is removed from the new revision. All reductions share the
same availability budget; scaling old and new down in one reconcile must not
spend the same Ready replica twice.

The controller may pause a downscale when insufficient Ready capacity exists.
This is expected: a lower target does not override `maxUnavailable`.

### If the target changes again

The controller does not remember or finish a sequence of past targets. For
example:

```text
8 -> 12 -> 5 -> 9
```

Each reconcile uses the newest target and the state currently observed in the
cluster. It never recreates an old revision or scales an old revision back up.

This makes the behavior restart-safe and avoids a queue of obsolete scaling
decisions. A rapidly changing target may still create churn; autoscaler
stabilization policies are responsible for controlling that.

## Design details

### Safety rules

For one role, define:

```text
total Spec      = old Spec + new Spec
committed Ready = sum(min(LWS status.readyReplicas, LWS spec.replicas))
```

Ready is capped at Spec separately for each LWS. This matters after a
scale-down: status can still include terminating replicas, but those replicas
must not authorize another removal.

The controller follows three rules.

**1. Surge limits growth**

```text
total Spec after growth <= target + maxSurge
```

If a lower target makes the current total larger than this limit, the
controller performs no more growth and safely reduces the excess.

**2. Ready capacity limits reduction**

Before removing `n` replicas:

```text
committed Ready - n >= max(0, target - maxUnavailable)
```

The controller assumes every removed replica might be Ready. This is
conservative, but it remains correct regardless of which LWS ordinal is
deleted.

PR #907 uses `min(initial, target) - maxUnavailable` for a rollout with a fixed
target. This KEP uses the latest target for scale decisions. In particular,
when the target increases, old Ready replicas must not drain until availability
catches up with the newly requested capacity.

**3. Pending work limits pipelining**

PR #907 bounds `new Spec - new Ready` using a proportional projection of
`maxSurge + maxUnavailable`. This KEP keeps that rule unchanged. A target
increase creates room to grow, but does not create unlimited pending pods.

These are bounds on controller actions. A target change can instantly make the
observed state fall outside a new bound; the controller must repair that state
without making it worse.

### How one reconcile works

One rolling-update reconcile performs the following steps:

1. Read old/new Spec and committed Ready for every role.
2. Read one consistent snapshot of the latest targets.
3. If total Spec is above `target + maxSurge`, calculate how much can safely be
   removed. Allocate that reduction to old revisions first.
4. Ask PR #907's fraction planner for the next revision-replacement step.
5. Bound the combined plan by the remaining surge, pending, and availability
   budgets.
6. Apply scale-downs before scale-ups so separate API calls cannot temporarily
   exceed `maxSurge`.
7. Requeue while either the rollout or target convergence remains incomplete,
   even when the controller is only waiting for readiness or termination.

The scaler may change while these calls run. That change triggers another
reconcile, which starts again from observed state.

### How replica fractions help

PR #907 expresses rollout progress as fractions rather than requiring every
role to change by one replica at the same time:

```text
smallestReplicaFraction = 1 / max(role sizes)
largestReplicaFraction  = 1 / min(positive role sizes)
```

For an `8 prefill / 4 decode` revision, the smallest fraction is `1/8`.
The shared curve begins like this:

| Progress | Prefill | Decode |
| --- | ---: | ---: |
| 0 | 0 | 0 |
| 1/8 | 1 | 1 |
| 2/8 | 2 | 1 |
| 3/8 | 3 | 2 |

Integer rounding can put the smaller role ahead temporarily. The
`largestReplicaFraction`, `1/4` in this example, bounds that skew.

When the target increases, the new-side curve is recomputed using the new role
sizes. Current new Spec is projected onto that curve, so the rollout continues
without a stored step number.

When the target decreases below current new Spec, the fraction planner alone
is insufficient: its new side was designed to grow. The capacity-reduction
step first removes the excess, after which fraction-based rollout coordination
continues normally. Scale-down is therefore not modeled as a rollout running
backward.

The old-side curve remains anchored to the replica counts captured when the
rollout began. A target change never resurrects an old replica.

### Multiple old revisions

An interrupted A-to-B-to-C rollout can leave both A and B as old revisions.
Existing newest-first behavior remains:

1. remove B before A;
2. consider a revision retired when all of its role Specs are zero; and
3. ignore stale Ready status from a retired revision.

Where possible, every role in an old revision is retired together. That
coordination preference never permits crossing a role's availability floor.

### Completion and status

A rollout is complete only when every role satisfies:

```text
old Spec == 0
new Spec == target
new Ready == target
```

The equality for new Spec is important. The existing `new Spec >= target`
check assumes that new Spec never shrinks; it would incorrectly call a
downscaled rollout complete while excess replicas still exist.

`DisaggregatedSetRoleScaler.status.replicas` continues to report observed
replicas across all revisions. It can temporarily be higher than Spec while
pods terminate. The DisaggregatedSet remains Progressing until it reaches the
latest target and old revisions are gone.

### Required code changes

The implementation after PR #907 needs to:

1. remove the no-shrink target clamp;
2. require exact new-Spec equality for completion;
3. bound new growth by `target + maxSurge`;
4. calculate one shared safe-reduction budget;
5. remove excess from old revisions first;
6. add an operation that can reduce new-revision Spec; and
7. retain PR #907's Spec/Ready, pending allowance, and fraction coordination.

There is no need to replace the fraction planner or add another global
stability function.

### API and compatibility

No new API field or resource is introduced. Replica counts remain outside the
revision hash, so changing a Static role's replicas also uses this behavior
without creating a new revision.

The behavior change is intentional: a downscale that was previously deferred
can now terminate old replicas during a rollout. Existing `maxSurge`,
`maxUnavailable`, and autoscaler policies bound that change.

Downgrading the controller is safe. An older controller simply returns to
deferring new-revision scale-down; there is no new persisted state to migrate.

### Test plan

[X] I/we understand the owners of the involved components may require updates
to existing tests to make this code solid enough prior to committing the
changes necessary to implement this enhancement.

#### Unit tests

Use a transition-level test harness that changes targets between reconciles
and checks the safety rules after every action. The important cases are:

- scale up while new Spec is ahead of new Ready;
- scale down to a target above, equal to, and below new Spec;
- old replicas are removed before new replicas;
- old replicas are insufficient, requiring new-revision scale-down;
- stale `Ready > Spec` cannot fund another reduction;
- one role scales up while another scales down;
- imbalanced role sizes preserve fraction coordination;
- A-to-B-to-C drains newest first; and
- repeated target changes converge once the target stabilizes.

Small replica counts should be exhaustively enumerated across target,
readiness, `maxSurge`, and `maxUnavailable` values.

#### Integration tests

- Change `DisaggregatedSetRoleScaler.spec.replicas` upward during an active
  rollout and verify the new target is consumed before all existing new pods
  are Ready.
- Change it below current new Spec and verify old-first reduction followed by
  exact convergence.
- Verify the same behavior for Static replica changes.
- Verify scaler status remains correct while old and terminating replicas
  coexist.

#### End-to-end tests

Use direct `/scale` writes and deterministic slow-starting pods:

1. Raise the target while `new Spec > new Ready`; prove another bounded batch
   is issued before the previous batch becomes fully Ready.
2. Lower the target below new Spec; prove total Spec decreases during the
   rollout, old replicas are selected first, and final new Spec/Ready equals
   the lower target.
3. Observe surge, availability, pending-work, and role-fraction bounds
   throughout both tests.

### Graduation criteria

This behavior graduates with DisaggregatedSet and
`DisaggregatedSetRoleScaler`.

Alpha requires moving-target unit coverage and deterministic scale-up and
scale-down end-to-end tests. Beta requires production feedback and sufficient
events or metrics to explain why convergence is waiting.

## Risks

**Autoscaler feedback:** scaler status includes both old and new replicas,
including rollout surge. Responding to every lower target could amplify
oscillation. The controller keeps the configured surge allowance, removes old
replicas first, and relies on the autoscaler's stabilization policy.

**Wasted startup work:** if old replicas cannot absorb a large reduction, a
new pod that just became Ready may be removed. This is preferable to ignoring
the requested target, but old-first reduction minimizes it.

**More executor states:** new Spec is no longer monotonic. Exact completion,
shared reduction accounting, and transition-level tests are required to keep
the additional states understandable.

## Alternatives

**Keep the current no-shrink guard.** This is simpler but can retain excess
capacity for the full duration of a slow rollout.

**Support scale-up only.** PR #907 makes this relatively small and it could be
delivered first, but it does not solve rollout-time scale-down.

**Restart the rollout when the target changes.** This would require persistent
target history and could continually reset progress under HPA. Recomputing
from observed state is simpler and restart-safe.

**Include replicas in the revision hash.** Every autoscaler write would create
a revision, LWS objects, and Services. Capacity is intentionally independent
from application revision.

## Implementation history

- 2026-09-02: Initial KEP draft.
