# KEP-NNNN: DisaggregatedSet Scaling During Rolling Updates

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Current Behavior](#current-behavior)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Scale Up During a Slow Rollout](#scale-up-during-a-slow-rollout)
    - [Scale Down During a Rollout](#scale-down-during-a-rollout)
  - [Behavioral Contract](#behavioral-contract)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Dependency on the Pipelined Rollout Model](#dependency-on-the-pipelined-rollout-model)
  - [State Model](#state-model)
  - [Safety Invariants](#safety-invariants)
  - [Reconciliation](#reconciliation)
  - [Required Controller Changes](#required-controller-changes)
  - [Scale Up](#scale-up)
  - [Scale Down](#scale-down)
  - [Replica Fractions and Moving Targets](#replica-fractions-and-moving-targets)
  - [Multiple Old Revisions](#multiple-old-revisions)
  - [Changing and Oscillating Targets](#changing-and-oscillating-targets)
  - [Completion and Status](#completion-and-status)
  - [API and Compatibility](#api-and-compatibility)
  - [Observability](#observability)
  - [Test Plan](#test-plan)
    - [Prerequisite Testing](#prerequisite-testing)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
    - [End-to-End Tests](#end-to-end-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Keep the Current No-Shrink Guard](#keep-the-current-no-shrink-guard)
  - [Support Only Scale Up During a Rollout](#support-only-scale-up-during-a-rollout)
  - [Restart the Rollout When the Target Changes](#restart-the-rollout-when-the-target-changes)
  - [Encode Replica Counts in the Revision](#encode-replica-counts-in-the-revision)
<!-- /toc -->

## Summary

This KEP allows a DisaggregatedSet role's replica target to change while a
rolling update is in progress. The controller responds to both increases and
decreases without waiting for the rollout to finish, while preserving the
rollout's surge, availability, pending-work, cross-role coordination, and
newest-first drain guarantees.

The proposal builds on the pipelined rollout model introduced by
[PR #907](https://github.com/kubernetes-sigs/lws/pull/907). That model removes
the global `isRevisionStable` gate, distinguishes work issued through `Spec`
from capacity available through `Ready`, and expresses multi-role progress as
replica fractions. This KEP uses those primitives to reconcile against the
latest replica target on every pass.

No API change is required. A target may come from
`DisaggregatedSetRoleScaler.spec.replicas` for an External role or from the
role's inline replicas for a Static role. The existing scale subresource and
status fields remain unchanged.

## Motivation

[KEP-849](/keps/849-DisaggregatedSet-HPA) provides a stable, per-role scale
target across revision changes. It deliberately treats the scaler value as a
post-rollout target: a target increase is consumed only when the rollout
planner next runs, and a target below the new revision's current Spec is
clamped until old revisions have drained.

That behavior is safe, but it delays a capacity response at exactly the time
an operator may need it most. A rollout can take minutes when pods load large
models, wait for accelerators, or perform startup checks. During that interval:

- an HPA scale-up should be able to add capacity without waiting for every
  already-issued pod to become Ready; and
- an HPA scale-down should be able to remove excess capacity, preferentially
  from obsolete revisions, without waiting for the entire replacement to
  finish.

### Current Behavior

On the controller behavior preceding PR #907, every rolling-update pass first
checks whether all new-revision roles have `Spec.Replicas ==
Status.ReadyReplicas`. If any role has pending work, reconciliation returns
before resolving and applying a changed target. A scaler update enqueues the
DisaggregatedSet correctly, but that event cannot make progress through this
global readiness gate.

KEP-849 adds a second, independent restriction for scale-down. During a
rollout, an External role's target is floored at the new revision's current
Spec. This no-shrink guard prevents a scale-down/scale-up flip, but means the
new revision can never contract until the old revisions reach zero and the
controller returns to its steady-state path.

PR #907 fixes the first architectural problem by replacing global stability
with action-specific bounds. It does not remove the no-shrink guard and does
not provide a new-revision scale-down operation. This KEP specifies that
remaining behavior.

### Goals

1. Process the latest resolved replica target on every rolling-update
   reconcile, even while some issued replicas are not Ready.
2. Allow a target increase to grow the new revision within the existing surge
   and pending-work bounds.
3. Allow a target decrease to reduce the aggregate fleet during the rollout,
   removing obsolete old-revision replicas before new-revision replicas.
4. Converge to exactly the latest target: old Spec is zero, new Spec equals the
   target, and the target replicas are Ready.
5. Preserve per-role `maxSurge`, `maxUnavailable`, committed-availability,
   cross-role fraction, whole-revision, and newest-first guarantees.
6. Remain stateless across reconciles. A controller restart must not require a
   remembered previous autoscaler target or rollout step.
7. Cover upward, downward, and repeatedly changing targets with transition-
   level invariant tests and deterministic end-to-end tests.

### Non-Goals

1. Changing how HPA or KEDA calculates a desired replica count.
2. Adding controller-specific scale stabilization. HPA/KEDA behavior and
   stabilization windows remain the source of target smoothing.
3. Supporting External scaling with `spec.slices > 1`. KEP-849 currently
   excludes that combination; this KEP does not resolve the aggregate versus
   per-slice target question.
4. Changing pod termination order within a LeaderWorkerSet. Scaling a LWS uses
   its existing replica semantics.
5. Guaranteeing immediate convergence when doing so would violate
   availability, surge, pending-work, or coordinated-revision constraints.
6. Treating a replica-target change as a new revision. Replica counts remain
   outside the revision hash.

## Proposal

The controller continuously reconciles the observed rollout state toward the
latest target vector. It does not classify a request using a stored previous
target. Instead, each role's current target is compared with the current old
and new Spec footprints:

- `newSpec < target` represents remaining new-revision growth;
- `oldSpec + newSpec > target + maxSurge` represents aggregate excess outside
  the target's current rollout envelope; and
- `newSpec > target` represents new-revision excess that must eventually be
  removed.

Revision replacement and capacity correction remain separate concerns. The
replica-fraction planner coordinates how roles advance from old revisions to
the new revision. A capacity-correction phase reacts to a decreased target by
draining old revisions first and shrinking the new revision only when old
replicas cannot absorb the excess or have already reached zero.

All actions use one observed snapshot and one target snapshot per reconcile.
A newer scaler write triggers another reconcile and supersedes the prior
target naturally.

### User Stories

#### Scale Up During a Slow Rollout

An External prefill role is rolling from revision A to revision B. Revision B
has issued four replicas, but only two are Ready because model loading is
slow. HPA raises the target from eight to twelve.

The controller does not wait for all four issued replicas to become Ready. It
rebases the new-side fraction curve on twelve and may issue more revision-B
replicas immediately, provided the resulting Spec stays within both the surge
ceiling and the pending allowance. Old revision-A replicas drain only when
committed Ready capacity makes that safe.

#### Scale Down During a Rollout

A role has four old replicas and five new replicas while rolling toward eight.
HPA lowers the target to four with `maxSurge: 1`. The current rollout envelope
is therefore five replicas.

The controller observes four excess Spec replicas, uses the shared safe-drain
budget, and removes them from the old revision first. If the new revision is
still above four after the old revision reaches zero, it scales the new
revision to four. It never counts terminating replicas twice and never reduces
committed availability below the configured floor.

### Behavioral Contract

For each role, target changes have the following observable behavior:

| State | Behavior during the rollout |
| --- | --- |
| Target increases above new Spec | Grow the new revision using pipelined, bounded issuance. |
| Target decreases but remains at or above new Spec | Stop unnecessary new growth and accelerate safe old-revision drain toward the smaller envelope. |
| Target decreases below new Spec | Drain old revisions first; shrink the new revision when old drain alone cannot satisfy the envelope or old Spec is zero. |
| Target changes while pods are pending | Recompute from current Spec and committed Ready; do not wait for global stability. |
| Target changes repeatedly | The newest observed target wins; no stored transition must complete first. |

`maxSurge` remains permission for temporary rollout capacity, not part of the
steady-state target. Consequently, a downscale first converges inside
`target + maxSurge`; the remaining surge disappears as old revisions finish
draining. Final completion requires new Spec to equal the target exactly.

### Risks and Mitigations

**Risk: Autoscaler feedback oscillates because scaler status includes rollout
surge.** `DisaggregatedSetRoleScaler.status.replicas` aggregates old and new
revisions, so an autoscaler observes temporary rollout replicas.

**Mitigation:** The controller acts only on the explicit target written to
`scaler.spec.replicas`, retains up to `maxSurge` while replacement is active,
and removes old replicas before new ones. HPA's existing scale-down
stabilization and rate policies remain applicable. The controller preserves
safety under an oscillating target but does not duplicate autoscaler
stabilization policy.

**Risk: A stale Ready count authorizes the same scale-down twice.** LWS status
can remain above Spec after a previous scale-down while pods terminate.

**Mitigation:** Every availability calculation uses committed Ready:
`min(status.readyReplicas, spec.replicas)` per LWS. All old and new reductions
in one pass consume a single per-role safe-reduction budget.

**Risk: Shrinking the new revision wastes successfully updated pods.** A later
scale-up may have to create them again.

**Mitigation:** Old revisions are always the first source of capacity
reduction. The new revision shrinks only when removing all eligible old Spec
cannot restore the target envelope or when old Spec has reached zero.

**Risk: Independent role targets break a serving revision by removing one
role before the others.** A per-role autoscaler may request different changes
at different times.

**Mitigation:** Existing fraction coordination and whole-revision retirement
rules remain in force. A revision is retired as a unit only when every affected
role can spend the required safe-drain budget; otherwise the planner keeps the
revision alive while safe replacement or capacity correction continues.

**Risk: A target change makes the observed state immediately exceed a new
surge or pending bound.** The controller cannot undo already-issued API work
atomically.

**Mitigation:** Bounds govern controller-authored transitions. When a target
decrease makes the observed state out of bounds, the controller performs no
additional growth and monotonically repairs the excess as availability
permits. Such an inherited state is not treated as a controller safety
violation.

## Design Details

### Dependency on the Pipelined Rollout Model

This proposal assumes the state and safety model from PR #907. If the two
features are implemented in a different order, equivalent prerequisites must
land first:

1. Rollout progress is based on Spec, representing issued work.
2. Availability is based on committed Ready, representing serving work.
3. There is no global `isRevisionStable` progress gate.
4. New Spec is bounded independently by surge and pending allowances.
5. Old drain is bounded independently by committed availability.
6. Completion is separate from permission to make progress.

There is intentionally no replacement `isRevisionStableV2` Boolean. A single
predicate cannot express whether scale-up, old drain, new shrink, and rollout
completion are independently safe. This KEP extends the same action-specific
model to target decreases.

### State Model

For each role `i`, reconciliation constructs the following snapshot:

```text
initialOld[i] = old-revision Spec captured when the rollout started
oldSpec[i]    = aggregate Spec across all old revisions
oldReady[i]   = aggregate committed Ready across all old revisions
newSpec[i]    = target-revision Spec
newReady[i]   = target-revision committed Ready
target[i]     = latest resolved target
surge[i]      = resolved maxSurge
unavailable[i]= resolved maxUnavailable
```

Committed Ready is calculated per LWS before aggregation:

```text
committedReady(lws) = min(lws.status.readyReplicas,
                          lws.spec.replicas)
```

The per-LWS cap matters. Capping only the aggregate can let stale Ready from a
retired LWS fund a reduction in another LWS.

The target is resolved once for the reconcile:

- External role: `DisaggregatedSetRoleScaler.spec.replicas`;
- Static role: `spec.roles[].spec.replicas`, defaulting as today.

The controller stores no previous target and adds no target annotation. The
observed Spec, Ready, rollout snapshot, and latest target are sufficient to
compute the next idempotent action after a restart.

### Safety Invariants

PR #907 provides the fraction projection and pending-work model. This KEP
distinguishes the fraction's role size from the hard bounds associated with
the latest desired capacity:

```text
roleSize          = max(initialOld, target)  // fraction projection
targetCeiling     = target + maxSurge        // hard Spec bound
availabilityFloor = max(0, target - maxUnavailable)
pending           = newSpec - newReady
```

PR #907 uses `min(initialOld, target)` in the availability floor for a fixed
source-to-target rollout. A live target increase needs the stronger dynamic
floor above: otherwise the controller could drain old Ready capacity while the
fleet is still below the newly requested capacity. Immediately after an
increase, observed availability may already be below the new floor; that is
not a controller-authored violation. The controller freezes further drain and
uses the additional target headroom for growth until availability catches up.

The pending allowance is the proportionally projected
`maxSurge + maxUnavailable` budget from PR #907. Controller-authored growth
must satisfy both:

```text
oldSpec + proposedNewSpec <= target + maxSurge
proposedNewSpec - newReady <= pendingAllowance
```

Using `target + maxSurge` for new growth is important after a target decrease.
The broader `max(initialOld, target) + maxSurge` value remains useful for
fraction projection and for describing a state inherited from before the
decrease, but it must not permit new growth above the latest target's rollout
envelope.

Any controller-authored Spec reduction must satisfy:

```text
postReductionCommittedReady >= availabilityFloor
```

Because the controller cannot know whether LWS will remove a Ready or unready
replica, it conservatively assumes every Spec reduction removes one committed
Ready replica. The maximum reduction that all old and new scale-down actions
may share in one reconcile is therefore:

```text
safeReduction = max(0, oldReady + newReady - availabilityFloor)
```

When a target change makes the observed state violate a new ceiling, the
controller must not worsen the violation. It either reduces the excess or
waits for enough committed availability to do so safely.

### Reconciliation

Each rolling-update reconcile follows these logical phases. An implementation
may combine calculations, but it must preserve their budgets and ordering.

1. **Observe:** list all old and new LWS objects and build the Spec/Ready state.
2. **Resolve:** snapshot the latest per-role targets and percentage-derived
   surge/unavailable values.
3. **Correct capacity excess:** calculate Spec above `target + maxSurge` and
   allocate the safe reduction old-first, newest revision first.
4. **Plan revision progress:** use the replica-fraction planner against the
   current target vector to propose new growth and old drain.
5. **Apply hard bounds:** clamp all proposals to the remaining surge, pending,
   availability, and coordination budgets after capacity correction.
6. **Execute reductions before growth:** scale down old revisions, then any
   unavoidable new-revision excess, then scale up the new revision. This
   ordering cannot create a transient surge breach between API calls.
7. **Requeue:** if the rollout or target convergence is incomplete, request a
   bounded requeue even when no API object changed; readiness and termination
   status may be the only expected progress.

The target snapshot is immutable within one pass. If it changes during API
updates, the scaler watch enqueues another reconcile, which computes a new
safe plan from observed state.

### Required Controller Changes

The implementation is expected to evolve the PR #907 executor in the
following focused ways:

1. Remove the External-role target clamp in `buildRolloutState`; retain the
   scaler target even when it is below new Spec.
2. Change structural completion from `newSpec >= target` to
   `newSpec == target`.
3. Keep `boundNewReplicaTargets` as a growth bound, but use
   `target + maxSurge` as its hard Spec envelope.
4. Add one capacity-correction calculation that shares `safeReduction` across
   old and new Spec reductions.
5. Add a new-revision scale-down operation, or generalize the current
   scale-up-only helper into exact new-Spec reconciliation.
6. Preserve `maxSafeDrain`, `committedReadyReplicas`, pending allowance, and
   fraction coordination as the safety inputs rather than adding another
   global readiness predicate.
7. Make the planned action explicitly distinguish rollout drain from capacity
   correction so execution cannot accidentally spend either budget twice.

This does not require replacing the whole fraction planner. Upward movement
continues through its existing new-side calculation; target-envelope repair is
a bounded executor concern around that planner.

### Scale Up

When `newSpec < target`, the current target vector defines the new-side
fraction curve. Because progress is reconstructed from observed Spec rather
than a stored step index, increasing the target simply places the observed
new revision at an earlier point on the new curve.

The planner may issue additional new Spec while earlier replicas remain
pending. Growth is limited by:

1. the latest `target + maxSurge` envelope;
2. the remaining projected pending allowance;
3. the largest-replica-fraction coordination bound; and
4. any per-role raw API limits.

Ready does not drive the new-side progress calculation. It authorizes pending
capacity and old-revision drain. This prevents both failure modes of using
Ready as rollout progress: repeatedly reissuing work already present in Spec,
and globally stalling on one slow pod.

An increased target may also increase percentage-based `maxSurge` and
`maxUnavailable`. These values are resolved from the same target snapshot and
remain constant for that reconcile.

### Scale Down

The no-shrink target clamp is removed. A target below new Spec is represented
explicitly and is not interpreted as rollout completion.

Scale-down has two stages:

1. **Restore the target rollout envelope.** For each role:

   ```text
   envelopeExcess = max(0, oldSpec + newSpec - (target + maxSurge))
   reduction      = min(envelopeExcess, safeReduction)
   ```

   Allocate `reduction` to old revisions first, newest revision first. This
   turns an autoscaler downscale into useful rollout work by removing obsolete
   capacity instead of updated capacity.

2. **Converge the new revision exactly.** If removing every eligible old
   replica cannot restore the envelope, the remainder may shrink the new
   revision using the same safe-reduction budget. Once old Spec is zero, any
   `newSpec > target` is removed without leaving the rolling-update path first.

The implementation must not calculate independent old and new safe-drain
budgets. They spend the same `safeReduction`; otherwise stale Ready could
authorize two reductions against one unit of availability.

If availability is insufficient, the downscale pauses even though the target
is lower. This can occur when many Ready replicas are already committed to
termination. Subsequent LWS status changes or the bounded requeue resume
progress.

The controller does not scale new down merely because old capacity is still
temporarily above the steady target. Up to `maxSurge` remains valid rollout
capacity. This avoids deleting updated pods only to recreate them to finish
the replacement.

### Replica Fractions and Moving Targets

PR #907 defines a fraction scale independently for each rollout side:

```text
sideSteps               = max(roleSizes)
smallestReplicaFraction = 1 / max(roleSizes)
largestReplicaFraction  = 1 / minPositive(roleSizes)
```

At step `k`, integer projection gives every role its count on the same
normalized curve. The smallest fraction is the finest shared rollout tick;
the largest fraction is the maximum rounding skew represented by one replica
of the smallest positive role.

This KEP retains that model for revision coordination:

- The old side remains anchored to `initialOld`, so a scaler write cannot
  rewrite rollout history or resurrect drained old replicas.
- The new side uses the latest target vector. A target increase extends or
  reshapes the curve, and observed new Spec is projected onto that curve.
- A target decrease may place new Spec beyond the end of the new curve. The
  capacity-correction phase handles that excess; the growth-only fraction
  calculation must not treat it as successful final convergence.
- After correction, the ordinary fraction planner resumes from the observed
  Spec without a stored step number.

Capacity reduction is not modeled as a reverse rollout. It must not scale an
old revision back up or undo old-side fraction progress. The fraction model is
used to keep role/revision progress coordinated; the old-first capacity rule
decides which revision supplies a requested reduction.

If target changes alter role ratios, proposed actions remain subject to the
existing largest-replica-fraction skew bound. A role may wait at that boundary
while another role catches up or acquires safe capacity.

### Multiple Old Revisions

An interrupted A-to-B-to-C rollout can leave A and B as old revisions while C
is the target. Capacity correction follows the same newest-first order as
ordinary rollout drain:

1. drain B before A;
2. consider a revision retired when all its role Specs are zero; and
3. do not wait for a retired revision's stale status or terminating pods
   before considering an older revision, provided committed availability
   remains sufficient.

Whole-revision coordination remains a preference, not permission to bypass
availability. If retiring B for every role would exceed any role's remaining
safe-reduction budget, the controller performs only a safe coordinated step
or waits.

### Changing and Oscillating Targets

The latest target is authoritative. No target-generation queue is introduced:

```text
8 -> 12 -> 5 -> 9
```

does not require completing 12 or 5 before moving toward 9. Every pass
recomputes from observed Spec and Ready toward the target it read for that
pass.

Safety is independent of target monotonicity:

- a higher target opens growth only within current surge and pending bounds;
- a lower target opens reduction only within current committed availability;
- an inherited bound violation is repaired rather than amplified; and
- no pass scales the same LWS both down and up.

Efficiency under a rapidly oscillating custom autoscaler is not guaranteed.
Users who require dampening should configure their autoscaler's stabilization
and rate policies. The controller guarantees convergence once the target and
pod readiness eventually stabilize.

### Completion and Status

Rollout planning and completion use different predicates.

The rollout is structurally converged for a role only when:

```text
oldSpec == 0 && newSpec == target
```

The rollout is Ready only when it is structurally converged and:

```text
newReady == target
```

Equality replaces the existing `newSpec >= target` completion assumption,
which is valid only while new Spec is monotonic. A new-revision fleet above a
decreased target is incomplete and requires contraction.

`DisaggregatedSetRoleScaler.status.replicas` continues to report observed LWS
replicas aggregated across revisions. It may lag Spec while pods terminate.
The DisaggregatedSet remains Progressing until old revisions are gone and the
new revision is at the latest target in both Spec and Ready.

### API and Compatibility

This proposal adds no fields, resources, or feature gates. It changes
controller behavior only when a resolved replica target changes during a
rolling update.

The change is intentionally observable: a scale-down that was previously
deferred can now terminate old-revision pods before rollout completion. The
same `maxUnavailable`, `maxSurge`, and autoscaler policies already configured
by the user bound that behavior.

Downgrading to a controller without this feature is safe. The older controller
reintroduces the no-shrink behavior and may delay further scale-down, but no
new persisted state requires migration or interpretation.

### Observability

Existing scaling events should identify the reason and target snapshot. Logs
for every applied step should include, per role:

- old and new Spec;
- old and new committed Ready;
- resolved target;
- pending allowance and pending count;
- surge ceiling and availability floor; and
- whether a reduction was capacity correction or ordinary rollout drain.

The implementation should expose enough information to distinguish a rollout
waiting for readiness from one repairing a decreased target. New API status
conditions are not required for the initial implementation.

### Test Plan

[X] I/we understand the owners of the involved components may require updates
to existing tests to make this code solid enough prior to committing the
changes necessary to implement this enhancement.

#### Prerequisite Testing

PR #907 must retain tests proving:

- new Spec can advance while earlier replicas are pending;
- pending work never exceeds its projected allowance;
- old drain uses committed Ready and cannot spend stale Ready twice;
- raw per-role surge and availability limits hold after every transition; and
- old Spec is monotonic downward and new Spec is monotonic upward when the
  target itself is fixed.

#### Unit Tests

Add a transition harness that changes the target between reconciles and checks
all safety invariants after every controller-authored action. Cover:

- target increase while new Spec is ahead of new Ready;
- target increase beyond `initialOld`;
- target decrease above, equal to, and below new Spec;
- target decrease below aggregate old-plus-new Spec;
- target decrease to zero;
- old-first reduction and the case where old replicas are insufficient;
- new-revision shrink after old Spec reaches zero;
- one shared safe-reduction budget across old and new shrink;
- stale `Ready > Spec` after an earlier reduction;
- percentage budgets recalculated from a changed target;
- imbalanced two-role and N-role targets;
- one role scaling up while another scales down;
- smallest- and largest-replica-fraction coordination after target rebasing;
- multiple old revisions with newest-first drain;
- added, removed, and zero-target roles;
- rapid `8 -> 12 -> 5 -> 9` target changes; and
- controller restart from every intermediate observed state.

For small replica vectors, exhaustively enumerate readiness progress, targets,
and integer surge/unavailable values. Assert:

```text
controller does not grow while total Spec exceeds target + maxSurge
a growth transition ends at or below target + maxSurge
controller reduction never crosses the availability floor
pending growth never exceeds the pending allowance
old revisions never scale up
the same LWS is not scaled both directions in one pass
stable target plus eventual readiness converges to old=0, new=target
```

#### Integration Tests

- Update `DisaggregatedSetRoleScaler.spec.replicas` upward during an active
  rollout and verify the reconciler consumes the new target before all
  previously issued pods are Ready.
- Update the scaler downward to a value below new Spec and verify old Spec is
  reduced first, followed by exact new-target convergence.
- Verify scaler watch events enqueue the parent DisaggregatedSet during a
  rollout.
- Verify Static replica changes follow the same resolved-target behavior and
  do not create a new revision.
- Verify status aggregation remains consistent across old, new, and
  terminating replicas.

#### End-to-End Tests

Use deterministic startup delays and direct `/scale` writes rather than an
uncontrolled metric source for the core behavior tests.

1. **Mid-rollout scale-up:** begin an imbalanced, slow-pod rollout; wait until
   new Spec exceeds new Ready; raise the scaler target; prove new Spec advances
   toward the higher target before the prior wave becomes fully Ready.
2. **Mid-rollout scale-down:** lower the target below current new Spec; prove
   aggregate Spec decreases while an old revision still exists, old Spec is
   selected first, and final new Spec/Ready equals the lower target.
3. **Safety observation:** at every sampled transition, assert the surge,
   committed-availability, pending, and role-fraction bounds.
4. **Eventual convergence:** require old Spec and old observed replicas to
   reach zero and only the target revision to remain active.

An HPA-backed smoke test may additionally verify integration with HPA
stabilization, but deterministic `/scale` tests are the release-blocking
coverage.

### Graduation Criteria

This behavior graduates with DisaggregatedSet and
`DisaggregatedSetRoleScaler`.

**Alpha:**

- moving-target reconciliation for both Static and External roles;
- scale-up, old-first scale-down, and new-revision contraction;
- transition-level invariant coverage;
- deterministic scale-up and scale-down end-to-end tests; and
- user documentation describing rollout-time scaling semantics.

**Beta:**

- production feedback from autoscaled rolling updates;
- metrics or structured events sufficient to diagnose blocked target
  convergence; and
- no unresolved correctness issues involving target oscillation, status lag,
  or multiple old revisions.

**Stable:** follows the graduation requirements of KEP-766 and KEP-849.

## Implementation History

- 2026-09-02: Initial KEP draft.

## Drawbacks

- The rolling-update executor gains a second direction for new-revision Spec;
  its previous monotonic-growth assumption is simpler to reason about.
- A target reduction can terminate pods sooner than the current no-shrink
  behavior, which makes correct availability accounting essential.
- Stateless target rebasing can produce a different sequence of integer
  fraction steps after every target change, although hard bounds remain stable.
- Autoscaler oscillation may waste pod startup work even when all safety
  constraints are preserved.

## Alternatives

### Keep the Current No-Shrink Guard

Continue treating the scaler value only as the post-rollout target. This is
the simplest and safest behavior, but it can retain expensive excess capacity
for the full duration of a slow rollout and does not meet the goal.

### Support Only Scale Up During a Rollout

PR #907's Spec/Ready and pending-work model makes upward target changes much
simpler than downward changes. Shipping only scale-up would address emergency
capacity growth with less risk, while preserving deferred scale-down.

This remains a viable staged implementation, but not the final behavior: long
rollouts would still ignore legitimate cost- or load-driven reductions.

### Restart the Rollout When the Target Changes

Snapshot the current aggregate fleet as a new rollout source every time the
target changes. This gives each transition a fixed curve, but HPA updates could
continually reset progress, require new persistent annotations, and obscure
newest-first revision history. Recomputing from observed state is simpler and
restart-safe.

### Encode Replica Counts in the Revision

Include replicas in the revision hash so every scale event creates a new
revision. This conflates capacity with application version, creates LWS and
Service churn under autoscaling, and can leave many short-lived revisions.
Replica targets deliberately remain independent of revision identity.
