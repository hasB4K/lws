# DisaggregatedSet Percentage Canary

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
- [Proposal](#proposal)
  - [Replica counts and readiness](#replica-counts-and-readiness)
  - [Scaling and interrupted rollouts](#scaling-and-interrupted-rollouts)
  - [Rollback and explicit limitations](#rollback-and-explicit-limitations)
  - [Status and progressive-delivery tools](#status-and-progressive-delivery-tools)
- [Implementation](#implementation)
- [Test plan](#test-plan)
- [Alternatives](#alternatives)
- [Implementation history](#implementation-history)
<!-- /toc -->

## Summary

Add an optional `spec.rolloutStrategy.canaryPercent` to hold a DisaggregatedSet rollout at one percentage across all roles and sub-roles. It supports both Ordinal and Hash groups. This is an independent alternative to per-role integer partitions, built directly on the existing DisaggregatedSet planner. It does not enable the unsupported `spec.roles[].spec.rolloutStrategy.rollingUpdateConfiguration.partition` field.

## Motivation

A new inference engine should be able to serve a small amount of real traffic before the full workload is replaced. Today a DisaggregatedSet rollout keeps advancing as readiness permits. Operators need a deliberate hold, then promotion after manual checks or automated analysis.

One percentage avoids making the promotion tool calculate and continuously update a separate integer for every role, virtual role, and autoscaler target. This controls replica groups, not traffic weights or GPU capacity; the routing layer still decides where requests go.

## Proposal

```yaml
spec:
  rolloutStrategy:
    canaryPercent: 25
```

The field is an optional integer from 0 to 100. Omitted or 100 means no canary hold. Set 0 together with a template change to retain all surviving old groups; raising the percentage advances the rollout. Changing the percentage does not change the revision hash. Initial creation without an old revision still starts the full desired workload.

### Replica counts and readiness

For each logical role, independently in each slice, let `d` be its resolved desired group count and `p` the percentage. During a partial hold (`0 < p < 100`, `d >= 2`):

```text
new = min(d - 1, max(1, ceil(d * p / 100)))
old = d - new
```

The new count rounds up but keeps at least one group on each revision. At 25%, four groups become three old and one new; eight groups become six old and two new. For a physical role containing sub-roles, the formula applies to each sub-role, not the parent's aggregate. At 0%, all desired groups remain old.

The planner receives an internal old-group hold derived from this formula. Existing availability budgets, revision completeness, and group assignment still apply. A hold is not declared until every rolling slice has no further step and every requested group is Ready at the desired total. The cross-role coordination window can delay a faster role; the percentage is a bound, not an exact traffic split.

### Scaling and interrupted rollouts

A percentage recalculates both shares when desired replicas change. A 25% hold at four groups is three old plus one new; increasing desired to eight becomes six old plus two new. Before normal planning, the executor may grow the oldest surviving compatible old revision to fill missing old capacity. It counts **all** other old revisions, and never requests more old groups than fit within desired minus already-requested target groups. It spends no surge and performs no drain in that step.

Historical templates, rollout budgets, and frozen initial-replica annotations stay unchanged. Loading groups count as requested capacity; losing readiness does not authorize more old groups. In A → B → C, the old share is the aggregate of A and B, not a separate allowance for each.

Replenishing old capacity under either scale-up policy is an explicit exception for this opt-in mode: an intentionally held rollout might never finish, so waiting for completion would leave demand unserved. Target growth still follows ordinary Deferred pacing or Immediate gap-filling. Rollouts without an active percentage hold retain their existing behavior.

While held, old-side coordination compares all remaining old groups with today's desired counts. After Prefill grows from four to twenty while Decode stays at eight, judging Prefill against its original four groups can prevent Decode from reaching its requested share. Using current demand keeps both roles progressing toward the same percentage. The usual readiness checks still limit every deletion. Promotion to 100 returns to the ordinary coordination rules.

Scale-down honors `scaleDownDuringRollout`. At 25%, reducing six old plus two new groups from eight desired to four gives:

| Policy | Settled result | Explanation |
|---|---|---|
| Immediate | Three old, one new | Safely reduce excess target groups toward their recalculated share. |
| Deferred | Two old, two new | Keep started target groups; old groups absorb the reduction. The new share can remain at 50% until promotion or another demand change. |

### Rollback and explicit limitations

Lowering the percentage does not undo issued target progress. If the workload is already above desired, normal demand-driven surplus reduction still follows the selected scale-down policy. To roll back immediately, restore the stable template and set `canaryPercent: 100` in the same update.

- A positive one-group logical role cannot be split while keeping both revisions within desired. Report `CanaryBlocked` until every affected role has at least two groups or the percentage reaches 100. Scaling a held role from two to one can therefore retain excess groups until one of those actions is taken. Zero percent can hold a singleton entirely old.
- Replenishment requires an old revision with the same active logical-role layout and surviving groups for every active role. Renames, ordinary-role/sub-role conversion, and positive↔zero leaf changes are not mapped onto history. Report `CanaryBlocked`; finish the layout change at 100 before starting a canary. Scaling the whole set to zero remains allowed.
- Fully drained old history is never recreated. A completed rollout is not rewound by adding or lowering the percentage; without surviving old groups, the target converges normally.
- Integer rounding can make actual shares differ significantly for small roles: 99% of two groups remains one old plus one new until promotion to 100.

### Status and progressive-delivery tools

A settled hold reports current-generation `Progressing=True`, reason `CanaryHeld`, with a message containing the requested percentage and observed new/desired role counts. `MinimumReplicasAvailable` remains the independent availability signal. Completion remains `Available=True`, reason `AllRolesReady`. `CanaryBlocked` explains why the split cannot currently be achieved and what action is required.

Argo CD can recognize these conditions through a custom health check; Git-driven promotion only changes the top-level percentage field. A separate promotion controller can own that field while Git owns templates, provided Argo CD ignores and preserves the controller-owned field during sync. This does not make DisaggregatedSet a native Argo Rollouts `workloadRef`: a step plugin or explicit analysis/promotion Jobs must bridge the APIs. Traffic routing and analysis metrics are separate concerns.

The [Argo CD guide](../../site/content/en/docs/concepts/disaggregatedset/argo-cd/_index.md) provides complete health and optional manual-action configuration for stock Argo CD, along with field-ownership rules. No Argo CD source patch is required.

## Implementation

- Validate and preserve the new percentage field in the API and CRD. Per-role LWS partition rejection remains unchanged.
- Derive internal `CanaryOldReplicas` limits in the existing executor; do not send a partition to child LWS resources.
- Bound target growth and old drain by the hold, and allow safe Immediate surplus reduction without changing ordinary rollout behavior.
- Replenish surviving old capacity before planning; coordinate its aggregate share against current demand.
- Report a hold only after all rolling slices settle and all groups are Ready. Reuse the existing planner, observer, executor, and sub-role assignment logic.

## Test plan

- Percentage rounding, initial creation, unchanged revision hashing, zero/100 controls, and no rewind/revival.
- Held progress and completion with different surge/unavailability budgets; validate every simulated plan and availability floor.
- Ordinal/Hash and Deferred/Immediate growth/downscale; asymmetric Prefill growth and downscale followed by full promotion.
- Real observer and assignment paths for Ordinal/Hash virtual roles, preserving old annotations and checking actual group labels.
- Multiple old revisions, singleton and layout restrictions, settled status, and every-slice readiness.
- API-server/webhook persistence and range validation; unchanged rejection of unsupported per-role partitions.
- Independent Git → Argo CD → Argo Rollouts step-plugin → DisaggregatedSet live comparisons, including analysis failure and resync while held.

## Alternatives

Per-role integer partitions, proposed in [PR #1265](https://github.com/kubernetes-sigs/lws/pull/1265), reuse the familiar StatefulSet/LWS count. They let roles choose different fixed holds, but promotion tools must calculate and track each count as demand changes, and virtual roles need their own treatment. This prototype adapts the bounded-hold arithmetic without depending on that API or its commits. Only the percentage field is exposed by this alternative.

## Implementation history

- 2026-11-02: work-in-progress independent proposal for comparing set-wide percentage canaries with per-role integer partitions.
