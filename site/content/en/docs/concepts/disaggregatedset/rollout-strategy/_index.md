---
title: "Rollout Strategy"
linkTitle: "Rollout Strategy"
weight: 50
description: >
  How DisaggregatedSet coordinates updates across roles, with an animated example of the fractional window and rollout budgets.
---

A serving application may need Encode, Prefill, and Decode to work together. Updating all the Encode groups while barely starting Prefill and Decode can leave new groups without the matching roles they need.

DisaggregatedSet coordinates their replacement **by proportion, not by equal numbers of groups**. The fractional coordination window limits how far one role's rollout can lead another. Separately, each role's `maxSurge` and `maxUnavailable` control extra replicas and availability.

The example below replaces **4 Encode, 13 Prefill, and 7 Decode groups** from revision A with revision B.

<figure class="td-max-width-on-larger-screens">
<!-- Keep a compact area for native playback controls below the picture. -->
<div style="position: relative; padding-top: calc(58.333333% + 2rem);">
<video controls autoplay loop muted playsinline preload="metadata" poster="/images/disaggregatedset-fractional-rollout-poster.png" class="img-fluid d-block" width="1200" height="700" style="position: absolute; inset: 0; width: 100%; height: 100%; object-fit: contain; object-position: top; background: #fff;" aria-label="A rollout of 4 Encode, 13 Prefill, and 7 Decode groups. Old groups leave as replacements become Ready, with removal progress kept inside a moving 25-percentage-point window and availability checked separately.">
  <source src="/videos/disaggregatedset-fractional-rollout.mp4" type="video/mp4">
  <a href="/videos/disaggregatedset-fractional-rollout.mp4">Download the rollout animation.</a>
</video>
</div>
<figcaption class="small text-muted mt-2">Old A groups retire as new B groups become Ready. The moving 25-point window keeps the roles progressing together; both rollout budgets are 10%. Each square is a single-Pod group. Timing is illustrative.</figcaption>
</figure>

## Set the rollout budgets

The unit is a **replica group**, which can contain a leader Pod and worker Pods. A group counts as Ready only when all its expected Pods are Ready. A running leader alone is not enough.

For example, set both budgets to `10%` in each role's `spec.rolloutStrategy.rollingUpdateConfiguration`:

```yaml
rolloutStrategy:
  rollingUpdateConfiguration:
    maxSurge: 10%
    maxUnavailable: 10%
```

- `maxSurge` allows extra groups above the desired count, including groups still starting. Percentages round **up**.
- `maxUnavailable` allows Ready capacity to fall below the desired count. Percentages round **down**.

The minimum Ready count is `desired - maxUnavailable`; the maximum requested count is `desired + maxSurge`. Both count across old and new revisions of the role. Terminating Pods can take time to disappear.

## One percentage for all roles

Use one set-wide percentage to hold a rollout for canary analysis:

```yaml
spec:
  rolloutStrategy:
    canaryPercent: 25
```

This requests 25% of each role's replica groups on the new revision, independently in every slice. It works with both Ordinal and Hash groups, including virtual roles: each sub-role gets its own share. It does **not** set a traffic percentage or account for different GPU sizes.

- Set `0` with the template change to start with all surviving groups old. Raise it to advance; `100` completes the rollout. Without an old revision, initial creation still starts the full desired workload. Omitting the field leaves the existing rollout behavior unchanged.
- Counts round up, keeping at least one old and one new group during a partial canary. At 25%, 4 groups become 3 old + 1 new; 8 groups become 6 old + 2 new. With just 2 groups, any partial percentage leaves 1 old + 1 new.
- Autoscaling up can add groups to both revisions. Historical templates, budgets and membership are preserved; fully drained old history is not recreated. New-group growth still follows the selected scale-up policy.
- Scale-down keeps its selected policy. At 25%, scaling 6 old + 2 new down to 4 gives 3 old + 1 new with `Immediate`, but 2 old + 2 new with `Deferred`. The latter retains started target groups, so its 50% share can remain until promotion or another replica change.
- Lowering the percentage does not undo rollout progress. To roll back immediately, revert the template and set `canaryPercent: 100` together.
- The embedded LWS `partition` field remains unsupported. This control belongs to DisaggregatedSet, not the child LWS rollout.

A settled, fully Ready hold reports `Progressing=True` with reason `CanaryHeld`, and its message includes the percentage and observed new/desired counts. For Git-driven promotion, health configuration, and optional manual actions with stock Argo CD, see [Canary Rollouts with Argo CD](../argo-cd/). This is not native Argo Rollouts workload support.

There are two explicit limits. A positive partial canary (`1`–`99`) cannot split a one-group logical role: the controller reports `CanaryBlocked` until it has at least two groups or the percentage reaches 100. Zero percent can hold a singleton entirely old. Scaling an existing partial canary down to one can therefore leave it above desired until that action is taken. Also, a surviving old revision must have the same active roles and sub-roles; renaming, adding/removing active sub-roles, or changing a leaf between zero and nonzero requires finishing that layout change at 100 before starting a canary. Scaling the whole workload to zero remains allowed.

## What is the fractional window?

The window limits how far one role can get ahead of the others. Its width is **`1 / smallest non-zero role count`**, calculated automatically.

For the old revision, progress is the fraction of its original groups removed. The window moves with the least-advanced role; the others can advance within its width. **It is not a batch size.**

The new revision's growth is coordinated separately, using the fraction of each role's target already requested.

## The window is not an unavailability allowance

Fitting inside the window is not enough: a removal must also respect each role's **`maxUnavailable` budget**.

The new revision counts as replacement serving capacity only once it has a Ready group in **every required role**. Groups still starting do not count as Ready capacity.

## Watch availability during a rollout

The DisaggregatedSet reports two different signals:

- `Available` is **completion**: every role has reached its desired count, with all of it Ready and updated. It is False during every rollout and scale-up, even when nothing has stopped serving.
- `MinimumReplicasAvailable` is **serving capacity**: every role and sub-role has at least its availability floor. It stays True through a rollout or scale-up that remains within each role's `maxUnavailable`. Use it for readiness checks and alerts.

Each entry in `status.roleStatuses` (and its `subRoleStatuses`) shows the numbers behind it:

- `minAvailableReplicas` is the floor, `desired - maxUnavailable` per slice, totalled across slices. It is unset while the role's target is unknown, and the condition is then `Unknown`, unless another role is already below its floor, which makes it `False`.
- `availableReplicas` counts Ready groups that can actually serve: those in a revision of a slice where **every required role** has a Ready group. A Ready Prefill group in the old revision does not count without a Ready old Decode, even when new Decode groups are Ready.

For example, with `maxUnavailable: 1`, a 4-group Decode role being replaced reports `minAvailableReplicas: 3`. While three old groups still serve, `MinimumReplicasAvailable` stays True; if one more is lost before its replacement is Ready, it becomes False and names the role, for example `decode (2/3)`.

A scale-up is judged against the new target. Raising Decode from 4 to 6 with `maxUnavailable: 1` sets the floor to 5, so the condition is False until one new group is Ready. Use a larger `maxUnavailable`, such as a percentage, if scale-ups should not count as a shortfall.

These checks govern ordinary rollout steps; they cannot prevent unrelated Pod or node failures or guarantee enough GPU capacity. For changing replica counts, see [Scaling during a rollout](../#scaling-during-a-rollout). The [design proposal](https://github.com/kubernetes-sigs/lws/tree/main/keps/766-DisaggregatedSet#n-dimensional-rolling-update-algorithm) also covers interrupted rollouts and bounded recovery.
