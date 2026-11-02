---
title: "Canary Rollouts with Argo CD"
linkTitle: "Argo CD Canaries"
weight: 55
description: >
  Promote a DisaggregatedSet percentage canary through Git, report its health in stock Argo CD, and optionally expose manual actions.
---

Argo CD can sync a DisaggregatedSet and change its canary percentage without an Argo CD source patch. LWS manages the replica groups; Argo CD applies the desired manifest from Git. A small health customization lets Argo CD distinguish an intentional, Ready canary hold from a rollout that is still progressing.

{{% alert title="Work in progress" color="warning" %}}
This guide describes the proposed `spec.rolloutStrategy.canaryPercent` field. Install **both the matching canary-enabled LWS controller and CRDs**; a controller or CRD without this field cannot provide the behavior described here. The Argo CD configuration below was checked with stock Argo CD v3.5.6. No custom Argo CD image is required.
{{% /alert %}}

The percentage applies to replica groups in each role and sub-role, with Ordinal or Hash identities. It is **not a traffic weight**: your inference router still decides which revision serves a request. See [Rollout Strategy](../rollout-strategy/#one-percentage-for-all-roles) for rounding, availability budgets, scaling, and layout restrictions.

## Promote through Git

Start with a healthy revision A. In one Git commit, change the intended role templates to revision B **and** set the initial percentage. For a zero-progress starting point, add this to the existing DisaggregatedSet manifest while making the template changes:

```yaml
spec:
  rolloutStrategy:
    canaryPercent: 0
```

This is a manifest fragment, not a replacement for the existing `spec`. Keep the roles and other configuration. Committing the template first and the percentage later can allow the rollout to advance before the hold is applied.

After Argo CD syncs the commit and the hold becomes Healthy, promote through later Git commits. With four Prefill and eight Decode groups per slice, the settled counts are:

| Percentage | Old Prefill / Decode | New Prefill / Decode |
| --- | --- | --- |
| `0` | 4 / 8 | 0 / 0 |
| `25` | 3 / 6 | 1 / 2 |
| `50` | 2 / 4 | 2 / 4 |
| `100` | 0 / 0 | 4 / 8 |

Wait for each requested hold, check the workload, and then commit the next percentage. Readiness and rollout budgets still govern every transition; temporary surge groups are not shown in the table. If autoscaling changes demand, LWS recalculates both shares.

Initial creation without an old revision starts the full workload, even with `0`. A positive partial canary (`1`–`99`) needs at least two groups in each active logical role; `0` can hold a one-group role entirely old. Lowering the percentage does not rewind an already advanced rollout. To request a rollback without a hold, restore the stable templates and set `100` together; normal rollout safety checks still apply.

## Report canary health

Stock Argo CD has no built-in DisaggregatedSet health check. It can sync the resource, but without a customization the DisaggregatedSet does not contribute a readiness assessment to Application health. An Application can therefore look Healthy while its DisaggregatedSet is still rolling out.

Save the following as `argocd-canary-health.yaml`. This is a merge patch for the existing `argocd-cm` ConfigMap:

```yaml
data:
  resource.customizations.health.disaggregatedset.x-k8s.io_DisaggregatedSet: |
    local waiting = {status = "Progressing", message = "Waiting for the current DisaggregatedSet status"}
    local generation = obj.metadata and obj.metadata.generation
    if generation == nil or obj.status == nil or obj.status.observedGeneration ~= generation then
        return waiting
    end

    local available = nil
    local progressing = nil
    for _, condition in ipairs(obj.status.conditions or {}) do
        if condition.observedGeneration == generation and condition.status == "True" then
            if condition.type == "Progressing" then
                progressing = condition
            elseif condition.type == "Available" then
                available = condition
            end
        end
    end

    if progressing ~= nil then
        local status = "Progressing"
        if progressing.reason == "CanaryHeld" then
            status = "Healthy"
        end
        return {status = status, message = progressing.message or progressing.reason}
    end
    if available ~= nil then
        return {status = "Healthy", message = available.message or "All roles are ready"}
    end
    return waiting
```

Choose your cluster context explicitly; replace `YOUR_CONTEXT` and, if necessary, the Argo CD namespace:

```sh
kubectl --context YOUR_CONTEXT -n argocd patch configmap argocd-cm \
  --type merge --patch-file argocd-canary-health.yaml
```

Refresh the Application. The customization only accepts status and conditions observed for the current resource generation:

- `CanaryHeld` becomes Healthy: the requested intermediate state has settled, not the whole rollout.
- `Available=True` becomes Healthy on completion.
- Missing or stale status, an active rollout, or `CanaryBlocked` remains Progressing, with the controller's explanation when available.

The patch preserves unrelated ConfigMap entries but replaces this kind's existing health customization. If Git or Helm manages `argocd-cm`, put the same entry in that source of truth so the change is not reverted. Review any existing customization before replacing it.

## Optional manual actions

Git commits are sufficient for promotion. To also offer **Set Canary Percentage** and **Promote All Roles** in Argo CD's resource menu, save this second merge patch as `argocd-canary-actions.yaml`:

```yaml
data:
  resource.customizations.actions.disaggregatedset.x-k8s.io_DisaggregatedSet: |
    discovery.lua: |
      local roles = obj.spec and obj.spec.roles or {}
      local strategy = obj.spec and obj.spec.rolloutStrategy
      local percent = strategy and strategy.canaryPercent
      return {
          ["promote-full"] = {
              ["disabled"] = #roles == 0 or percent == nil or percent == 100,
              ["displayName"] = "Promote All Roles",
          },
          ["set-canary-percent"] = {
              ["disabled"] = #roles == 0,
              ["displayName"] = "Set Canary Percentage",
              ["params"] = {{["name"] = "percent"}},
          },
      }
    definitions:
      - name: set-canary-percent
        action.lua: |
          local params = actionParams or {}
          local percent = tonumber(params.percent)
          if percent == nil or percent < 0 or percent > 100 or percent % 1 ~= 0 then
              error("percent must be an integer between 0 and 100", 0)
          end
          if obj.spec == nil or obj.spec.roles == nil or #obj.spec.roles == 0 then
              error("a DisaggregatedSet must contain roles", 0)
          end
          obj.spec.rolloutStrategy = obj.spec.rolloutStrategy or {}
          obj.spec.rolloutStrategy.canaryPercent = percent
          return obj
      - name: promote-full
        action.lua: |
          if obj.spec and obj.spec.rolloutStrategy and obj.spec.rolloutStrategy.canaryPercent ~= nil then
              obj.spec.rolloutStrategy.canaryPercent = 100
          end
          return obj
```

```sh
kubectl --context YOUR_CONTEXT -n argocd patch configmap argocd-cm \
  --type merge --patch-file argocd-canary-actions.yaml
```

The first action asks for `percent`, an integer from 0 to 100. The second sets an existing percentage to 100. Neither bypasses readiness or availability budgets. Users still need Argo CD permission to run the respective resource action. As with the health patch, this replaces the action customization for this kind, not other ConfigMap entries.

{{% alert title="Choose one owner for promotion" color="warning" %}}
These actions change the live DisaggregatedSet, **not Git**. A later sync or automatic self-heal can restore the percentage from Git. Prefer Git-owned progression. Do not mix manual actions, Git updates, and another promotion controller without explicitly assigning ownership of the percentage.
{{% /alert %}}

## Optional external automation

Argo Rollouts is separate from Argo CD. Automated pauses and analysis require an external integration, such as a step plugin or explicit promotion Jobs, that updates the percentage and waits for the corresponding current-generation LWS status. **No such integration is installed by this guide or shipped with this feature.** A driver Rollout's health alone is not proof that the DisaggregatedSet reached the requested hold.

This is not native DisaggregatedSet `workloadRef` support. Rollouts `setWeight` does not change `canaryPercent`, and the percentage does not configure a traffic router. The integration must also define failure and rollback behavior; a failed analysis does not inherently roll a DisaggregatedSet back.

If an external controller owns the percentage while Argo CD owns the templates, the following is an **Application fragment** for that ownership arrangement. Replace the resource name and namespace:

```yaml
metadata:
  annotations:
    argocd.argoproj.io/compare-options: ServerSideDiff=true
spec:
  syncPolicy:
    syncOptions:
      - ServerSideApply=true
      - RespectIgnoreDifferences=true
  ignoreDifferences:
    - group: disaggregatedset.x-k8s.io
      kind: DisaggregatedSet
      name: canary
      namespace: inference
      jsonPointers:
        - /spec/rolloutStrategy/canaryPercent
```

First let Git sync the new templates and their initial percentage hold. **Only then** enable the ignore rule and hand progression to the external controller. Otherwise Git may not be able to establish the initial hold on an existing resource.

`RespectIgnoreDifferences` preserves the controller-owned percentage during sync, including automatic sync or self-heal if already enabled, rather than merely hiding its diff. `ServerSideDiff` uses a server-side dry-run comparison; mutating-webhook changes are excluded unless separately enabled in Argo CD. Ignore only the percentage, not the roles or templates. Keep the template and role layout fixed during an active automated sequence unless the integration explicitly handles concurrent changes; these settings are not a lock.

For a purely Git-driven canary, do not add this ignore rule: Git should continue to own and apply every percentage change.
