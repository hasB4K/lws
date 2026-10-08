---
title: "Roles in DisaggregatedSet"
linkTitle: "Roles"
weight: 10
description: >
  Configuring roles (prefill, decode, encode) and their independent pod specifications in DisaggregatedSet.
---

A **Role** in DisaggregatedSet represents a distinct operational phase in a disaggregated serving architecture (e.g., `prefill`, `decode`, or `encode`). Each role defines its own pod specifications, scaling properties, and replica topology.

## Relationship to Child LeaderWorkerSets

Each role defined in a `DisaggregatedSet` specification maps directly to an independent child `LeaderWorkerSet` managed by the DisaggregatedSet controller:

```
DisaggregatedSet "my-inference"
├── roles[0]: prefill  →  LeaderWorkerSet "my-inference-0-<rev>-prefill"
├── roles[1]: decode   →  LeaderWorkerSet "my-inference-0-<rev>-decode"
└── roles[2]: encode   →  LeaderWorkerSet "my-inference-0-<rev>-encode"
```

Child LeaderWorkerSets follow the naming convention:
`<DisaggregatedSet-name>-<slice>-<revision-hash>-<role-name>`

{{% alert title="Note" color="info" %}}
The revision hash in the child resource name is dynamic across updates. Always select child resources using Kubernetes labels (`disaggregatedset.x-k8s.io/name`, `disaggregatedset.x-k8s.io/role`, `disaggregatedset.x-k8s.io/slice`) rather than hardcoding names.
{{% /alert %}}

---

## Role Configuration Fields

A `DisaggregatedSet` spec defines a `roles` list where each entry represents a role (`DisaggregatedRoleSpec`):

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Unique name for this role within the set (e.g., `prefill`, `decode`, `encode`). |
| `scaling` | `RoleScaling` | Optional scaling configuration. `scaling.mode: External` delegates replica management to an external autoscaler (e.g., HPA/KEDA via `DisaggregatedSetRoleScaler`). Default is `Static`. |
| `spec.replicas` | `*int32` | Number of LWS replicas (pod groups) for this role per slice. |
| `spec.leaderWorkerTemplate` | `LeaderWorkerTemplate` | Full pod template defining the leader and worker pod containers, resource requests/limits, restart policies, and subgroup configurations for this role. |
| `spec.rolloutStrategy` | `RolloutStrategy` | Optional rolling update configuration for this role. DisaggregatedSet coordinates updates across roles to maintain capacity ratios. |

---

## Example Multi-Role Configuration

Here is an example `DisaggregatedSet` defining independent `prefill` and `decode` roles with different pod group sizes and hardware accelerator configurations:

{{< include file="examples/disaggregatedset/roles/prefill-decode.yaml" lang="yaml" >}}

---

## Independent Per-Role Capabilities

Because each role maps to an independent child LeaderWorkerSet, each role inherits all core LWS features tailored to its specific workload phase:

1. **Heterogeneous Hardware:**
   Prefill servers can run on high-bandwidth, high-compute accelerator nodes (e.g., 8-GPU tensor-parallel groups), while decode servers run on memory-optimized nodes (e.g., 2-GPU or 4-GPU groups).
2. **Independent Group Sizes:**
   Each role configures its own `leaderWorkerTemplate.size` and optional `subGroupPolicy`.
3. **Independent Autoscaling:**
   Prefill and decode roles can be scaled dynamically based on distinct metric signals (e.g., time-to-first-token vs. inter-token-latency) using `DisaggregatedSetRoleScaler`.
4. **Dedicated Storage:**
   Each role can configure its own `volumeClaimTemplates` for local model caching or intermediate tensor offloading.

## Virtual roles: shared templates, independent routing pools

Optional `subRoles` partition one parent's configuration-identical LWS groups:

```yaml
roles:
- name: decode
  subRoles:
  - name: interactive
    replicas: 3
  - name: batch
    replicas: 1
  spec:
    groupIdentity: Ordinal # The default; Hash sub-roles are deferred.
    leaderWorkerTemplate: # The same template is used by both children.
      # ...
```

The parent has four groups per slice: its replica target is the sum of its
children, and parent `spec.replicas` is ignored. Parent `scaling` must be omitted.
Each child can instead use `scaling.mode: External` with `replicas` omitted;
the controller creates `<ds>-<parent>-<child>` RoleScalers. External scaling
retains the single-slice alpha restriction; Static children support multiple slices.

Every Pod in an assigned group gets the controller-owned
`disaggregatedset.x-k8s.io/subrole` label. Select it together with the existing
set, role, and slice labels for routing; the controller does not create Services.
A group counts Ready only when its leader and workers are Ready and carry one
coherent assignment. Status exposes child counts and `SubRolesAssigned`.
Changing child names, counts, or scaling modes does not create a template revision.

During rollouts, each child is an ordinary role with its own availability,
fractional coordination and revision-completeness rules. Initially required
old children retire together, like ordinary roles. Each child inherits the
parent's raw `maxSurge`/`maxUnavailable` settings: percentages resolve against
that child's target, rounding surge up and unavailability down. Budgets add;
two children with `maxSurge: 1` permit two normal surge groups, with no separate
parent-wide cap. The default/opt-in scaling policy and ordinary emergency
fallback rules apply unchanged. Shrink prepares the actual retained Ordinal
prefix and reobserves coherent assignments before reducing the physical count.
Accepted plans resume after restarts or partial writes before another plan can
spend the same capacity. Native health recovery and restart-budget finalizers
remain unchanged.

In-place relabeling does not promise zero Pod churn during rollouts: the ordinary
executor may shrink one child before growing another even when their final sum
is unchanged. Removing `subRoles` clears group labels and transaction metadata
without replacing the groups. Unpartitioned LWS behavior is unchanged.

This initial implementation rejects `subRoles` with `groupIdentity: Hash`.
Hash support is a follow-up because independent child availability requires
controlling which native groups are deleted, not just their aggregate count.
