---
title: DisaggregatedSet API
content_type: tool-reference
package: disaggregatedset.x-k8s.io/v1
auto_generated: true
description: Generated API reference documentation for disaggregatedset.x-k8s.io/v1.
---


## Resource Types 


  

## `DisaggregatedRoleSpec`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedRoleSpec}
    

**Appears in:**

- [DisaggregatedSetSpec](#disaggregatedset-x-k8s-io-v1-DisaggregatedSetSpec)


<p>DisaggregatedRoleSpec defines the configuration for a disaggregated role.
This structure embeds LeaderWorkerSetTemplateSpec from sigs.k8s.io/lws, with validation
to reject unsupported fields (RolloutStrategy.Type must be RollingUpdate,
RolloutStrategy.RollingUpdateConfiguration.Partition must not be set).</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name is the unique identifier for this role.</p>
</td>
</tr>
<tr><td><code>scaling</code><br/>
<a href="#disaggregatedset-x-k8s-io-v1-RoleScaling"><code>RoleScaling</code></a>
</td>
<td>
   <p>Scaling configures how the desired replica count for this role is
determined. Omit for the default Static mode (inline spec.replicas).</p>
</td>
</tr>
<tr><td><code>LeaderWorkerSetTemplateSpec</code> <B>[Required]</B><br/>
<a href="#leaderworkerset-x-k8s-io-v1-LeaderWorkerSetTemplateSpec"><code>LeaderWorkerSetTemplateSpec</code></a>
</td>
<td>(Members of <code>LeaderWorkerSetTemplateSpec</code> are embedded into this type.)
   <p>LeaderWorkerSetTemplateSpec defines the LWS template for this role.
Note: Spec.RolloutStrategy.Type must be RollingUpdate (or empty) and
Spec.RolloutStrategy.RollingUpdateConfiguration.Partition must not be set.
DisaggregatedSet handles rollouts across roles.</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSet`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSet}
    

**Appears in:**



<p>DisaggregatedSet is the Schema for the disaggregatedsets API</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedSetSpec"><code>DisaggregatedSetSpec</code></a>
</td>
<td>
   <p>spec defines the desired state of DisaggregatedSet</p>
</td>
</tr>
<tr><td><code>status,omitzero</code><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedSetStatus"><code>DisaggregatedSetStatus</code></a>
</td>
<td>
   <p>status defines the observed state of DisaggregatedSet</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetRoleRef`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleRef}
    

**Appears in:**

- [DisaggregatedSetRoleScalerSpec](#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScalerSpec)


<p>DisaggregatedSetRoleRef selects a specific role of a DisaggregatedSet in
the same namespace as the scaler.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name is the DisaggregatedSet name in the same namespace as the scaler.</p>
</td>
</tr>
<tr><td><code>role</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Role is the role name within the referenced DisaggregatedSet
(matches spec.roles[].name).</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetRoleScaler`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScaler}
    

**Appears in:**



<p>DisaggregatedSetRoleScaler exposes the /scale subresource for a single
(DisaggregatedSet, role) pair, allowing external autoscalers (HPA, KEDA,
or any /scale-aware controller) to drive that role's replica count
independently of the rest of the DisaggregatedSet.</p>
<p>The scaler name is stable across rolling updates of the target
DisaggregatedSet, which lets an autoscaler pointed at the scaler continue
to work when the underlying LeaderWorkerSet's revision-hashed name
changes.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>spec</code> <B>[Required]</B><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScalerSpec"><code>DisaggregatedSetRoleScalerSpec</code></a>
</td>
<td>
   <p>spec defines the desired state of DisaggregatedSetRoleScaler</p>
</td>
</tr>
<tr><td><code>status,omitzero</code><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScalerStatus"><code>DisaggregatedSetRoleScalerStatus</code></a>
</td>
<td>
   <p>status defines the observed state of DisaggregatedSetRoleScaler</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetRoleScalerSpec`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScalerSpec}
    

**Appears in:**

- [DisaggregatedSetRoleScaler](#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScaler)


<p>DisaggregatedSetRoleScalerSpec defines the desired state of a
DisaggregatedSetRoleScaler.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>targetRef</code> <B>[Required]</B><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleRef"><code>DisaggregatedSetRoleRef</code></a>
</td>
<td>
   <p>TargetRef selects the DisaggregatedSet and role this scaler drives.
The referenced DisaggregatedSet must live in the same namespace as
this scaler.</p>
</td>
</tr>
<tr><td><code>replicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>Replicas is the desired replica count for the referenced role.
Set by the /scale subresource (e.g., by an HPA or KEDA ScaledObject).
Read by the DisaggregatedSet controller when the referenced role is
configured with scaling.mode: External.</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetRoleScalerStatus`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScalerStatus}
    

**Appears in:**

- [DisaggregatedSetRoleScaler](#disaggregatedset-x-k8s-io-v1-DisaggregatedSetRoleScaler)


<p>DisaggregatedSetRoleScalerStatus defines the observed state of a
DisaggregatedSetRoleScaler.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>replicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>Replicas is the observed replica count of the role's current
new-revision LeaderWorkerSet. Reported to /scale readers.</p>
</td>
</tr>
<tr><td><code>selector</code><br/>
<code>string</code>
</td>
<td>
   <p>Selector is a label selector, in string form, matching pods of the
role's current new-revision LeaderWorkerSet. Used by HorizontalPodAutoscaler
to compute per-pod metrics against the correct pod set. The
DisaggregatedSet controller rewrites this on every rolling update
because the LeaderWorkerSet name changes with the revision hash.</p>
</td>
</tr>
<tr><td><code>observedGeneration</code><br/>
<code>int64</code>
</td>
<td>
   <p>ObservedGeneration is the .metadata.generation the status reflects.</p>
</td>
</tr>
<tr><td><code>conditions</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#condition-v1-meta"><code>[]k8s.io/apimachinery/pkg/apis/meta/v1.Condition</code></a>
</td>
<td>
   <p>Conditions represent the current state of the scaler.</p>
<p>Standard condition types include:</p>
<ul>
<li>&quot;Ready&quot;: the scaler has resolved its target and status is fresh</li>
<li>&quot;TargetMissing&quot;: the referenced DisaggregatedSet or role is not resolvable</li>
<li>&quot;Conflicting&quot;: another scaler in this namespace targets the same (set, role) pair</li>
</ul>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetSpec`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetSpec}
    

**Appears in:**

- [DisaggregatedSet](#disaggregatedset-x-k8s-io-v1-DisaggregatedSet)


<p>DisaggregatedSetSpec defines the desired state of DisaggregatedSet.
The all-or-nothing replica rule applies only to roles that do not use
external scaling; roles with scaling.mode == External source their
replica count from a DisaggregatedSetRoleScaler and are exempt.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>roles</code> <B>[Required]</B><br/>
<a href="#disaggregatedset-x-k8s-io-v1-DisaggregatedRoleSpec"><code>[]DisaggregatedRoleSpec</code></a>
</td>
<td>
   <p>Roles defines the list of roles (at least 2 required).
Each role has a unique name and its own configuration.</p>
</td>
</tr>
</tbody>
</table>

## `DisaggregatedSetStatus`     {#disaggregatedset-x-k8s-io-v1-DisaggregatedSetStatus}
    

**Appears in:**

- [DisaggregatedSet](#disaggregatedset-x-k8s-io-v1-DisaggregatedSet)


<p>DisaggregatedSetStatus defines the observed state of DisaggregatedSet.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>roleStatuses</code><br/>
<a href="#disaggregatedset-x-k8s-io-v1-RoleStatus"><code>[]RoleStatus</code></a>
</td>
<td>
   <p>RoleStatuses contains the status for each role.
The order matches spec.roles.</p>
</td>
</tr>
<tr><td><code>conditions</code><br/>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#condition-v1-meta"><code>[]k8s.io/apimachinery/pkg/apis/meta/v1.Condition</code></a>
</td>
<td>
   <p>conditions represent the current state of the DisaggregatedSet resource.
Each condition has a unique type and reflects the status of a specific aspect of the resource.</p>
<p>Standard condition types include:</p>
<ul>
<li>&quot;Available&quot;: the resource is fully functional</li>
<li>&quot;Progressing&quot;: the resource is being created or updated</li>
<li>&quot;Degraded&quot;: the resource failed to reach or maintain its desired state</li>
</ul>
<p>The status of each condition is one of True, False, or Unknown.</p>
</td>
</tr>
</tbody>
</table>

## `RoleScaling`     {#disaggregatedset-x-k8s-io-v1-RoleScaling}
    

**Appears in:**

- [DisaggregatedRoleSpec](#disaggregatedset-x-k8s-io-v1-DisaggregatedRoleSpec)


<p>RoleScaling configures how the desired replica count for a role is
determined.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>mode</code><br/>
<a href="#disaggregatedset-x-k8s-io-v1-RoleScalingMode"><code>RoleScalingMode</code></a>
</td>
<td>
   <p>Mode selects the source of the replica count:</p>
<ul>
<li>Static (default): read from the inline spec.replicas value.</li>
<li>External: the DisaggregatedSet controller auto-creates a
DisaggregatedSetRoleScaler named &quot;<!-- raw HTML omitted -->-<!-- raw HTML omitted -->&quot;
whose /scale subresource an external autoscaler drives.</li>
</ul>
</td>
</tr>
<tr><td><code>initialReplicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>InitialReplicas seeds the auto-created DisaggregatedSetRoleScaler's
spec.replicas so the role has a cold-start replica count before an
external autoscaler makes its first write. Applies only when Mode
is External. If unset, the role holds at 0 replicas until an
autoscaler writes a value; leaving it unset only works with
autoscalers that can scale from zero (e.g. KEDA with idleReplicaCount).</p>
</td>
</tr>
</tbody>
</table>

## `RoleScalingMode`     {#disaggregatedset-x-k8s-io-v1-RoleScalingMode}
    
(Alias of `string`)

**Appears in:**

- [RoleScaling](#disaggregatedset-x-k8s-io-v1-RoleScaling)


<p>RoleScalingMode selects the source of the desired replica count for a role.</p>




## `RoleStatus`     {#disaggregatedset-x-k8s-io-v1-RoleStatus}
    

**Appears in:**

- [DisaggregatedSetStatus](#disaggregatedset-x-k8s-io-v1-DisaggregatedSetStatus)


<p>RoleStatus defines the observed state of a single role.</p>


<table class="table">
<thead><tr><th width="30%">Field</th><th>Description</th></tr></thead>
<tbody>
    
  
<tr><td><code>name</code> <B>[Required]</B><br/>
<code>string</code>
</td>
<td>
   <p>Name is the name of the role (matches spec.roles[].name).</p>
</td>
</tr>
<tr><td><code>replicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>Replicas is the total number of replicas for this role.</p>
</td>
</tr>
<tr><td><code>readyReplicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>ReadyReplicas is the number of ready replicas for this role.</p>
</td>
</tr>
<tr><td><code>updatedReplicas</code><br/>
<code>int32</code>
</td>
<td>
   <p>UpdatedReplicas is the number of replicas updated to the latest revision.</p>
</td>
</tr>
</tbody>
</table>
  