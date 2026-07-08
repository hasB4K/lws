/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const (
	// SetNameLabelKey records the DisaggregatedSet name that resources belong to.
	// Applied to LWS and Service objects in the same namespace as the DisaggregatedSet.
	SetNameLabelKey string = "disaggregatedset.x-k8s.io/name"

	// RoleLabelKey records which role the resource belongs to (e.g. "prefill", "decode").
	// Applied to LWS and Service objects in the same namespace as the DisaggregatedSet.
	RoleLabelKey string = "disaggregatedset.x-k8s.io/role"

	// RevisionLabelKey records the revision hash for the resource.
	// Applied to LWS and Service objects in the same namespace as the DisaggregatedSet.
	RevisionLabelKey string = "disaggregatedset.x-k8s.io/revision"

	// InitialReplicasAnnotationKey stores the initial replica count at rollout start.
	InitialReplicasAnnotationKey string = "disaggregatedset.x-k8s.io/initial-replicas"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RoleScalingMode selects the source of the desired replica count for a role.
// +kubebuilder:validation:Enum=Static;External
type RoleScalingMode string

const (
	// RoleScalingStatic uses the inline spec.replicas value on the role.
	// This is the default and preserves pre-scaler behavior.
	RoleScalingStatic RoleScalingMode = "Static"

	// RoleScalingExternal delegates the replica count to an external
	// autoscaler (HPA, KEDA, or any /scale-aware controller) via a
	// DisaggregatedSetRoleScaler that the DisaggregatedSet controller
	// auto-creates for the role. The inline spec.replicas on the role is
	// ignored and must not be set to a non-zero value.
	RoleScalingExternal RoleScalingMode = "External"
)

// RoleScaling configures how the desired replica count for a role is
// determined.
type RoleScaling struct {
	// Mode selects the source of the replica count:
	//   - Static (default): read from the inline spec.replicas value.
	//   - External: the DisaggregatedSet controller auto-creates a
	//     DisaggregatedSetRoleScaler named "<disaggregatedset>-<role>"
	//     whose /scale subresource an external autoscaler drives.
	// +kubebuilder:default=Static
	// +optional
	Mode RoleScalingMode `json:"mode,omitempty"`

	// InitialReplicas seeds the auto-created DisaggregatedSetRoleScaler's
	// spec.replicas so the role has a cold-start replica count before an
	// external autoscaler makes its first write. Applies only when Mode
	// is External. If unset, the role holds at 0 replicas until an
	// autoscaler writes a value; leaving it unset only works with
	// autoscalers that can scale from zero (e.g. KEDA with idleReplicaCount).
	// +optional
	// +kubebuilder:validation:Minimum=0
	InitialReplicas *int32 `json:"initialReplicas,omitempty"`
}

// DisaggregatedRoleSpec defines the configuration for a disaggregated role.
// This structure embeds LeaderWorkerSetTemplateSpec from sigs.k8s.io/lws, with validation
// to reject unsupported fields (RolloutStrategy.Type must be RollingUpdate,
// RolloutStrategy.RollingUpdateConfiguration.Partition must not be set).
// +kubebuilder:validation:XValidation:rule="!has(self.scaling) || self.scaling.mode != 'External' || !has(self.spec.replicas) || self.spec.replicas == 0",message="spec.replicas must be unset or zero when scaling.mode is External"
type DisaggregatedRoleSpec struct {
	// Name is the unique identifier for this role.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	Name string `json:"name"`

	// Scaling configures how the desired replica count for this role is
	// determined. Omit for the default Static mode (inline spec.replicas).
	// +optional
	Scaling *RoleScaling `json:"scaling,omitempty"`

	// LeaderWorkerSetTemplateSpec defines the LWS template for this role.
	// Note: Spec.RolloutStrategy.Type must be RollingUpdate (or empty) and
	// Spec.RolloutStrategy.RollingUpdateConfiguration.Partition must not be set.
	// DisaggregatedSet handles rollouts across roles.
	leaderworkerset.LeaderWorkerSetTemplateSpec `json:",inline"`
}

// DisaggregatedSetSpec defines the desired state of DisaggregatedSet.
// The all-or-nothing replica rule applies only to roles that do not use
// external scaling; roles with scaling.mode == External source their
// replica count from a DisaggregatedSetRoleScaler and are exempt.
// +kubebuilder:validation:XValidation:rule="self.roles.filter(r, !has(r.scaling) || r.scaling.mode != 'External').all(r, !has(r.spec.replicas) || r.spec.replicas == 0) || self.roles.filter(r, !has(r.scaling) || r.scaling.mode != 'External').all(r, has(r.spec.replicas) && r.spec.replicas > 0)",message="replicas must be zero for all non-External roles or non-zero for all non-External roles"
type DisaggregatedSetSpec struct {
	// Roles defines the list of roles (at least 2 required).
	// Each role has a unique name and its own configuration.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=10
	// +required
	Roles []DisaggregatedRoleSpec `json:"roles"`
}

// RoleStatus defines the observed state of a single role.
type RoleStatus struct {
	// Name is the name of the role (matches spec.roles[].name).
	// +required
	Name string `json:"name"`

	// Replicas is the total number of replicas for this role.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of ready replicas for this role.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// UpdatedReplicas is the number of replicas updated to the latest revision.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`
}

// DisaggregatedSetStatus defines the observed state of DisaggregatedSet.
type DisaggregatedSetStatus struct {
	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// RoleStatuses contains the status for each role.
	// The order matches spec.roles.
	// +listType=map
	// +listMapKey=name
	// +optional
	RoleStatuses []RoleStatus `json:"roleStatuses,omitempty"`

	// conditions represent the current state of the DisaggregatedSet resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DisaggregatedSet is the Schema for the disaggregatedsets API
type DisaggregatedSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DisaggregatedSet
	// +required
	Spec DisaggregatedSetSpec `json:"spec"`

	// status defines the observed state of DisaggregatedSet
	// +optional
	Status DisaggregatedSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DisaggregatedSetList contains a list of DisaggregatedSet
type DisaggregatedSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DisaggregatedSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DisaggregatedSet{}, &DisaggregatedSetList{})
}
