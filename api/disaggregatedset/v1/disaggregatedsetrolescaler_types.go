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
)

// Condition types for DisaggregatedSetRoleScaler.
const (
	// ScalerReady indicates the scaler has resolved a matching DisaggregatedSet
	// and role, and its status fields are up to date with the observed workload.
	ScalerReady string = "Ready"

	// ScalerTargetMissing indicates the referenced DisaggregatedSet or role
	// cannot be resolved (either it does not exist, or the role is not opted
	// into external scaling via scaling.mode: External).
	ScalerTargetMissing string = "TargetMissing"

	// ScalerConflicting indicates another DisaggregatedSetRoleScaler in the
	// same namespace already targets the same (DisaggregatedSet, role) pair.
	// Neither scaler is honored while both exist.
	ScalerConflicting string = "Conflicting"
)

// DisaggregatedSetRoleRef selects a specific role of a DisaggregatedSet in
// the same namespace as the scaler.
type DisaggregatedSetRoleRef struct {
	// Name is the DisaggregatedSet name in the same namespace as the scaler.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// Role is the role name within the referenced DisaggregatedSet
	// (matches spec.roles[].name).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	Role string `json:"role"`
}

// DisaggregatedSetRoleScalerSpec defines the desired state of a
// DisaggregatedSetRoleScaler.
type DisaggregatedSetRoleScalerSpec struct {
	// TargetRef selects the DisaggregatedSet and role this scaler drives.
	// The referenced DisaggregatedSet must live in the same namespace as
	// this scaler.
	// +required
	TargetRef DisaggregatedSetRoleRef `json:"targetRef"`

	// Replicas is the desired replica count for the referenced role.
	// Set by the /scale subresource (e.g., by an HPA or KEDA ScaledObject).
	// Read by the DisaggregatedSet controller when the referenced role is
	// configured with scaling.mode: External.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`
}

// DisaggregatedSetRoleScalerStatus defines the observed state of a
// DisaggregatedSetRoleScaler.
type DisaggregatedSetRoleScalerStatus struct {
	// Replicas is the observed replica count of the role's current
	// new-revision LeaderWorkerSet. Reported to /scale readers.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Selector is a label selector, in string form, matching pods of the
	// role's current new-revision LeaderWorkerSet. Used by HorizontalPodAutoscaler
	// to compute per-pod metrics against the correct pod set. The
	// DisaggregatedSet controller rewrites this on every rolling update
	// because the LeaderWorkerSet name changes with the revision hash.
	// +optional
	Selector string `json:"selector,omitempty"`

	// ObservedGeneration is the .metadata.generation the status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current state of the scaler.
	//
	// Standard condition types include:
	// - "Ready": the scaler has resolved its target and status is fresh
	// - "TargetMissing": the referenced DisaggregatedSet or role is not resolvable
	// - "Conflicting": another scaler in this namespace targets the same (set, role) pair
	//
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=dsrs
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.targetRef.role`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DisaggregatedSetRoleScaler exposes the /scale subresource for a single
// (DisaggregatedSet, role) pair, allowing external autoscalers (HPA, KEDA,
// or any /scale-aware controller) to drive that role's replica count
// independently of the rest of the DisaggregatedSet.
//
// The scaler name is stable across rolling updates of the target
// DisaggregatedSet, which lets an autoscaler pointed at the scaler continue
// to work when the underlying LeaderWorkerSet's revision-hashed name
// changes.
type DisaggregatedSetRoleScaler struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DisaggregatedSetRoleScaler
	// +required
	Spec DisaggregatedSetRoleScalerSpec `json:"spec"`

	// status defines the observed state of DisaggregatedSetRoleScaler
	// +optional
	Status DisaggregatedSetRoleScalerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DisaggregatedSetRoleScalerList contains a list of DisaggregatedSetRoleScaler
type DisaggregatedSetRoleScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DisaggregatedSetRoleScaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DisaggregatedSetRoleScaler{}, &DisaggregatedSetRoleScalerList{})
}
