/*
Copyright 2026.

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

package v1_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"

	disaggv1 "sigs.k8s.io/disaggregatedset/api/v1"
	disaggv1alpha1 "sigs.k8s.io/disaggregatedset/api/v1alpha1"
)

// v1 and v1alpha1 differ in shape: v1 nests LWS spec fields under role.spec and
// role.metadata (inlined); v1alpha1 inlines LWS spec fields directly on the role
// and uses a pointer *metav1.ObjectMeta for role.Metadata. Conversion moves
// fields between these shapes with no semantic change.

func newLWSSpec(replicas int32, image string) leaderworkerset.LeaderWorkerSetSpec {
	return leaderworkerset.LeaderWorkerSetSpec{
		Replicas: ptr.To(replicas),
		LeaderWorkerTemplate: leaderworkerset.LeaderWorkerTemplate{
			Size: ptr.To[int32](1),
			WorkerTemplate: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "main", Image: image}},
				},
			},
		},
	}
}

func TestConvertTo_SingleRole(t *testing.T) {
	src := &disaggv1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: disaggv1.DisaggregatedSetSpec{
			Roles: []disaggv1.DisaggregatedRoleSpec{{
				Name: "prefill",
				Spec: newLWSSpec(2, "img"),
			}},
		},
	}

	dst := &disaggv1alpha1.DisaggregatedSet{}
	if err := src.ConvertTo(dst); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}

	if got, want := dst.Name, "s"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got := len(dst.Spec.Roles); got != 1 {
		t.Fatalf("Roles len = %d, want 1", got)
	}
	if got, want := dst.Spec.Roles[0].Name, "prefill"; got != want {
		t.Errorf("role.Name = %q, want %q", got, want)
	}
	if got, want := *dst.Spec.Roles[0].Replicas, int32(2); got != want {
		t.Errorf("role.Replicas = %d, want %d (should be flat, not under .spec)", got, want)
	}
	if dst.Spec.Roles[0].Metadata != nil {
		t.Errorf("role.Metadata should be nil (source had empty metadata), got %#v", dst.Spec.Roles[0].Metadata)
	}
}

func TestConvertFrom_SingleRole(t *testing.T) {
	src := &disaggv1alpha1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Spec: disaggv1alpha1.DisaggregatedSetSpec{
			Roles: []disaggv1alpha1.DisaggregatedRoleSpec{{
				Name:                "prefill",
				LeaderWorkerSetSpec: newLWSSpec(3, "img"),
			}},
		},
	}

	dst := &disaggv1.DisaggregatedSet{}
	if err := dst.ConvertFrom(src); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}

	if got, want := dst.Name, "s"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got := len(dst.Spec.Roles); got != 1 {
		t.Fatalf("Roles len = %d, want 1", got)
	}
	if got, want := *dst.Spec.Roles[0].Spec.Replicas, int32(3); got != want {
		t.Errorf("role.Spec.Replicas = %d, want %d (should be nested under .spec)", got, want)
	}
}

func TestRoundtrip_V1AlphaHub_MultiRole(t *testing.T) {
	// Start at v1alpha1 (the storage version / hub).
	original := &disaggv1alpha1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "ns", Labels: map[string]string{"a": "b"}},
		Spec: disaggv1alpha1.DisaggregatedSetSpec{
			Roles: []disaggv1alpha1.DisaggregatedRoleSpec{
				{
					Name:                "prefill",
					LeaderWorkerSetSpec: newLWSSpec(2, "prefill:img"),
					Metadata: &metav1.ObjectMeta{
						Labels:      map[string]string{"kueue.x-k8s.io/queue-name": "gpu"},
						Annotations: map[string]string{"x": "y"},
					},
				},
				{
					Name:                "decode",
					LeaderWorkerSetSpec: newLWSSpec(3, "decode:img"),
				},
			},
		},
		Status: disaggv1alpha1.DisaggregatedSetStatus{
			RoleStatuses: []disaggv1alpha1.RoleStatus{
				{Name: "prefill", Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2},
				{Name: "decode", Replicas: 3, ReadyReplicas: 1, UpdatedReplicas: 3},
			},
			Conditions: []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue, Reason: "Ready", Message: "ok"}},
		},
	}

	spoke := &disaggv1.DisaggregatedSet{}
	if err := spoke.ConvertFrom(original); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}
	roundtripped := &disaggv1alpha1.DisaggregatedSet{}
	if err := spoke.ConvertTo(roundtripped); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}

	if diff := cmp.Diff(original, roundtripped); diff != "" {
		t.Fatalf("roundtrip mismatch (-original +roundtripped):\n%s", diff)
	}
}

func TestRoundtrip_V1Spoke_MultiRole(t *testing.T) {
	// Start at v1 (spoke).
	original := &disaggv1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "ns"},
		Spec: disaggv1.DisaggregatedSetSpec{
			Roles: []disaggv1.DisaggregatedRoleSpec{
				{
					Name: "prefill",
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"kueue.x-k8s.io/queue-name": "gpu"},
					},
					Spec: newLWSSpec(2, "prefill:img"),
				},
				{Name: "decode", Spec: newLWSSpec(3, "decode:img")},
			},
		},
	}

	hub := &disaggv1alpha1.DisaggregatedSet{}
	if err := original.ConvertTo(hub); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}
	roundtripped := &disaggv1.DisaggregatedSet{}
	if err := roundtripped.ConvertFrom(hub); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}

	if diff := cmp.Diff(original, roundtripped); diff != "" {
		t.Fatalf("roundtrip mismatch (-original +roundtripped):\n%s", diff)
	}
}

func TestConvertTo_MetadataEmptyStaysEmpty(t *testing.T) {
	// v1 with all-zero ObjectMeta on a role should NOT produce a non-nil
	// v1alpha1 Metadata pointer (that would be a semantic no-op that still
	// churns object bytes and could confuse consumers).
	src := &disaggv1.DisaggregatedSet{
		Spec: disaggv1.DisaggregatedSetSpec{
			Roles: []disaggv1.DisaggregatedRoleSpec{
				{Name: "a", Spec: newLWSSpec(1, "img")},
				{Name: "b", Spec: newLWSSpec(1, "img")},
			},
		},
	}
	dst := &disaggv1alpha1.DisaggregatedSet{}
	if err := src.ConvertTo(dst); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}
	for i, r := range dst.Spec.Roles {
		if r.Metadata != nil {
			t.Errorf("role[%d].Metadata should be nil, got %#v", i, r.Metadata)
		}
	}
}

func TestConvertFrom_MetadataPointerRoundtripsAsInlineMeta(t *testing.T) {
	// A v1alpha1 role with Metadata should surface as v1 role's inline ObjectMeta.
	src := &disaggv1alpha1.DisaggregatedSet{
		Spec: disaggv1alpha1.DisaggregatedSetSpec{
			Roles: []disaggv1alpha1.DisaggregatedRoleSpec{{
				Name:                "prefill",
				LeaderWorkerSetSpec: newLWSSpec(1, "img"),
				Metadata: &metav1.ObjectMeta{
					Labels:      map[string]string{"leaderworkerset.sigs.k8s.io/exclusive-topology": "gke-topology-block"},
					Annotations: map[string]string{"stub": "yes"},
				},
			}},
		},
	}
	dst := &disaggv1.DisaggregatedSet{}
	if err := dst.ConvertFrom(src); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}
	got := dst.Spec.Roles[0].ObjectMeta
	if diff := cmp.Diff(*src.Spec.Roles[0].Metadata, got); diff != "" {
		t.Errorf("metadata mismatch (-want +got):\n%s", diff)
	}
}

func TestConvert_EmptyRoles(t *testing.T) {
	// Edge case: no roles. Conversion should not panic and should preserve emptiness.
	src := &disaggv1alpha1.DisaggregatedSet{ObjectMeta: metav1.ObjectMeta{Name: "e"}}
	dst := &disaggv1.DisaggregatedSet{}
	if err := dst.ConvertFrom(src); err != nil {
		t.Fatalf("ConvertFrom: %v", err)
	}
	if got := len(dst.Spec.Roles); got != 0 {
		t.Errorf("Roles len = %d, want 0", got)
	}
	back := &disaggv1alpha1.DisaggregatedSet{}
	if err := dst.ConvertTo(back); err != nil {
		t.Fatalf("ConvertTo: %v", err)
	}
	if diff := cmp.Diff(src, back); diff != "" {
		t.Errorf("empty-roles roundtrip mismatch (-want +got):\n%s", diff)
	}
}
