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

package v1

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/conversion"

	disaggv1alpha1 "sigs.k8s.io/disaggregatedset/api/v1alpha1"
)

// v1 is the spoke; v1alpha1 is the hub (storage version).
// Conversion is purely mechanical: v1 wraps LWS spec fields under role.spec
// and inlines metadata via ObjectMeta; v1alpha1 inlines LWS spec fields at the
// role level and uses a pointer *metav1.ObjectMeta for role.Metadata.
var (
	_ conversion.Convertible = &DisaggregatedSet{}
)

// ConvertTo converts this DisaggregatedSet (v1) to the Hub (v1alpha1) version.
//
// TypeMeta is intentionally NOT copied from src: each version's Convert
// method is responsible only for populating the RECEIVER's spec/metadata,
// and the API server enforces that the returned object's TypeMeta matches
// the version the conversion is targeting (v1alpha1 here). Copying src's
// TypeMeta would leave "v1" leaking into a v1alpha1 response.
func (src *DisaggregatedSet) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*disaggv1alpha1.DisaggregatedSet)
	dst.ObjectMeta = *src.ObjectMeta.DeepCopy()
	dst.Spec.Roles = convertRolesToV1Alpha1(src.Spec.Roles)
	dst.Status = convertStatusToV1Alpha1(src.Status)
	return nil
}

// ConvertFrom converts the Hub (v1alpha1) version to this DisaggregatedSet (v1).
//
// TypeMeta is intentionally NOT copied from src (see ConvertTo).
func (dst *DisaggregatedSet) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*disaggv1alpha1.DisaggregatedSet)
	dst.ObjectMeta = *src.ObjectMeta.DeepCopy()
	dst.Spec.Roles = convertRolesFromV1Alpha1(src.Spec.Roles)
	dst.Status = convertStatusFromV1Alpha1(src.Status)
	return nil
}

func convertRolesToV1Alpha1(in []DisaggregatedRoleSpec) []disaggv1alpha1.DisaggregatedRoleSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]disaggv1alpha1.DisaggregatedRoleSpec, len(in))
	for i, r := range in {
		out[i] = disaggv1alpha1.DisaggregatedRoleSpec{
			Name:                r.Name,
			LeaderWorkerSetSpec: *r.Spec.DeepCopy(),
		}
		// v1's inline ObjectMeta becomes v1alpha1's *ObjectMeta pointer.
		// Only populate the pointer if the source metadata is non-empty; this
		// avoids gratuitous {} objects and keeps roundtrip byte-equivalent.
		if !isObjectMetaEmpty(&r.ObjectMeta) {
			meta := *r.ObjectMeta.DeepCopy()
			out[i].Metadata = &meta
		}
	}
	return out
}

func convertRolesFromV1Alpha1(in []disaggv1alpha1.DisaggregatedRoleSpec) []DisaggregatedRoleSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]DisaggregatedRoleSpec, len(in))
	for i, r := range in {
		out[i] = DisaggregatedRoleSpec{
			Name: r.Name,
			Spec: *r.LeaderWorkerSetSpec.DeepCopy(),
		}
		if r.Metadata != nil {
			out[i].ObjectMeta = *r.Metadata.DeepCopy()
		}
	}
	return out
}

func convertStatusToV1Alpha1(in DisaggregatedSetStatus) disaggv1alpha1.DisaggregatedSetStatus {
	out := disaggv1alpha1.DisaggregatedSetStatus{}
	if len(in.RoleStatuses) > 0 {
		out.RoleStatuses = make([]disaggv1alpha1.RoleStatus, len(in.RoleStatuses))
		for i, s := range in.RoleStatuses {
			out.RoleStatuses[i] = disaggv1alpha1.RoleStatus(s)
		}
	}
	if len(in.Conditions) > 0 {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i, c := range in.Conditions {
			out.Conditions[i] = *c.DeepCopy()
		}
	}
	return out
}

func convertStatusFromV1Alpha1(in disaggv1alpha1.DisaggregatedSetStatus) DisaggregatedSetStatus {
	out := DisaggregatedSetStatus{}
	if len(in.RoleStatuses) > 0 {
		out.RoleStatuses = make([]RoleStatus, len(in.RoleStatuses))
		for i, s := range in.RoleStatuses {
			out.RoleStatuses[i] = RoleStatus(s)
		}
	}
	if len(in.Conditions) > 0 {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i, c := range in.Conditions {
			out.Conditions[i] = *c.DeepCopy()
		}
	}
	return out
}

// isObjectMetaEmpty reports whether m is the zero value of ObjectMeta.
// Guards against emitting a non-nil pointer for a semantically empty metadata.
func isObjectMetaEmpty(m *metav1.ObjectMeta) bool {
	if m == nil {
		return true
	}
	return reflect.DeepEqual(*m, metav1.ObjectMeta{})
}
