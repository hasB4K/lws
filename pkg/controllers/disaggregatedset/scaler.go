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

package disaggregatedset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	disaggregatedsetutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

// ErrScalerNotReady is returned by getTargetReplicas when a role is
// configured with scaling.mode: External but no matching
// DisaggregatedSetRoleScaler (with a set spec.replicas) exists yet.
// The reconciler surfaces this as a WaitingForScaler condition on the
// DisaggregatedSet and holds the role's LeaderWorkerSet at its current
// replica count.
var ErrScalerNotReady = errors.New("waiting for scaler to be ready")

// Condition type set on the DisaggregatedSet when at least one
// External-mode role has no ready scaler.
const ConditionWaitingForScaler = "WaitingForScaler"

// scalerToDSKey maps a DisaggregatedSetRoleScaler event to a reconcile
// request for the DisaggregatedSet it targets.
func scalerToDSKey(_ context.Context, obj client.Object) []reconcile.Request {
	s, ok := obj.(*disaggregatedsetv1.DisaggregatedSetRoleScaler)
	if !ok {
		return nil
	}
	if s.Spec.TargetRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: s.Namespace,
			Name:      s.Spec.TargetRef.Name,
		},
	}}
}

// loadScalersForDS lists all DisaggregatedSetRoleScaler resources in the
// DisaggregatedSet's namespace, filters those whose targetRef.name matches
// this DisaggregatedSet, and returns a map keyed by role name.
//
// If more than one scaler targets the same role, both are marked with a
// Conflicting condition (in a subsequent status write) and neither is
// returned in the map — the reconciler treats the role as
// "waiting for scaler".
func (r *DisaggregatedSetReconciler) loadScalersForDS(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
) (map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler, []*disaggregatedsetv1.DisaggregatedSetRoleScaler, error) {
	list := &disaggregatedsetv1.DisaggregatedSetRoleScalerList{}
	if err := r.List(ctx, list, client.InNamespace(ds.Namespace)); err != nil {
		return nil, nil, fmt.Errorf("failed to list DisaggregatedSetRoleScalers: %w", err)
	}

	scalers := make(map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler)
	conflicting := make(map[string]bool)
	all := make([]*disaggregatedsetv1.DisaggregatedSetRoleScaler, 0, len(list.Items))

	for i := range list.Items {
		s := &list.Items[i]
		if s.Spec.TargetRef.Name != ds.Name {
			continue
		}
		all = append(all, s)
		role := s.Spec.TargetRef.Role
		if _, exists := scalers[role]; exists {
			conflicting[role] = true
			continue
		}
		scalers[role] = s
	}

	// Drop any role that has a conflict so callers treat it as "not ready".
	for role := range conflicting {
		delete(scalers, role)
	}

	return scalers, all, nil
}

// getTargetReplicas returns the desired replica count for a role.
//
// When the role's Scaling.Mode is External, the value is read from a
// matching DisaggregatedSetRoleScaler. If no scaler is present (or the
// scaler has no spec.replicas set), ErrScalerNotReady is returned.
//
// For Static mode (or when Scaling is unset), the value is read from the
// inline spec.replicas, defaulting to 1 when unset.
func getTargetReplicas(
	ds *disaggregatedsetv1.DisaggregatedSet,
	roleName string,
	scalers map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) (int, error) {
	role := findRoleSpec(ds, roleName)
	if role == nil {
		return 1, nil
	}

	if role.Scaling != nil && role.Scaling.Mode == disaggregatedsetv1.RoleScalingExternal {
		s, ok := scalers[roleName]
		if !ok || s.Spec.Replicas == nil {
			return 0, ErrScalerNotReady
		}
		return int(*s.Spec.Replicas), nil
	}

	if role.Spec.Replicas == nil {
		return 1, nil
	}
	return int(*role.Spec.Replicas), nil
}

// findRoleSpec returns the DisaggregatedRoleSpec whose Name matches
// roleName, or nil if none does.
func findRoleSpec(ds *disaggregatedsetv1.DisaggregatedSet, roleName string) *disaggregatedsetv1.DisaggregatedRoleSpec {
	for i := range ds.Spec.Roles {
		if ds.Spec.Roles[i].Name == roleName {
			return &ds.Spec.Roles[i]
		}
	}
	return nil
}

// roleIsExternal reports whether a given role opts into external scaling.
func roleIsExternal(ds *disaggregatedsetv1.DisaggregatedSet, roleName string) bool {
	role := findRoleSpec(ds, roleName)
	return role != nil && role.Scaling != nil && role.Scaling.Mode == disaggregatedsetv1.RoleScalingExternal
}

// ScalerName returns the deterministic name of the auto-created scaler for
// a given (DisaggregatedSet, role) pair: "<ds>-<role>".
//
// Kubernetes limits object names to 253 characters. Since roles are bounded
// at 63 chars (validated on DisaggregatedRoleSpec.Name) and DS names are
// bounded at 253, this can technically exceed the limit; we truncate the
// DS-name portion and append a short hash to keep the result unique.
func ScalerName(dsName, roleName string) string {
	const maxLen = 253
	full := fmt.Sprintf("%s-%s", dsName, roleName)
	if len(full) <= maxLen {
		return full
	}
	// Reserve room for the role, a separator, and an 8-char hash suffix.
	reserved := len(roleName) + 1 + 1 + 8
	head := dsName
	if len(head) > maxLen-reserved {
		head = head[:maxLen-reserved]
	}
	sum := sha256.Sum256([]byte(dsName))
	return fmt.Sprintf("%s-%s-%s", head, roleName, hex.EncodeToString(sum[:4]))
}

// ensureScalerForRole creates the DisaggregatedSetRoleScaler for an
// External-mode role if it does not already exist. The scaler carries a
// controller owner reference back to the DisaggregatedSet so it is garbage
// collected when the DisaggregatedSet is deleted (Deployment→ReplicaSet
// pattern).
//
// If the scaler already exists (created by a previous reconcile), the
// function is a no-op — its spec.replicas is subsequently driven by
// external autoscalers via the /scale subresource, not by the DS controller.
func (r *DisaggregatedSetReconciler) ensureScalerForRole(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	role *disaggregatedsetv1.DisaggregatedRoleSpec,
) (*disaggregatedsetv1.DisaggregatedSetRoleScaler, error) {
	name := ScalerName(ds.Name, role.Name)
	scaler := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	err := r.Get(ctx, types.NamespacedName{Namespace: ds.Namespace, Name: name}, scaler)
	if err == nil {
		return scaler, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get scaler %s: %w", name, err)
	}

	// Not found — create it.
	desired := &disaggregatedsetv1.DisaggregatedSetRoleScaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ds.Namespace,
			Labels: map[string]string{
				disaggregatedsetv1.SetNameLabelKey: ds.Name,
				disaggregatedsetv1.RoleLabelKey:    role.Name,
			},
		},
		Spec: disaggregatedsetv1.DisaggregatedSetRoleScalerSpec{
			TargetRef: disaggregatedsetv1.DisaggregatedSetRoleRef{
				Name: ds.Name,
				Role: role.Name,
			},
		},
	}
	if role.Scaling != nil && role.Scaling.InitialReplicas != nil {
		v := *role.Scaling.InitialReplicas
		desired.Spec.Replicas = &v
	}
	if err := controllerutil.SetControllerReference(ds, desired, r.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference on scaler %s: %w", name, err)
	}
	if err := r.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Lost a race — refetch and return the existing object.
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: ds.Namespace, Name: name}, desired); getErr == nil {
				return desired, nil
			}
		}
		return nil, fmt.Errorf("failed to create scaler %s: %w", name, err)
	}
	return desired, nil
}

// deleteObsoleteScalers removes scalers whose target role is no longer
// External on the DisaggregatedSet (either because the role's mode flipped
// back to Static, or because the role was removed entirely). Only scalers
// with the deterministic auto-create name and a controller ownerRef to
// this DS are deleted; user-created scalers (if any) are left alone.
func (r *DisaggregatedSetReconciler) deleteObsoleteScalers(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	existing []*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) error {
	external := make(map[string]bool)
	for _, role := range ds.Spec.Roles {
		if role.Scaling != nil && role.Scaling.Mode == disaggregatedsetv1.RoleScalingExternal {
			external[role.Name] = true
		}
	}

	for _, s := range existing {
		role := s.Spec.TargetRef.Role
		if external[role] {
			continue
		}
		if s.Name != ScalerName(ds.Name, role) {
			continue // not our auto-created scaler
		}
		if !isControlledBy(s, ds) {
			continue // user's scaler; leave it alone
		}
		if err := r.Delete(ctx, s); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete obsolete scaler %s: %w", s.Name, err)
		}
	}
	return nil
}

// isControlledBy reports whether obj has a controller owner reference to owner.
func isControlledBy(obj metav1.Object, owner metav1.Object) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner.GetUID() && ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

// writeBackScalerStatus updates each scaler's status.replicas,
// status.selector, and conditions to reflect the current observed state
// of the role's new-revision LeaderWorkerSet.
//
// The selector must be rewritten on every rolling update because the LWS
// name (revision hash) changes; that rewrite is what lets an HPA/KEDA
// pointed at the scaler continue to compute per-pod metrics correctly
// across revisions.
//
// Each scaler is fetched fresh before the status write to avoid stale
// resourceVersion conflicts with earlier writes in the same reconcile
// (e.g. ensureScalerOwnerRef).
func (r *DisaggregatedSetReconciler) writeBackScalerStatus(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	scalers []*disaggregatedsetv1.DisaggregatedSetRoleScaler,
	newRevision *disaggregatedsetutils.RevisionRoles,
) error {
	log := logf.FromContext(ctx)

	for _, s := range scalers {
		role := s.Spec.TargetRef.Role

		fresh := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: s.Name}, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("failed to refresh scaler %s: %w", s.Name, err)
		}

		// Determine the target state for this scaler.
		targetMissing := !roleIsExternal(ds, role)
		var observedReplicas int32
		var selector string
		if !targetMissing && newRevision != nil {
			if lws := newRevision.Roles[role]; lws != nil {
				observedReplicas = getLWSReplicas(lws)
				selector = fmt.Sprintf("%s=%s", leaderworkersetv1.SetNameLabelKey, lws.Name)
			}
		}

		desired := fresh.Status.DeepCopy()
		if desired == nil {
			desired = &disaggregatedsetv1.DisaggregatedSetRoleScalerStatus{}
		}
		desired.Replicas = observedReplicas
		desired.Selector = selector
		desired.ObservedGeneration = fresh.Generation

		if targetMissing {
			setStatusCondition(&desired.Conditions, metav1.Condition{
				Type:    disaggregatedsetv1.ScalerTargetMissing,
				Status:  metav1.ConditionTrue,
				Reason:  "RoleNotExternal",
				Message: fmt.Sprintf("Role %q on DisaggregatedSet %q does not opt into external scaling (scaling.mode: External)", role, ds.Name),
			})
			setStatusCondition(&desired.Conditions, metav1.Condition{
				Type:    disaggregatedsetv1.ScalerReady,
				Status:  metav1.ConditionFalse,
				Reason:  "TargetMissing",
				Message: "Scaler target role is not opted into external scaling",
			})
		} else {
			setStatusCondition(&desired.Conditions, metav1.Condition{
				Type:    disaggregatedsetv1.ScalerTargetMissing,
				Status:  metav1.ConditionFalse,
				Reason:  "Resolved",
				Message: fmt.Sprintf("Resolved role %q on DisaggregatedSet %q", role, ds.Name),
			})
			setStatusCondition(&desired.Conditions, metav1.Condition{
				Type:    disaggregatedsetv1.ScalerReady,
				Status:  metav1.ConditionTrue,
				Reason:  "Reconciled",
				Message: "Scaler status reflects the current new-revision LeaderWorkerSet",
			})
		}

		if scalerStatusEqual(&fresh.Status, desired) {
			continue
		}

		fresh.Status = *desired
		if err := r.Status().Update(ctx, fresh); err != nil {
			log.Error(err, "failed to update scaler status", "scaler", s.Name)
			return err
		}
	}
	return nil
}

// markScalerConflicting sets the Conflicting condition on scalers that
// duplicate another scaler's targetRef. Called when loadScalersForDS
// detects a collision.
func (r *DisaggregatedSetReconciler) markScalerConflicting(
	ctx context.Context,
	scaler *disaggregatedsetv1.DisaggregatedSetRoleScaler,
	otherName string,
) error {
	desired := scaler.Status.DeepCopy()
	if desired == nil {
		desired = &disaggregatedsetv1.DisaggregatedSetRoleScalerStatus{}
	}
	desired.ObservedGeneration = scaler.Generation
	setStatusCondition(&desired.Conditions, metav1.Condition{
		Type:    disaggregatedsetv1.ScalerConflicting,
		Status:  metav1.ConditionTrue,
		Reason:  "DuplicateTargetRef",
		Message: fmt.Sprintf("Another DisaggregatedSetRoleScaler %q targets the same (DisaggregatedSet, role) pair", otherName),
	})
	setStatusCondition(&desired.Conditions, metav1.Condition{
		Type:    disaggregatedsetv1.ScalerReady,
		Status:  metav1.ConditionFalse,
		Reason:  "Conflicting",
		Message: "Multiple scalers target the same (DisaggregatedSet, role) pair; none is honored",
	})
	if scalerStatusEqual(&scaler.Status, desired) {
		return nil
	}
	updated := scaler.DeepCopy()
	updated.Status = *desired
	return r.Status().Update(ctx, updated)
}

// setDSWaitingForScalerCondition sets or clears the WaitingForScaler
// condition on the DisaggregatedSet.
func setDSWaitingForScalerCondition(ds *disaggregatedsetv1.DisaggregatedSet, waiting bool, message string) {
	status := metav1.ConditionFalse
	reason := "AllScalersReady"
	if waiting {
		status = metav1.ConditionTrue
		reason = "MissingScaler"
	}
	if message == "" {
		message = "All External-mode roles have a ready DisaggregatedSetRoleScaler"
	}
	setStatusCondition(&ds.Status.Conditions, metav1.Condition{
		Type:    ConditionWaitingForScaler,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// setStatusCondition writes or updates a condition on the given slice.
// Unlike meta.SetStatusCondition, it preserves the LastTransitionTime
// when status doesn't change so status updates stay idempotent within
// the same second.
func setStatusCondition(conditions *[]metav1.Condition, cond metav1.Condition) {
	if cond.LastTransitionTime.IsZero() {
		cond.LastTransitionTime = metav1.Now()
	}
	for i, existing := range *conditions {
		if existing.Type != cond.Type {
			continue
		}
		if existing.Status == cond.Status {
			// Preserve LastTransitionTime when status is unchanged.
			cond.LastTransitionTime = existing.LastTransitionTime
		}
		(*conditions)[i] = cond
		return
	}
	*conditions = append(*conditions, cond)
}

// scalerStatusEqual compares two scaler statuses for equality, ignoring
// per-condition LastTransitionTime differences that don't reflect a
// meaningful state change.
func scalerStatusEqual(a, b *disaggregatedsetv1.DisaggregatedSetRoleScalerStatus) bool {
	if a.Replicas != b.Replicas ||
		a.Selector != b.Selector ||
		a.ObservedGeneration != b.ObservedGeneration ||
		len(a.Conditions) != len(b.Conditions) {
		return false
	}
	byType := make(map[string]metav1.Condition, len(a.Conditions))
	for _, c := range a.Conditions {
		byType[c.Type] = c
	}
	for _, c := range b.Conditions {
		other, ok := byType[c.Type]
		if !ok {
			return false
		}
		if other.Status != c.Status || other.Reason != c.Reason || other.Message != c.Message {
			return false
		}
	}
	return true
}
