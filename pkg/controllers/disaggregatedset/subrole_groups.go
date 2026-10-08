/*
Copyright 2026 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
)

const (
	subRoleReplicasAnnotation = "disaggregatedset.x-k8s.io/subrole-replicas"
	subRolePlanAnnotation     = "disaggregatedset.x-k8s.io/subrole-scale-plan"
)

// A plan survives partial Pod patches, controller restarts and target changes.
// Until it completes no new planner decision spends the same availability.
// Counts is the accepted logical Spec. The native StatefulSet chooses the
// physical retained prefix; ReadyFloor constrains its coherent assignment.
type subRolePlan struct {
	Armed      bool           `json:"armed,omitempty"`
	Counts     map[string]int `json:"counts"`
	ReadyFloor map[string]int `json:"readyFloor"`
}

func subRoleCounts(lws *leaderv1.LeaderWorkerSet, annotation string) (map[string]int, error) {
	counts := map[string]int{}
	if value := lws.Annotations[annotation]; value != "" {
		if err := json.Unmarshal([]byte(value), &counts); err != nil {
			return nil, fmt.Errorf("LWS %s has invalid %s: %w", lws.Name, annotation, err)
		}
	}
	if err := validateSubRoleCounts(counts); err != nil {
		return nil, fmt.Errorf("LWS %s has invalid %s: %w", lws.Name, annotation, err)
	}
	return counts, nil
}

func validateSubRoleCounts(counts map[string]int) error {
	total := 0
	for name, count := range counts {
		if count < 0 || count > math.MaxInt32-total {
			return fmt.Errorf("replica count for %q is outside the physical LWS range", name)
		}
		total += count
	}
	return nil
}

func countSubRoles(counts map[string]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

func setSubRoleJSON(lws *leaderv1.LeaderWorkerSet, key string, value any) {
	if lws.Annotations == nil {
		lws.Annotations = map[string]string{}
	}
	data, _ := json.Marshal(value) // Only string/int maps and subRolePlan are serialized.
	lws.Annotations[key] = string(data)
}

// fitSubRoles distributes a physical prefix of the final vector deterministically.
// It is used only to initialize state; normal assignment preserves existing labels.
func fitSubRoles(final map[string]int, total int) map[string]int {
	result := map[string]int{}
	keys := slices.Sorted(maps.Keys(final))
	remaining := total
	for _, key := range keys {
		result[key] = min(final[key], remaining)
		remaining -= result[key]
	}
	if remaining > 0 && len(keys) > 0 {
		result[keys[0]] += remaining
	}
	return result
}

// Native counters acknowledge observation, never supply Ready credit. A
// finalizer-retained failed Pod still occupies its ordinal until it disappears.
// Health is not a completion condition: native replacement remains independent.
func subRoleNativeSettled(s *replicagroups.Snapshot) bool {
	if s == nil || s.LWS == nil || !s.LWS.DeletionTimestamp.IsZero() {
		return false
	}
	replicas := ptr.Deref(s.LWS.Spec.Replicas, 1)
	if s.LWS.Status.ObservedGeneration < s.LWS.Generation {
		return false
	}
	sts := s.LeaderStatefulSet
	return sts != nil && sts.DeletionTimestamp.IsZero() && ptr.Deref(sts.Spec.Replicas, 1) == replicas &&
		sts.Status.ObservedGeneration >= sts.Generation && sts.Status.Replicas == replicas
}

func activeSubRoleGroups(snapshot *replicagroups.Snapshot) []replicagroups.Group {
	var groups []replicagroups.Group
	for _, group := range snapshot.Groups {
		if group.Leader.DeletionTimestamp.IsZero() && group.Leader.Status.Phase != corev1.PodFailed && group.Leader.Status.Phase != corev1.PodSucceeded {
			groups = append(groups, group)
		}
	}
	return groups
}

func allocatedSubRoles(groups []replicagroups.Group, counts map[string]int) map[types.UID]string {
	return allocateSubRoleReadiness(groups, counts, nil)
}

// Raw Ready is not necessarily retained capacity: a failed group may have
// exhausted recovery, or a worker update may not have been observed yet.
func retainedSubRoleEligible(group replicagroups.Group) bool {
	leader := group.Leader
	return group.Ready && !group.Terminating &&
		leader.Annotations[leaderv1.GroupRestartBudgetExhaustedAnnotationKey] != "true" &&
		(group.WorkerStatefulSet == nil || group.WorkerStatefulSet.Status.ObservedGeneration >= group.WorkerStatefulSet.Generation)
}

// Reserve each child's required Ready survivors before filling count-only
// slots. A low unready ordinal cannot silently replace a Ready high ordinal of
// another child merely because their assignment counts happen to match.
func allocateSubRoleReadiness(groups []replicagroups.Group, counts, readyFloor map[string]int) map[types.UID]string {
	remaining := maps.Clone(counts)
	assigned := map[types.UID]string{}
	keys := slices.Sorted(maps.Keys(remaining))
	// Reserve Ready floors first, then fill count-only slots. Within either
	// phase keep existing labels before assigning groups to another child.
	for _, readyOnly := range []bool{true, false} {
		need := remaining
		if readyOnly {
			need = maps.Clone(readyFloor)
		}
		for _, keepLabel := range []bool{true, false} {
			for _, group := range groups {
				if _, found := assigned[group.Leader.UID]; found || readyOnly && !retainedSubRoleEligible(group) {
					continue
				}
				choices := keys
				if keepLabel {
					choices = []string{group.Leader.Labels[disaggv1.SubRoleLabelKey]}
				}
				for _, name := range choices {
					if need[name] > 0 && remaining[name] > 0 {
						assigned[group.Leader.UID] = name
						remaining[name]--
						if readyOnly {
							need[name]--
						}
						break
					}
				}
			}
		}
		if readyOnly {
			for _, n := range need {
				if n > 0 {
					return nil
				}
			}
		}
	}
	return assigned
}

func newSubRolePlan(snapshot *replicagroups.Snapshot, counts map[string]int) *subRolePlan {
	current, _ := subRoleCounts(snapshot.LWS, subRoleReplicasAnnotation)
	floor := map[string]int{}
	for _, group := range snapshot.Groups {
		name, coherent := groupSubRole(group)
		if coherent && group.Ready && group.Leader.DeletionTimestamp.IsZero() {
			floor[name]++
		}
	}
	for name, ready := range floor {
		floor[name] = max(0, ready-max(0, current[name]-counts[name]))
	}
	plan := &subRolePlan{Counts: maps.Clone(counts), ReadyFloor: floor}
	if subRolePlanAssignments(snapshot, plan) == nil {
		return nil
	}
	return plan
}

// Only the native retained prefix can supply assignments. Occupied failed or
// terminating slots may receive count-only assignments, never Ready credit.
func subRolePlanAssignments(s *replicagroups.Snapshot, plan *subRolePlan) map[types.UID]string {
	target := countSubRoles(plan.Counts)
	var candidates []replicagroups.Group
	start := 0
	if sts := s.LeaderStatefulSet; sts != nil && sts.Spec.Ordinals != nil {
		start = int(sts.Spec.Ordinals.Start)
	}
	for _, group := range s.Groups {
		if group.Ordinal < start || group.Ordinal >= start+target {
			continue
		}
		candidates = append(candidates, group)
	}
	floor := plan.ReadyFloor
	if plan.Armed {
		floor = nil
	} // Failed survivors must be able to undergo native replacement.
	return allocateSubRoleReadiness(candidates, plan.Counts, floor)
}

func (m *LeaderWorkerSetManager) patchSubRoleLWS(ctx context.Context, observed *leaderv1.LeaderWorkerSet, mutate func(*leaderv1.LeaderWorkerSet)) error {
	desired := observed.DeepCopy()
	mutate(desired)
	return m.client.Patch(ctx, desired, client.MergeFromWithOptions(observed, client.MergeFromWithOptimisticLock{}))
}

// patchSubRoleGroup commits the leader label last. A partially patched group is
// not Ready for either child in the planner adapter. Every write is UID/RV-bound.
func (m *LeaderWorkerSetManager) patchSubRoleGroup(ctx context.Context, group replicagroups.Group, assignment string) (bool, error) {
	changed := false
	pods := append(slices.Clone(group.Pods[1:]), group.Leader)
	for _, observed := range pods {
		desired := observed.DeepCopy()
		if desired.Labels == nil {
			desired.Labels = map[string]string{}
		}
		if assignment == "" {
			delete(desired.Labels, disaggv1.SubRoleLabelKey)
		} else {
			desired.Labels[disaggv1.SubRoleLabelKey] = assignment
		}
		if maps.Equal(desired.Labels, observed.Labels) {
			continue
		}
		if err := m.client.Patch(ctx, desired, client.MergeFromWithOptions(observed, client.MergeFromWithOptimisticLock{})); err != nil {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

// reconcileSubRolePlan first prepares retained groups, then atomically publishes
// physical and logical Specs. Native StatefulSet downscale removes the suffix.
func (m *LeaderWorkerSetManager) reconcileSubRolePlan(ctx context.Context, s *replicagroups.Snapshot, plan *subRolePlan) (bool, error) {
	lws := s.LWS
	target := countSubRoles(plan.Counts)
	armed := plan.Armed
	groups := s.Groups
	if !armed && !subRoleNativeSettled(s) {
		return false, nil
	}
	if armed && int(getLWSReplicas(lws)) != target {
		return false, fmt.Errorf("LWS %s Spec changed during a sub-role transaction", lws.Name)
	}
	assignments := subRolePlanAssignments(s, plan)
	if assignments == nil {
		return false, nil
	}
	changed := false
	for _, group := range groups {
		assignment, retained := assignments[group.Leader.UID]
		if !retained {
			continue
		}
		patched, err := m.patchSubRoleGroup(ctx, group, assignment)
		if err != nil {
			return false, err
		}
		changed = changed || patched
	}
	if changed {
		return false, nil
	}
	if !armed {
		if len(assignments) != min(len(groups), target) {
			return false, nil
		}
		// Non-nil unarmed assignments already satisfy every Ready floor.
		return false, m.patchSubRoleLWS(ctx, lws, func(l *leaderv1.LeaderWorkerSet) {
			l.Spec.Replicas = ptr.To(int32(target))
			setSubRoleJSON(l, subRoleReplicasAnnotation, plan.Counts)
			plan.Armed = true
			setSubRoleJSON(l, subRolePlanAnnotation, plan)
		})
	}
	if len(groups) != target || len(assignments) != target || !subRoleNativeSettled(s) {
		return false, nil
	}
	return true, m.patchSubRoleLWS(ctx, lws, func(l *leaderv1.LeaderWorkerSet) {
		delete(l.Annotations, subRolePlanAnnotation)
	})
}

func readSubRolePlan(lws *leaderv1.LeaderWorkerSet) (*subRolePlan, error) {
	value := lws.Annotations[subRolePlanAnnotation]
	if value == "" {
		return nil, nil
	}
	plan := &subRolePlan{}
	if err := json.Unmarshal([]byte(value), plan); err != nil {
		return nil, fmt.Errorf("LWS %s has invalid sub-role scale plan: %w", lws.Name, err)
	}
	if plan.Counts == nil || plan.ReadyFloor == nil {
		return nil, fmt.Errorf("LWS %s has incomplete sub-role scale plan", lws.Name)
	}
	if err := validateSubRoleCounts(plan.Counts); err != nil {
		return nil, err
	}
	for child, floor := range plan.ReadyFloor {
		if floor < 0 || floor > plan.Counts[child] {
			return nil, fmt.Errorf("LWS %s has invalid sub-role readiness floor", lws.Name)
		}
	}
	return plan, nil
}

// syncSubRoleGroups resumes an accepted transaction before looking at new
// desired values, or repairs labels on the current physical replica set.
func (m *LeaderWorkerSetManager) syncSubRoleGroups(ctx context.Context, lws *leaderv1.LeaderWorkerSet) (*replicagroups.Snapshot, bool, error) {
	s, err := replicagroups.Observe(ctx, m.apiReader, lws)
	if err != nil {
		return nil, false, err
	}
	if s == nil || s.LWS.Generation != lws.Generation || !s.LWS.DeletionTimestamp.IsZero() {
		return nil, false, errReplicaGroupsPending
	}
	expectedOwner, liveOwner := metav1.GetControllerOf(lws), metav1.GetControllerOf(s.LWS)
	if expectedOwner == nil || liveOwner == nil || expectedOwner.UID != liveOwner.UID || expectedOwner.Kind != liveOwner.Kind || expectedOwner.APIVersion != liveOwner.APIVersion {
		return nil, false, errReplicaGroupsPending
	}
	plan, err := readSubRolePlan(s.LWS)
	if err != nil {
		return nil, false, err
	}
	if plan != nil {
		_, err = m.reconcileSubRolePlan(ctx, s, plan)
		return s, false, err
	}
	counts, err := subRoleCounts(s.LWS, subRoleReplicasAnnotation)
	if err != nil || len(counts) == 0 {
		return s, true, err
	}
	_, removing := counts[""]
	removing = removing && len(counts) == 1
	groups := s.Groups // Cleanup also covers failed or terminating groups.
	assignments := allocatedSubRoles(groups, counts)
	changed := false
	for _, group := range groups {
		name, assigned := assignments[group.Leader.UID]
		if !assigned && !removing {
			continue
		} // Excess/pending deletion groups keep their labels.
		patched, err := m.patchSubRoleGroup(ctx, group, name)
		if err != nil {
			return nil, false, err
		}
		changed = changed || patched
	}
	if removing && !changed {
		return s, false, m.patchSubRoleLWS(ctx, s.LWS, func(l *leaderv1.LeaderWorkerSet) {
			delete(l.Annotations, subRoleReplicasAnnotation)
			delete(l.Annotations, disaggv1.InitialSubRoleReplicasAnnotationKey)
		})
	}
	return s, !changed, nil
}

func (m *LeaderWorkerSetManager) scaleSubRoles(ctx context.Context, ds *disaggv1.DisaggregatedSet, lws *leaderv1.LeaderWorkerSet, counts map[string]int) error {
	if err := validateSubRoleCounts(counts); err != nil {
		return err
	}
	if !metav1.IsControlledBy(lws, ds) {
		return fmt.Errorf("LWS %s is not owned by DisaggregatedSet %s", lws.Name, ds.Name)
	}
	s, settled, err := m.syncSubRoleGroups(ctx, lws)
	if err != nil {
		return err
	}
	if !settled {
		return errReplicaGroupsPending
	}
	current, err := subRoleCounts(s.LWS, subRoleReplicasAnnotation)
	if err != nil {
		return err
	}
	observedCounts, observedErr := subRoleCounts(lws, subRoleReplicasAnnotation)
	if observedErr != nil {
		return observedErr
	}
	// Metadata-only child count changes do not advance Generation. Pin both
	// the physical and logical Specs that the planner actually observed.
	if !metav1.IsControlledBy(s.LWS, ds) || getLWSReplicas(lws) != getLWSReplicas(s.LWS) || !maps.Equal(observedCounts, current) {
		return errReplicaGroupsPending
	}
	if err != nil || maps.Equal(current, counts) {
		return err
	}
	// Removing routing is metadata-only. Persist cleanup intent so partial
	// group patches/restarts cannot restore labels from the previous counts.
	// Any accepted scale plan was completed by syncSubRoleGroups above.
	if replicas, removing := counts[""]; removing {
		if len(counts) != 1 || replicas != int(getLWSReplicas(s.LWS)) {
			return fmt.Errorf("LWS %s routing removal must keep its physical Spec", lws.Name)
		}
		return m.patchSubRoleLWS(ctx, s.LWS, func(l *leaderv1.LeaderWorkerSet) {
			setSubRoleJSON(l, subRoleReplicasAnnotation, counts)
		})
	}
	target := countSubRoles(counts)
	draining := false
	for child, n := range current {
		draining = draining || counts[child] < n
	}
	if !draining {
		return m.patchSubRoleLWS(ctx, s.LWS, func(l *leaderv1.LeaderWorkerSet) {
			l.Spec.Replicas = ptr.To(int32(target))
			setSubRoleJSON(l, subRoleReplicasAnnotation, counts)
		})
	}
	if !subRoleNativeSettled(s) {
		return errReplicaGroupsPending
	}
	plan := newSubRolePlan(s, counts)
	if plan == nil {
		return errReplicaGroupsPending
	}
	if err := m.patchSubRoleLWS(ctx, s.LWS, func(l *leaderv1.LeaderWorkerSet) { setSubRoleJSON(l, subRolePlanAnnotation, plan) }); err != nil {
		return err
	}
	return errReplicaGroupsPending
}
