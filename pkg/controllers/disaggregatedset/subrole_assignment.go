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
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

type replicaGroup struct {
	id     string
	leader *corev1.Pod
	pods   []*corev1.Pod
}

const (
	// These annotations are internal bookkeeping for a Hash scale-down. They let
	// the reconciler restore a template-provided deletion cost after the selected
	// groups have disappeared.
	scaleDownTargetAnnotationKey      = "disaggregatedset.x-k8s.io/scale-down-target"
	originalDeletionCostAnnotationKey = "disaggregatedset.x-k8s.io/original-pod-deletion-cost"
	noDeletionCost                    = "<none>"
	preferredDeletionCost             = "-2147483648"
	protectedDeletionCost             = "2147483647"
)

// SubRoleAssignmentSummary is the leader-based observed assignment state for
// one LeaderWorkerSet.
type SubRoleAssignmentSummary struct {
	Replicas      map[string]int
	ReadyReplicas map[string]int
	Unassigned    int
	GroupIDs      []string
}

// SubRoleAssignmentReconciler maintains the controller-owned sub-role label on
// every Pod in an LWS replica group.
type SubRoleAssignmentReconciler struct {
	client client.Client
}

func NewSubRoleAssignmentReconciler(c client.Client) *SubRoleAssignmentReconciler {
	return &SubRoleAssignmentReconciler{client: c}
}

func (r *SubRoleAssignmentReconciler) listGroups(ctx context.Context, namespace, lwsName string) ([]replicaGroup, error) {
	list := &corev1.PodList{}
	if err := r.client.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		leaderworkersetv1.SetNameLabelKey: lwsName,
	}); err != nil {
		return nil, fmt.Errorf("list Pods for LWS %s: %w", lwsName, err)
	}

	byID := make(map[string]*replicaGroup)
	for i := range list.Items {
		pod := &list.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		id := pod.Labels[leaderworkersetv1.GroupIndexLabelKey]
		if id == "" {
			continue
		}
		group := byID[id]
		if group == nil {
			group = &replicaGroup{id: id}
			byID[id] = group
		}
		group.pods = append(group.pods, pod)
		if pod.Labels[leaderworkersetv1.WorkerIndexLabelKey] == "0" {
			group.leader = pod
		}
	}

	groups := make([]replicaGroup, 0, len(byID))
	for _, group := range byID {
		if group.leader != nil {
			groups = append(groups, *group)
		}
	}
	slices.SortFunc(groups, compareGroupID)
	return groups, nil
}

func compareGroupID(a, b replicaGroup) int {
	aOrdinal, aErr := strconv.Atoi(a.id)
	bOrdinal, bErr := strconv.Atoi(b.id)
	if aErr == nil && bErr == nil {
		if aOrdinal < bOrdinal {
			return -1
		}
		if aOrdinal > bOrdinal {
			return 1
		}
		return 0
	}
	return strings.Compare(a.id, b.id)
}

func (r *SubRoleAssignmentReconciler) Observe(ctx context.Context, namespace, lwsName string, validSubRoles map[string]bool) (SubRoleAssignmentSummary, error) {
	groups, err := r.listGroups(ctx, namespace, lwsName)
	if err != nil {
		return SubRoleAssignmentSummary{}, err
	}
	return summarizeGroups(groups, validSubRoles), nil
}

// Reconcile keeps valid assignments where possible and fills remaining target
// deficits in API order. It patches only Pods whose effective assignment differs.
func (r *SubRoleAssignmentReconciler) Reconcile(ctx context.Context, namespace, lwsName string, order []string, desired map[string]int) (bool, SubRoleAssignmentSummary, error) {
	groups, err := r.listGroups(ctx, namespace, lwsName)
	if err != nil {
		return false, SubRoleAssignmentSummary{}, err
	}
	assignments := allocateSubRoles(groups, order, desired)

	changed := false
	for _, group := range groups {
		groupChanged, err := r.patchGroup(ctx, group, assignments[group.id])
		if err != nil {
			return changed, SubRoleAssignmentSummary{}, err
		}
		changed = changed || groupChanged
	}
	return changed, summarizeAssignments(groups, assignments), nil
}

// allocateSubRoles is the pure assignment step. Existing valid labels are kept
// up to their desired counts; remaining groups fill the largest deficit, with
// API order and then group identity providing deterministic tie-breaking.
func allocateSubRoles(groups []replicaGroup, order []string, desired map[string]int) map[string]string {
	valid := make(map[string]bool, len(order))
	for _, name := range order {
		valid[name] = true
	}

	assigned := make(map[string]int, len(order))
	result := make(map[string]string, len(groups))
	available := make([]replicaGroup, 0, len(groups))
	for _, group := range groups {
		name := group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]
		if valid[name] && assigned[name] < desired[name] {
			result[group.id] = name
			assigned[name]++
		} else {
			available = append(available, group)
		}
	}

	for _, group := range available {
		name := largestSubRoleDeficit(order, desired, assigned)
		if name == "" {
			// A caller normally fits desired to the physical group count. During
			// observation races, preserve a valid label instead of adding churn.
			current := group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]
			if valid[current] {
				name = current
			} else if len(order) > 0 {
				name = order[0]
			}
		}
		result[group.id] = name
		if name != "" {
			assigned[name]++
		}
	}
	return result
}

// PrepareScaleDown arranges the assignment multiset so the groups that the
// underlying workload controller will retain match retained. It returns true
// only once all label and readiness changes have been observed and replicas may
// safely be lowered.
func (r *SubRoleAssignmentReconciler) PrepareScaleDown(
	ctx context.Context,
	namespace, lwsName string,
	identity leaderworkersetv1.GroupIdentityType,
	order []string,
	retained map[string]int,
) (bool, error) {
	groups, err := r.listGroups(ctx, namespace, lwsName)
	if err != nil {
		return false, err
	}
	retainTotal := sumSubRoleCounts(retained)
	if retainTotal >= len(groups) {
		return true, nil
	}

	if identity == leaderworkersetv1.GroupIdentityHash {
		return r.prepareHashScaleDown(ctx, lwsName, groups, order, retained)
	}
	return r.prepareOrdinalScaleDown(ctx, lwsName, groups, order, retained)
}

func (r *SubRoleAssignmentReconciler) prepareOrdinalScaleDown(
	ctx context.Context,
	lwsName string,
	groups []replicaGroup,
	order []string,
	retained map[string]int,
) (bool, error) {
	for ordinal, group := range groups {
		if group.id != strconv.Itoa(ordinal) {
			return false, fmt.Errorf("cannot prepare scale-down for LWS %s: expected group ordinal %d, observed %s", lwsName, ordinal, group.id)
		}
	}
	retainTotal := sumSubRoleCounts(retained)
	desiredByGroup, err := scaleDownAssignments(lwsName, groups[:retainTotal], groups[retainTotal:], order, retained)
	if err != nil {
		return false, err
	}
	changed, err := r.patchAssignments(ctx, groups, desiredByGroup)
	return !changed, err
}

func (r *SubRoleAssignmentReconciler) prepareHashScaleDown(
	ctx context.Context,
	lwsName string,
	groups []replicaGroup,
	order []string,
	retained map[string]int,
) (bool, error) {
	retainTotal := sumSubRoleCounts(retained)
	victimCount := len(groups) - retainTotal

	// ReplicaSet applies scheduling, phase, and readiness before deletion cost.
	// Select the groups it already prefers, then make the boundary deterministic
	// by protecting survivors and lowering the selected leaders' cost.
	byDeletionPreference := slices.Clone(groups)
	slices.SortStableFunc(byDeletionPreference, compareHashDeletionPreference)
	victims := slices.Clone(byDeletionPreference[:victimCount])
	survivors := slices.Clone(byDeletionPreference[victimCount:])
	slices.SortFunc(victims, compareGroupID)
	slices.SortFunc(survivors, compareGroupID)

	desiredByGroup, err := scaleDownAssignments(lwsName, survivors, victims, order, retained)
	if err != nil {
		return false, err
	}
	changed, err := r.patchAssignments(ctx, groups, desiredByGroup)
	if err != nil {
		return false, err
	}

	target := strconv.Itoa(retainTotal)
	victimIDs := make(map[string]bool, len(victims))
	for _, group := range victims {
		victimIDs[group.id] = true
	}
	for _, group := range groups {
		metadataChanged, err := r.patchHashScaleDownMetadata(ctx, group, target, victimIDs[group.id])
		if err != nil {
			return false, err
		}
		changed = changed || metadataChanged
	}
	if changed {
		return false, nil
	}

	// The custom readiness condition is written by the LWS pod controller. Wait
	// for it to flow into PodReady before letting ReplicaSet select victims.
	for _, group := range victims {
		if podReady(group.leader) {
			return false, nil
		}
	}
	return true, nil
}

// scaleDownAssignments preserves as many labels as possible while making the
// survivor set exactly match retained. Victims receive the remaining assignment
// multiset, so no logical capacity disappears before the physical scale-down.
func scaleDownAssignments(
	lwsName string,
	survivors, victims []replicaGroup,
	order []string,
	retained map[string]int,
) (map[string]string, error) {
	groups := append(slices.Clone(survivors), victims...)

	available := make(map[string]int, len(order))
	for _, group := range groups {
		available[group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]]++
	}
	for name, count := range retained {
		if available[name] < count {
			return nil, fmt.Errorf("cannot retain %d groups for sub-role %s in LWS %s: only %d assigned", count, name, lwsName, available[name])
		}
	}

	desiredByGroup := make(map[string]string, len(groups))
	usedRetained := make(map[string]int, len(order))
	for _, group := range survivors {
		name := group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]
		if usedRetained[name] < retained[name] {
			desiredByGroup[group.id] = name
			usedRetained[name]++
		}
	}
	for _, group := range survivors {
		if desiredByGroup[group.id] != "" {
			continue
		}
		name := largestSubRoleDeficit(order, retained, usedRetained)
		desiredByGroup[group.id] = name
		usedRetained[name]++
	}

	remaining := make(map[string]int, len(available))
	for name, count := range available {
		remaining[name] = count - usedRetained[name]
	}
	for _, group := range victims {
		current := group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]
		if remaining[current] > 0 {
			desiredByGroup[group.id] = current
			remaining[current]--
			continue
		}
		for _, name := range order {
			if remaining[name] > 0 {
				desiredByGroup[group.id] = name
				remaining[name]--
				break
			}
		}
	}
	return desiredByGroup, nil
}

func (r *SubRoleAssignmentReconciler) patchAssignments(ctx context.Context, groups []replicaGroup, desiredByGroup map[string]string) (bool, error) {
	changed := false
	for _, group := range groups {
		groupChanged, err := r.patchGroup(ctx, group, desiredByGroup[group.id])
		if err != nil {
			return changed, err
		}
		changed = changed || groupChanged
	}
	return changed, nil
}

func compareHashDeletionPreference(a, b replicaGroup) int {
	aPod, bPod := a.leader, b.leader
	if (aPod.Spec.NodeName == "") != (bPod.Spec.NodeName == "") {
		if aPod.Spec.NodeName == "" {
			return -1
		}
		return 1
	}
	aPhase, bPhase := podPhaseRank(aPod.Status.Phase), podPhaseRank(bPod.Status.Phase)
	if aPhase != bPhase {
		return aPhase - bPhase
	}
	if podReady(aPod) != podReady(bPod) {
		if !podReady(aPod) {
			return -1
		}
		return 1
	}
	return compareGroupID(a, b)
}

func podPhaseRank(phase corev1.PodPhase) int {
	switch phase {
	case corev1.PodPending:
		return 0
	case corev1.PodUnknown:
		return 1
	case corev1.PodRunning:
		return 2
	default:
		return 3
	}
}

func (r *SubRoleAssignmentReconciler) patchHashScaleDownMetadata(ctx context.Context, group replicaGroup, target string, victim bool) (bool, error) {
	pod := group.leader
	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	previousTarget := pod.Annotations[scaleDownTargetAnnotationKey]
	if previousTarget == "" {
		if previous, found := pod.Annotations[corev1.PodDeletionCost]; found {
			pod.Annotations[originalDeletionCostAnnotationKey] = previous
		} else {
			pod.Annotations[originalDeletionCostAnnotationKey] = noDeletionCost
		}
	}
	pod.Annotations[scaleDownTargetAnnotationKey] = target
	if victim {
		pod.Annotations[leaderworkersetv1.GroupDrainingAnnotationKey] = target
		pod.Annotations[corev1.PodDeletionCost] = preferredDeletionCost
	} else {
		if previousTarget != "" && pod.Annotations[leaderworkersetv1.GroupDrainingAnnotationKey] == previousTarget {
			delete(pod.Annotations, leaderworkersetv1.GroupDrainingAnnotationKey)
		}
		pod.Annotations[corev1.PodDeletionCost] = protectedDeletionCost
	}
	if mapsEqual(before.Annotations, pod.Annotations) {
		return false, nil
	}
	if err := r.client.Patch(ctx, pod, client.MergeFrom(before)); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("patch Pod %s Hash scale-down metadata: %w", pod.Name, err)
	}
	return true, nil
}

// CleanupScaleDown restores deletion-cost annotations after a Hash scale-down
// has reached the replica count recorded by its preparation handshake.
func (r *SubRoleAssignmentReconciler) CleanupScaleDown(ctx context.Context, namespace, lwsName string, replicas int) (bool, error) {
	groups, err := r.listGroups(ctx, namespace, lwsName)
	if err != nil {
		return false, err
	}
	if len(groups) != replicas {
		return false, nil
	}
	target := strconv.Itoa(replicas)
	changed := false
	for _, group := range groups {
		pod := group.leader
		if pod.Annotations[scaleDownTargetAnnotationKey] != target {
			continue
		}
		before := pod.DeepCopy()
		if pod.Annotations[leaderworkersetv1.GroupDrainingAnnotationKey] == target {
			delete(pod.Annotations, leaderworkersetv1.GroupDrainingAnnotationKey)
		}
		if original, found := pod.Annotations[originalDeletionCostAnnotationKey]; found {
			if original == noDeletionCost {
				delete(pod.Annotations, corev1.PodDeletionCost)
			} else {
				pod.Annotations[corev1.PodDeletionCost] = original
			}
		}
		delete(pod.Annotations, originalDeletionCostAnnotationKey)
		delete(pod.Annotations, scaleDownTargetAnnotationKey)
		if err := r.client.Patch(ctx, pod, client.MergeFrom(before)); err != nil && !apierrors.IsNotFound(err) {
			return changed, fmt.Errorf("restore Pod %s Hash scale-down metadata: %w", pod.Name, err)
		}
		changed = true
	}
	return changed, nil
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func (r *SubRoleAssignmentReconciler) patchGroup(ctx context.Context, group replicaGroup, desired string) (bool, error) {
	changed := false
	pods := slices.Clone(group.pods)
	slices.SortStableFunc(pods, func(a, b *corev1.Pod) int { return workerIndex(a) - workerIndex(b) })
	for _, pod := range pods {
		if pod.Labels[disaggregatedsetv1.SubRoleLabelKey] == desired {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		if desired == "" {
			delete(pod.Labels, disaggregatedsetv1.SubRoleLabelKey)
		} else {
			pod.Labels[disaggregatedsetv1.SubRoleLabelKey] = desired
		}
		if err := r.client.Patch(ctx, pod, client.MergeFrom(before)); err != nil && !apierrors.IsNotFound(err) {
			return changed, fmt.Errorf("patch Pod %s sub-role assignment: %w", pod.Name, err)
		}
		changed = true
	}
	return changed, nil
}

func summarizeGroups(groups []replicaGroup, valid map[string]bool) SubRoleAssignmentSummary {
	summary := SubRoleAssignmentSummary{Replicas: make(map[string]int), ReadyReplicas: make(map[string]int)}
	for _, group := range groups {
		summary.GroupIDs = append(summary.GroupIDs, group.id)
		name := group.leader.Labels[disaggregatedsetv1.SubRoleLabelKey]
		if !valid[name] {
			summary.Unassigned++
			continue
		}
		summary.Replicas[name]++
		if podReady(group.leader) {
			summary.ReadyReplicas[name]++
		}
	}
	return summary
}

func summarizeAssignments(groups []replicaGroup, assignments map[string]string) SubRoleAssignmentSummary {
	summary := SubRoleAssignmentSummary{Replicas: make(map[string]int), ReadyReplicas: make(map[string]int)}
	for _, group := range groups {
		summary.GroupIDs = append(summary.GroupIDs, group.id)
		name := assignments[group.id]
		if name == "" {
			summary.Unassigned++
			continue
		}
		summary.Replicas[name]++
		if podReady(group.leader) {
			summary.ReadyReplicas[name]++
		}
	}
	return summary
}

func largestSubRoleDeficit(order []string, desired, assigned map[string]int) string {
	best := ""
	bestDeficit := 0
	for _, name := range order {
		if deficit := desired[name] - assigned[name]; deficit > bestDeficit {
			best, bestDeficit = name, deficit
		}
	}
	return best
}

// fitSubRoleTargets returns the deterministic prefix of the final assignment
// vector for total physical groups.
func fitSubRoleTargets(order []string, final map[string]int, total int) map[string]int {
	result := make(map[string]int, len(order))
	for range max(total, 0) {
		name := largestSubRoleDeficit(order, final, result)
		if name == "" {
			if len(order) == 0 {
				break
			}
			name = order[0]
		}
		result[name]++
	}
	return result
}

func sumSubRoleCounts(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

func hasExpectedGroupCount(summary SubRoleAssignmentSummary, replicas int) bool {
	return len(summary.GroupIDs) == replicas
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func workerIndex(pod *corev1.Pod) int {
	index, _ := strconv.Atoi(pod.Labels[leaderworkersetv1.WorkerIndexLabelKey])
	return index
}
