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
	"errors"
	"maps"
	"slices"
	"strconv"

	"k8s.io/utils/ptr"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

const physicalLWSAnnotation = "internal/physical-lws" // In-memory planner views only; never persisted.

func desiredSubRoles(role *disaggv1.DisaggregatedRoleSpec, desired map[string]int) map[string]int {
	counts := map[string]int{}
	for _, child := range role.SubRoles {
		counts[child.Name] = desired[childRoleKey(role.Name, child.Name)]
	}
	return counts
}

func (m *LeaderWorkerSetManager) initializeSubRoles(ctx context.Context, lws *leaderv1.LeaderWorkerSet, counts map[string]int, target bool) (bool, error) {
	if _, removing := counts[""]; removing {
		return false, nil
	}
	current, err := subRoleCounts(lws, subRoleReplicasAnnotation)
	if err != nil {
		return false, err
	}
	if _, removing := current[""]; removing && len(current) == 1 {
		return false, nil // Finish metadata cleanup before re-enabling children.
	}
	initial, err := subRoleCounts(lws, disaggv1.InitialSubRoleReplicasAnnotationKey)
	if err != nil {
		return false, err
	}
	if len(current) == 0 && len(counts) == 0 {
		return false, nil
	}
	if len(current) == 0 {
		current = fitSubRoles(counts, int(getLWSReplicas(lws)))
	}
	currentChanged := false
	for child := range counts {
		if _, found := current[child]; !found {
			current[child] = 0
			currentChanged = true
		}
	}
	if len(initial) == 0 {
		total, _ := disaggutils.GetInitialReplicas(lws)
		initial = fitSubRoles(current, max(int(total), int(getLWSReplicas(lws))))
	}
	if target && len(counts) > 0 {
		initial = maps.Clone(counts)
	}
	if value := lws.Annotations[subRoleReplicasAnnotation]; value != "" {
		oldInitial, _ := subRoleCounts(lws, disaggv1.InitialSubRoleReplicasAnnotationKey)
		if maps.Equal(initial, oldInitial) && !currentChanged {
			return false, nil
		}
	}
	return true, m.patchSubRoleLWS(ctx, lws, func(l *leaderv1.LeaderWorkerSet) {
		setSubRoleJSON(l, subRoleReplicasAnnotation, current)
		setSubRoleJSON(l, disaggv1.InitialSubRoleReplicasAnnotationKey, initial)
		if target {
			l.Annotations[disaggv1.InitialReplicasAnnotationKey] = strconv.Itoa(countSubRoles(initial))
		}
	})
}

// prepareSubRoleRevisions resumes every accepted transaction before expanding
// the logical roles. An interrupted revision must finish its accepted assignment
// before another plan can spend the same availability.
func (m *LeaderWorkerSetManager) prepareSubRoleRevisions(ctx context.Context, ds *disaggv1.DisaggregatedSet, old disaggutils.RevisionRolesList, target disaggutils.RevisionRoles, desired map[string]int) (map[string]*replicagroups.Snapshot, bool, error) {
	configs := disaggutils.GetRoleConfigs(ds)
	snapshots := map[string]*replicagroups.Snapshot{}
	settled := true
	for _, revision := range append(slices.Clone(old), target) {
		for name, lws := range revision.Roles {
			counts := map[string]int{}
			if role := configs[name]; role != nil {
				counts = desiredSubRoles(role, desired)
			}
			if len(counts) == 0 && lws.Annotations[subRoleReplicasAnnotation] == "" && lws.Annotations[subRolePlanAnnotation] == "" {
				continue
			}
			// Finish a pending plan before changing even its initial baseline.
			if lws.Annotations[subRolePlanAnnotation] == "" {
				if len(counts) == 0 {
					// Removing routing partitions does not remove physical groups.
					// Finish unlabeling/unprotecting before returning to an ordinary
					// parent-only planner layout, including during a template rollout.
					err := m.scaleSubRoles(ctx, ds, lws, map[string]int{"": int(getLWSReplicas(lws))})
					if err != nil && !errors.Is(err, errReplicaGroupsPending) {
						return nil, false, err
					}
					settled = false
					continue
				}
				changed, err := m.initializeSubRoles(ctx, lws, counts, revision.Revision == target.Revision)
				if err != nil {
					return nil, false, err
				}
				if changed {
					settled = false
					continue
				}
			}
			s, ready, err := m.syncSubRoleGroups(ctx, lws)
			if err != nil {
				return nil, false, err
			}
			settled = settled && ready
			snapshots[lws.Name] = s
		}
	}
	return snapshots, settled, nil
}

func groupSubRole(group replicagroups.Group) (string, bool) {
	name := group.Leader.Labels[disaggv1.SubRoleLabelKey]
	for _, pod := range group.Pods {
		if pod.Labels[disaggv1.SubRoleLabelKey] != name {
			return name, false
		}
	}
	return name, true
}

func expandSubRoleRevision(revision disaggutils.RevisionRoles, snapshots map[string]*replicagroups.Snapshot, readiness rolloutReadiness) (disaggutils.RevisionRoles, error) {
	expanded := revision
	expanded.Roles = map[string]*leaderv1.LeaderWorkerSet{}
	for name, lws := range revision.Roles {
		if snapshot := snapshots[lws.Name]; snapshot != nil {
			lws = snapshot.LWS
		}
		counts, err := subRoleCounts(lws, subRoleReplicasAnnotation)
		if err != nil {
			return expanded, err
		}
		if len(counts) == 0 {
			expanded.Roles[name] = lws
			continue
		}
		// Each child is an ordinary planner role. The physical LWS is only
		// an execution detail; its replica field is the sum of child targets.
		if countSubRoles(counts) != int(getLWSReplicas(lws)) {
			return expanded, errReplicaGroupsPending
		}
		initial, err := subRoleCounts(lws, disaggv1.InitialSubRoleReplicasAnnotationKey)
		if err != nil {
			return expanded, err
		}
		snapshot := snapshots[lws.Name]
		if snapshot == nil {
			return expanded, errReplicaGroupsPending
		}
		for child, count := range counts {
			key := childRoleKey(name, child)
			logical := lws.DeepCopy()
			logical.Name = lws.Name + "/" + child
			logical.Spec.Replicas = ptr.To(int32(count))
			logical.Annotations[physicalLWSAnnotation] = lws.Name
			logical.Annotations[disaggv1.InitialReplicasAnnotationKey] = strconv.Itoa(initial[child])
			expanded.Roles[key] = logical
			// Keep every leader in the snapshot so a native pending deletion is
			// reserved against this child too; filter only readiness, not victims.
			view := *snapshot
			view.Groups = slices.Clone(snapshot.Groups)
			for i, group := range view.Groups {
				assigned, coherent := groupSubRole(group)
				view.Groups[i].Ready = group.Ready && coherent && assigned == child
			}
			availability := view.Availability()
			readiness[logical.Name] = replicaReadiness{raw: int(availability.ReadyReplicas), committed: int(availability.RetainedReadyReplicas)}
		}
	}
	return expanded, nil
}

func expandSubRoleSpec(ds *disaggv1.DisaggregatedSet, desired map[string]int) *disaggv1.DisaggregatedSet {
	expanded := ds.DeepCopy()
	expanded.Spec.Roles = nil
	for _, role := range ds.Spec.Roles {
		if len(role.SubRoles) == 0 {
			expanded.Spec.Roles = append(expanded.Spec.Roles, role)
			continue
		}
		for _, child := range role.SubRoles {
			logical := *role.DeepCopy()
			logical.Name = childRoleKey(role.Name, child.Name)
			logical.SubRoles = nil
			logical.Scaling = child.Scaling
			logical.Spec.Replicas = ptr.To(int32(desired[logical.Name]))
			// Inherit the raw IntOrString values. The ordinary config extractor
			// resolves percentages against this child's desired replica count.
			expanded.Spec.Roles = append(expanded.Spec.Roles, logical)
		}
	}
	return expanded
}

func physicalSubRoleLWS(logical *leaderv1.LeaderWorkerSet) *leaderv1.LeaderWorkerSet {
	name := logical.Annotations[physicalLWSAnnotation]
	if name == "" {
		return logical
	}
	physical := logical.DeepCopy()
	physical.Name = name
	counts, _ := subRoleCounts(physical, subRoleReplicasAnnotation)
	physical.Spec.Replicas = ptr.To(int32(countSubRoles(counts)))
	delete(physical.Annotations, physicalLWSAnnotation)
	return physical
}
