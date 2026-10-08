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
	"time"

	"k8s.io/utils/ptr"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

// rolloutRole binds one planner dimension to its physical workload. Siblings
// share LWS; their counts are values, never synthetic Kubernetes objects.
type rolloutRole struct {
	LWS             *leaderv1.LeaderWorkerSet
	SubRole         string
	Replicas        int
	InitialReplicas int
}

func (r *rolloutRole) readinessKey() string {
	if r.SubRole != "" {
		return r.LWS.Name + "/" + r.SubRole
	}
	return r.LWS.Name
}

type rolloutRevision struct {
	Revision  string
	Roles     map[string]*rolloutRole
	createdAt time.Time
}

type rolloutRevisionList []rolloutRevision

func (revisions rolloutRevisionList) totalReplicas(role string) int {
	total := 0
	for _, revision := range revisions {
		if binding := revision.Roles[role]; binding != nil {
			total += binding.Replicas
		}
	}
	return total
}

// subRoleTargets groups logical decisions by their real Kubernetes write target.
func subRoleTargets(revision rolloutRevision, names []string, targets RoleReplicaState) map[*leaderv1.LeaderWorkerSet]map[string]int {
	grouped := map[*leaderv1.LeaderWorkerSet]map[string]int{}
	for i, name := range names {
		binding := revision.Roles[name]
		if binding == nil || binding.SubRole == "" {
			continue
		}
		if grouped[binding.LWS] == nil {
			grouped[binding.LWS] = map[string]int{}
		}
		grouped[binding.LWS][binding.SubRole] = targets[i]
	}
	return grouped
}

func desiredSubRoles(role *disaggv1.DisaggregatedRoleSpec, desired map[string]int) map[string]int {
	counts := map[string]int{}
	for _, child := range role.SubRoles {
		counts[child.Name] = desired[childRoleKey(role.Name, child.Name)]
	}
	return counts
}

func (m *LeaderWorkerSetManager) initializeSubRoles(ctx context.Context, lws *leaderv1.LeaderWorkerSet, counts map[string]int) (bool, error) {
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
	currentChanged := len(current) == 0
	if currentChanged {
		current = fitSubRoles(counts, int(getLWSReplicas(lws)))
	}
	for child := range counts {
		if _, found := current[child]; !found {
			current[child] = 0
			currentChanged = true
		}
	}
	initialChanged := len(initial) == 0
	if initialChanged {
		total, _ := disaggutils.GetInitialReplicas(lws)
		initial = fitSubRoles(current, max(int(total), int(getLWSReplicas(lws))))
	}
	if !currentChanged && !initialChanged {
		return false, nil
	}
	return true, m.patchSubRoleLWS(ctx, lws, func(l *leaderv1.LeaderWorkerSet) {
		setSubRoleJSON(l, subRoleReplicasAnnotation, current)
		setSubRoleJSON(l, disaggv1.InitialSubRoleReplicasAnnotationKey, initial)
	})
}

// prepareSubRoles owns the lifecycle order for both steady state and rollout:
// finish accepted work, initialize/remove routing, then observe coherent groups.
// No desired baseline is written here: the rollout first resolves effective
// logical targets (including External clamping), then persists those targets.
func (m *LeaderWorkerSetManager) prepareSubRoles(ctx context.Context, ds *disaggv1.DisaggregatedSet, lws *leaderv1.LeaderWorkerSet, counts map[string]int) (*replicagroups.Snapshot, bool, error) {
	if len(counts) == 0 && lws.Annotations[subRoleReplicasAnnotation] == "" && lws.Annotations[subRolePlanAnnotation] == "" {
		return nil, true, nil
	}
	if lws.Annotations[subRolePlanAnnotation] == "" {
		if len(counts) == 0 {
			// Removing routing preserves physical Spec until cleanup is observed.
			err := m.scaleSubRoles(ctx, ds, lws, map[string]int{"": int(getLWSReplicas(lws))})
			if errors.Is(err, errReplicaGroupsPending) {
				err = nil
			}
			return nil, false, err
		}
		changed, err := m.initializeSubRoles(ctx, lws, counts)
		if err != nil || changed {
			return nil, false, err
		}
	}
	return m.syncSubRoleGroups(ctx, lws)
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
			s, ready, err := m.prepareSubRoles(ctx, ds, lws, counts)
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

func expandSubRoleRevision(revision disaggutils.RevisionRoles, snapshots map[string]*replicagroups.Snapshot, readiness rolloutReadiness) (rolloutRevision, error) {
	expanded := rolloutRevision{Revision: revision.Revision, createdAt: revision.LatestCreationTime(), Roles: map[string]*rolloutRole{}}
	for name, lws := range revision.Roles {
		if snapshot := snapshots[lws.Name]; snapshot != nil {
			lws = snapshot.LWS
		}
		counts, err := subRoleCounts(lws, subRoleReplicasAnnotation)
		if err != nil {
			return expanded, err
		}
		if len(counts) == 0 {
			expanded.Roles[name] = &rolloutRole{LWS: lws, Replicas: int(getLWSReplicas(lws)), InitialReplicas: revision.GetInitialReplicasPerRole(name)}
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
			logical := &rolloutRole{LWS: lws, SubRole: child, Replicas: count, InitialReplicas: initial[child]}
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
			readiness[logical.readinessKey()] = replicaReadiness{raw: int(availability.ReadyReplicas), committed: int(availability.RetainedReadyReplicas)}
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

// syncSubRoleBaseline records the same resolved targets the ordinary planner
// consumes, in one physical write. A revision's child vector freezes once old.
func (m *LeaderWorkerSetManager) syncSubRoleBaseline(ctx context.Context, lws *leaderv1.LeaderWorkerSet, initial map[string]int) error {
	if err := validateSubRoleCounts(initial); err != nil {
		return err
	}
	current, err := subRoleCounts(lws, disaggv1.InitialSubRoleReplicasAnnotationKey)
	if err != nil {
		return err
	}
	total := strconv.Itoa(countSubRoles(initial))
	if maps.Equal(current, initial) && lws.Annotations[disaggv1.InitialReplicasAnnotationKey] == total {
		return nil
	}
	return m.patchSubRoleLWS(ctx, lws, func(l *leaderv1.LeaderWorkerSet) {
		setSubRoleJSON(l, disaggv1.InitialSubRoleReplicasAnnotationKey, initial)
		l.Annotations[disaggv1.InitialReplicasAnnotationKey] = total
	})
}
