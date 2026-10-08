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

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
)

// subRoleStatusObservation is shared read-only by the two status writers. It is
// collected after workload reconciliation, never reused for mutation decisions.
type subRoleStatusObservation struct {
	children []disaggv1.SubRoleStatus
	assigned bool
}

func (m *LeaderWorkerSetManager) observeSubRoleStatuses(ctx context.Context, ds *disaggv1.DisaggregatedSet, workloads []*leaderv1.LeaderWorkerSet, revision string) (map[string]subRoleStatusObservation, error) {
	byRole := make(map[string][]*leaderv1.LeaderWorkerSet)
	for _, lws := range workloads {
		role := lws.Labels[disaggv1.RoleLabelKey]
		byRole[role] = append(byRole[role], lws)
	}
	observed := make(map[string]subRoleStatusObservation)
	for _, role := range ds.Spec.Roles {
		if len(role.SubRoles) == 0 {
			continue
		}
		children, assigned, err := m.subRoleStatus(ctx, &role, byRole[role.Name], revision)
		if err != nil {
			return nil, fmt.Errorf("observing sub-role status for role %s: %w", role.Name, err)
		}
		observed[role.Name] = subRoleStatusObservation{children: children, assigned: assigned}
	}
	return observed, nil
}

func (m *LeaderWorkerSetManager) subRoleStatus(ctx context.Context, role *disaggv1.DisaggregatedRoleSpec, workloads []*leaderv1.LeaderWorkerSet, revision string) ([]disaggv1.SubRoleStatus, bool, error) {
	statuses := make([]disaggv1.SubRoleStatus, len(role.SubRoles))
	index := map[string]int{}
	for i, child := range role.SubRoles {
		index[child.Name] = i
		statuses[i].Name = child.Name
	}
	assigned := true
	for _, lws := range workloads {
		s, err := replicagroups.Observe(ctx, m.apiReader, lws)
		if err != nil {
			return nil, false, err
		}
		if s == nil {
			return nil, false, errReplicaGroupsPending
		}
		for _, group := range activeSubRoleGroups(s) {
			name, coherent := groupSubRole(group)
			i, known := index[name]
			if !known || !coherent {
				assigned = false
				continue
			}
			statuses[i].Replicas++
			if group.Ready && !group.Terminating {
				statuses[i].ReadyReplicas++
			}
			if lws.Labels[disaggv1.RevisionLabelKey] == revision {
				statuses[i].UpdatedReplicas++
			}
		}
	}
	return statuses, assigned, nil
}
