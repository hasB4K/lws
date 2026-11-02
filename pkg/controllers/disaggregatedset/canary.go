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
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

func canaryPercent(ds *disaggv1.DisaggregatedSet) (int, bool) {
	strategy := ds.Spec.RolloutStrategy
	if strategy == nil || strategy.CanaryPercent == nil || *strategy.CanaryPercent == 100 {
		return 100, false
	}
	return int(*strategy.CanaryPercent), true
}

// Round new groups up, but keep both revisions for a partial canary. One group
// cannot be split: keep it old and report the limitation before planning.
func canaryOldReplicas(desired, percent int) int {
	if percent == 0 || desired <= 1 {
		return desired
	}
	updated := min(desired-1, max(1, int((int64(desired)*int64(percent)+99)/100)))
	return desired - updated
}

// reconcileCanaryScaleUp fills only missing stable capacity. Ordinary rollout
// plans move groups from old to new, not the reverse.
// This growth cannot consume surge or authorize a drain in the same snapshot.
func (executor *RollingUpdateExecutor) reconcileCanaryScaleUp(
	ctx context.Context, ds *disaggv1.DisaggregatedSet,
	old disaggutils.RevisionRolesList, target disaggutils.RevisionRoles,
	desired map[string]int, inputs rolloutInputs,
) (bool, error) {
	percent, active := canaryPercent(ds)
	if !active {
		return false, nil
	}
	remaining, wanted := 0, 0
	for _, name := range inputs.allRoleNames {
		remaining += inputs.readiness.totalReplicas(old, name)
		wanted += desired[name]
	}
	if remaining == 0 || wanted == 0 {
		return false, nil // Do not recreate retired history or block scaling to zero.
	}
	for _, name := range inputs.allRoleNames {
		if percent > 0 && desired[name] == 1 {
			executor.canaryBlocked = fmt.Sprintf("Cannot split role %s's single replica group between two revisions; increase its replicas or set canaryPercent to 100", name)
			return true, nil
		}
	}

	// Prefer the oldest surviving compatible revision, not the newest failed
	// canary. Every active role must still exist; never reconstruct old templates
	// or enroll current sub-roles into an older revision's layout.
	candidates := old.SortedByNewestTimestamp()
	for i := len(candidates) - 1; i >= 0; i-- {
		stable := candidates[i]
		if !compatibleCanaryRevision(stable, inputs.allRoleNames, desired, inputs.readiness) {
			continue
		}
		targets := make(RoleReplicaState, len(inputs.allRoleNames))
		changed := false
		for j, name := range inputs.allRoleNames {
			current := inputs.readiness.role(stable, name).spec
			allOld := inputs.readiness.totalReplicas(old, name)
			newSpec := inputs.readiness.role(target, name).spec
			// The live target may already exceed its requested share. Lowering
			// the percentage must not recreate stable groups to undo that progress.
			missing := min(inputs.config[j].CanaryOldReplicas-allOld, desired[name]-newSpec-allOld)
			targets[j] = current + max(0, missing)
			changed = changed || targets[j] > current
		}
		if !changed {
			return false, nil
		}
		return true, executor.scaleRevisionWithStrategy(ctx, ds, stable, inputs.allRoleNames, targets, scaleUp, false)
	}
	executor.canaryBlocked = "Canary requires a surviving old revision with the same active roles and sub-roles; finish the layout change with canaryPercent 100 before starting a canary"
	return true, nil
}

func compatibleCanaryRevision(revision disaggutils.RevisionRoles, names []string, desired map[string]int, readiness rolloutReadiness) bool {
	for _, lws := range revision.Roles {
		if !lws.DeletionTimestamp.IsZero() {
			return false
		}
	}
	for _, name := range names {
		observed := readiness.role(revision, name)
		if (observed.initial > 0) != (desired[name] > 0) || desired[name] > 0 && observed.spec == 0 {
			return false
		}
	}
	return true
}
