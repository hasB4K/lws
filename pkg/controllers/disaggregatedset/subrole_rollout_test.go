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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

func virtualRevision(t *testing.T, s *replicagroups.Snapshot) (disaggutils.RevisionRoles, rolloutReadiness) {
	t.Helper()
	ready := rolloutReadiness{}
	revision, err := expandSubRoleRevision(disaggutils.RevisionRoles{Revision: "a", Roles: map[string]*leaderv1.LeaderWorkerSet{"model": s.LWS}}, map[string]*replicagroups.Snapshot{s.LWS.Name: s}, ready)
	require.NoError(t, err)
	return revision, ready
}

func TestSubRoleAdapterRequiresCoherentOwnedGroups(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	s := f.observe()
	spoof := s.Groups[0].Leader.DeepCopy()
	spoof.Name = "spoof"
	spoof.UID = "foreign"
	spoof.ResourceVersion = ""
	spoof.OwnerReferences[0].UID = "foreign-rs"
	require.NoError(t, f.manager.client.Create(context.Background(), spoof))
	worker := s.Groups[0].Pods[1]
	worker.Labels[disaggv1.SubRoleLabelKey] = "b"
	require.NoError(t, f.manager.client.Update(context.Background(), worker))
	s = f.observe()
	require.Len(t, s.Groups, 3)
	expanded, ready := virtualRevision(t, s)
	require.Equal(t, replicaReadiness{raw: 1, committed: 1}, ready[expanded.Roles["model/a"].Name])
	require.Equal(t, replicaReadiness{raw: 1, committed: 1}, ready[expanded.Roles["model/b"].Name])
	require.NotContains(t, expanded.Roles, "model", "the shared physical parent is not an extra planner role")
	require.Equal(t, f.lws.Name, physicalSubRoleLWS(expanded.Roles["model/a"]).Name)
	require.EqualValues(t, 3, getLWSReplicas(physicalSubRoleLWS(expanded.Roles["model/a"])))

	// Every positive child is an ordinary required role. A child cannot supply
	// usable capacity while another required child or ordinary role is absent.
	expanded.Roles["router"] = &leaderv1.LeaderWorkerSet{}
	expanded.Roles["router"].Name = "router"
	expanded.Roles["router"].Spec.Replicas = ptr.To[int32](1)
	names := []string{"model/a", "model/b", "router"}
	state := rolloutStateForRevision(names, disaggutils.RevisionRolesList{expanded}, expanded, disaggutils.RevisionRoles{}, []int{2, 1, 1}, make([]RollingUpdateConfig, 3), ready)
	require.Equal(t, []bool{true, true, true}, state.ActiveOld.RequiredRoles)
	snapshot := snapshotForRolloutState(state)
	for _, role := range snapshot {
		require.Zero(t, role.ActiveOldUsableReadyReplicas)
	}
	ready["router"] = replicaReadiness{raw: 1, committed: 1}
	state = rolloutStateForRevision(names, disaggutils.RevisionRolesList{expanded}, expanded, disaggutils.RevisionRoles{}, []int{2, 1, 1}, make([]RollingUpdateConfig, 3), ready)
	require.Equal(t, 1, snapshotForRolloutState(state)[0].ActiveOldUsableReadyReplicas)
	ready[expanded.Roles["model/b"].Name] = replicaReadiness{}
	state = rolloutStateForRevision(names, disaggutils.RevisionRolesList{expanded}, expanded, disaggutils.RevisionRoles{}, []int{2, 1, 1}, make([]RollingUpdateConfig, 3), ready)
	require.Zero(t, snapshotForRolloutState(state)[0].ActiveOldUsableReadyReplicas)
}

func TestSubRoleInitializerAddsZeroDimensionAndRemovalDuringRollout(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	ctx := context.Background()
	changed, err := f.manager.initializeSubRoles(ctx, f.observe().LWS, map[string]int{"a": 2, "b": 1, "new": 1}, true)
	require.NoError(t, err)
	require.True(t, changed)
	s := f.observe()
	counts, err := subRoleCounts(s.LWS, subRoleReplicasAnnotation)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"a": 2, "b": 1, "new": 0}, counts)
	expanded, _ := virtualRevision(t, s)
	require.NotNil(t, expanded.Roles["model/new"])
	require.Zero(t, getLWSReplicas(expanded.Roles["model/new"]))
	// Disabling virtual roles while an old revision exists strips routing
	// metadata in place, without interpreting parent desired=3 as child total=0.
	f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{Name: "model"}}
	for attempt := 0; attempt < 8; attempt++ {
		old := disaggutils.RevisionRolesList{{Revision: "a", Roles: map[string]*leaderv1.LeaderWorkerSet{"model": f.observe().LWS}}}
		_, _, err = f.manager.prepareSubRoleRevisions(ctx, f.ds, old, disaggutils.RevisionRoles{Revision: "b"}, map[string]int{"model": 3})
		require.NoError(t, err)
		f.native()
	}
	s = f.observe()
	require.EqualValues(t, 3, getLWSReplicas(s.LWS))
	require.Empty(t, s.LWS.Annotations[subRoleReplicasAnnotation])
	for _, g := range s.Groups {
		require.Empty(t, g.Leader.Labels[disaggv1.SubRoleLabelKey])
	}
}

func TestSubRoleExecutorNeverResurrectsStaleChildCounts(t *testing.T) {
	for _, targets := range []RoleReplicaState{{4, 2}, {4, 3}, {3, 2}} {
		t.Run(fmt.Sprint(targets), func(t *testing.T) {
			f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "a", "a", "a", "b")
			executor := &RollingUpdateExecutor{LWSManager: f.manager}
			original, _ := virtualRevision(t, f.observe())
			names := []string{"model/a", "model/b"}
			require.ErrorIs(t, executor.scaleRevision(context.Background(), f.ds, original, names, targets, scaleDown), errReplicaGroupsPending)
			for range 8 {
				f.sync()
				f.native()
			}
			down := f.observe()
			counts, err := subRoleCounts(down.LWS, subRoleReplicasAnnotation)
			require.NoError(t, err)
			require.Equal(t, map[string]int{"a": targets[0], "b": 1}, counts)
			require.ErrorIs(t, executor.scaleRevision(context.Background(), f.ds, original, names, targets, scaleUp), errReplicaGroupsPending, "stale a=5 must not overwrite accepted drain")
			fresh, _ := virtualRevision(t, f.observe())
			require.NoError(t, executor.scaleRevision(context.Background(), f.ds, fresh, names, targets, scaleUp))
			latest := f.observe()
			counts, err = subRoleCounts(latest.LWS, subRoleReplicasAnnotation)
			require.NoError(t, err)
			require.Equal(t, map[string]int{"a": targets[0], "b": targets[1]}, counts)
			require.EqualValues(t, targets[0]+targets[1], getLWSReplicas(latest.LWS))
		})
	}
}

func TestSubRoleInitializerWaitsForRemovalBeforeReenable(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	ctx := context.Background()
	require.NoError(t, f.manager.patchSubRoleLWS(ctx, f.observe().LWS, func(lws *leaderv1.LeaderWorkerSet) {
		setSubRoleJSON(lws, subRoleReplicasAnnotation, map[string]int{"": 3})
	}))
	changed, err := f.manager.initializeSubRoles(ctx, f.observe().LWS, map[string]int{"new": 3}, true)
	require.NoError(t, err)
	require.False(t, changed)
	counts, err := subRoleCounts(f.observe().LWS, subRoleReplicasAnnotation)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"": 3}, counts, "cleanup must finish before named children can be added again")
}

func TestSubRoleStatusTracksWholeGroupAndAssignedCondition(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	s := f.observe()
	worker := s.Groups[0].Pods[1]
	worker.Status.Conditions = nil
	require.NoError(t, f.manager.client.Status().Update(context.Background(), worker))
	role := &disaggv1.DisaggregatedRoleSpec{SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}, {Name: "b"}}}
	statuses, assigned, err := f.manager.subRoleStatus(context.Background(), role, []*leaderv1.LeaderWorkerSet{f.lws}, "")
	require.NoError(t, err)
	require.True(t, assigned)
	require.EqualValues(t, 2, statuses[0].Replicas)
	require.EqualValues(t, 1, statuses[0].ReadyReplicas)
	worker = &corev1.Pod{}
	require.NoError(t, f.manager.client.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: "model-0-1"}, worker))
	delete(worker.Labels, disaggv1.SubRoleLabelKey)
	require.NoError(t, f.manager.client.Update(context.Background(), worker))
	_, assigned, err = f.manager.subRoleStatus(context.Background(), role, []*leaderv1.LeaderWorkerSet{f.lws}, "")
	require.NoError(t, err)
	require.False(t, assigned)
}

func TestSubRoleUnschedulabilityDoesNotLeakAcrossSiblings(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "b")
	s := f.observe()
	for i, g := range s.Groups {
		leader := g.Leader
		leader.Status.Phase = corev1.PodPending
		leader.Status.Conditions = nil
		if i == 1 {
			leader.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Minute))}}
		}
		require.NoError(t, f.manager.client.Status().Update(context.Background(), leader))
	}
	revision, ready := virtualRevision(t, f.observe())
	executor := &RollingUpdateExecutor{LWSManager: f.manager}
	flags, err := executor.targetUnschedulableRoles(context.Background(), revision, []string{"model/a", "model/b"}, ready)
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, flags)
}

func TestSubRoleTargetOnlyRemovedChildRemainsVisible(t *testing.T) {
	for _, policy := range []disaggv1.ScalingDuringRolloutPolicy{disaggv1.ScalingDuringRolloutPolicyRolloutCoupled, disaggv1.ScalingDuringRolloutPolicyAdvanceRollout} {
		t.Run(string(policy), func(t *testing.T) {
			f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "b")
			target, ready := virtualRevision(t, f.observe())
			old := disaggutils.RevisionRoles{Revision: "old", Roles: map[string]*leaderv1.LeaderWorkerSet{}}
			// Old predates child b. The current target added and issued b, then
			// the user removed b without creating a new template revision.
			for _, name := range []string{"model/a"} {
				old.Roles[name] = target.Roles[name].DeepCopy()
				old.Roles[name].Name = "old-" + old.Roles[name].Name
				old.Roles[name].Spec.Replicas = ptr.To[int32](2)
			}
			f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a", Replicas: ptr.To[int32](1)}}}}
			f.ds.Spec.ScalingPolicy = &disaggv1.DisaggregatedSetScalingPolicy{DuringRollout: policy}
			desired := map[string]int{"model": 1, "model/a": 1}
			inputs := buildRolloutInputs(expandSubRoleSpec(f.ds, desired), disaggutils.RevisionRolesList{old}, target, desired)
			require.Equal(t, []string{"model/a", "model/b"}, inputs.allRoleNames)
			require.Equal(t, RoleReplicaState{1, 0}, inputs.targetReplicas)
			complete, _ := rolloutCompletionStatus(nil, target, inputs.allRoleNames, inputs.targetReplicas, ready, policy == disaggv1.ScalingDuringRolloutPolicyAdvanceRollout)
			require.Equal(t, policy == disaggv1.ScalingDuringRolloutPolicyRolloutCoupled, complete, "default defers correction until steady state; opt-in requires exact child targets")
			executor := &RollingUpdateExecutor{LWSManager: f.manager}
			require.ErrorIs(t, executor.scaleRevision(context.Background(), f.ds, target, inputs.allRoleNames, inputs.targetReplicas, scaleDown), errReplicaGroupsPending)
			plan, err := readSubRolePlan(f.observe().LWS)
			require.NoError(t, err)
			require.Equal(t, map[string]int{"a": 1, "b": 0}, plan.Counts)
		})
	}
}

func TestSubRoleBudgetsResolveIndependently(t *testing.T) {
	for _, tc := range []struct {
		name               string
		surge, unavailable intstr.IntOrString
		want               []RollingUpdateConfig
	}{
		{"integer", intstr.FromInt(2), intstr.FromInt(1), []RollingUpdateConfig{{MaxSurge: 2, MaxUnavailable: 1}, {MaxSurge: 2, MaxUnavailable: 1}}},
		{"percentage", intstr.FromString("50%"), intstr.FromString("20%"), []RollingUpdateConfig{{MaxSurge: 2}, {MaxSurge: 4, MaxUnavailable: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &disaggv1.DisaggregatedSet{Spec: disaggv1.DisaggregatedSetSpec{Roles: []disaggv1.DisaggregatedRoleSpec{{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}, {Name: "b"}}}}}}
			ds.Spec.Roles[0].Spec.RolloutStrategy.RollingUpdateConfiguration = &leaderv1.RollingUpdateConfiguration{MaxSurge: tc.surge, MaxUnavailable: tc.unavailable}
			desired := map[string]int{"model": 10, "model/a": 3, "model/b": 7}
			expanded := expandSubRoleSpec(ds, desired)
			names := disaggutils.GetRoleNames(expanded)
			require.Equal(t, []string{"model/a", "model/b"}, names)
			require.Equal(t, tc.want, extractRollingUpdateConfig(expanded, names, desired))
			require.Equal(t, tc.surge, expanded.Spec.Roles[0].Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge)
		})
	}
}

func TestSubRoleFormerSharedCeilingStallConverges(t *testing.T) {
	// The shared-envelope prototype stalled on these edits because obsolete
	// target child A held B's physical surge slots. Rebuild the real leaf adapter
	// inputs on every edit, including removed-child history and reordered names.
	edits := [][2]int{{1, 4}, {2, 0}, {6, 0}, {3, 6}, {0, 3}}
	for _, advance := range []bool{false, true} {
		f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "b", "b", "b")
		if advance {
			f.ds.Spec.ScalingPolicy = &disaggv1.DisaggregatedSetScalingPolicy{DuringRollout: disaggv1.ScalingDuringRolloutPolicyAdvanceRollout}
		}
		old, ready := virtualRevision(t, f.observe())
		old.Roles["router"] = revisionLWS("old", "router", 5, 5, time.Time{}, 5)
		ready[old.Roles["router"].Name] = replicaReadiness{raw: 5, committed: 5}
		target := disaggutils.RevisionRoles{Revision: "new", Roles: map[string]*leaderv1.LeaderWorkerSet{}}
		for name, lws := range old.Roles {
			target.Roles[name] = lws.DeepCopy()
			target.Roles[name].Name += "-target"
			target.Roles[name].Spec.Replicas = ptr.To[int32](0)
		}
		var inputs rolloutInputs
		for tick := range 40 {
			edit := edits[min(tick, len(edits)-1)]
			children := []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}, {Name: "b"}}
			if edit[0] == 0 {
				children = children[1:]
			} else if edit[1] == 0 {
				children = children[:1]
			}
			f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{Name: "model", SubRoles: children}, {Name: "router"}}
			desired := map[string]int{"model": edit[0] + edit[1], "model/a": edit[0], "model/b": edit[1], "router": 5}
			inputs = buildRolloutInputs(expandSubRoleSpec(f.ds, desired), disaggutils.RevisionRolesList{old}, target, desired)
			require.Len(t, inputs.allRoleNames, 3, "only leaves, including removed children; no parent dimension")
			if tick%3 == 2 {
				// Native writes/readiness are simulated after the initial real
				// observer snapshot; this is an adapter replay, not a live test.
				for _, revision := range []disaggutils.RevisionRoles{old, target} {
					for _, lws := range revision.Roles {
						ready[lws.Name] = replicaReadiness{raw: int(getLWSReplicas(lws)), committed: int(getLWSReplicas(lws))}
					}
				}
			}
			state := rolloutStateForRevision(inputs.allRoleNames, disaggutils.RevisionRolesList{old}, old, target, inputs.targetReplicas, inputs.config, ready)
			state.ScaleDuringRollout = inputs.scaleDuringRollout
			for i, name := range inputs.allRoleNames {
				require.True(t, state.ActiveOld.RequiredRoles[i], "old membership must retain %s", name)
				require.Equal(t, inputs.targetReplicas[i] > 0, state.Target.RequiredRoles[i])
			}
			step := ComputeNextStep(state)
			if step == nil {
				continue
			}
			require.NoError(t, validateUpdateStep(state, step))
			for r, revision := range []disaggutils.RevisionRoles{old, target} {
				counts := [][]int{step.Past, step.New}[r]
				for i, name := range inputs.allRoleNames {
					lws := revision.Roles[name]
					availability := ready[lws.Name]
					availability.committed = max(0, availability.committed-max(0, int(getLWSReplicas(lws))-counts[i]))
					ready[lws.Name] = availability
					lws.Spec.Replicas = ptr.To(int32(counts[i]))
				}
			}
		}
		complete, targetReady := rolloutCompletionStatus(disaggutils.RevisionRolesList{old}, target, inputs.allRoleNames, inputs.targetReplicas, ready, advance)
		require.True(t, complete, "advance=%v", advance)
		require.True(t, targetReady)
	}
}
