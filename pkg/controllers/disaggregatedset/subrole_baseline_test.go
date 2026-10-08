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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

func TestSubRoleBaselineRejectsOverflowWithoutWriting(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "b")
	before := f.observe().LWS
	// Independently clamped child targets can overflow even when the raw
	// desired total fits. Never persist a vector that the next read rejects.
	err := f.manager.syncSubRoleBaseline(t.Context(), before, map[string]int{"a": math.MaxInt32, "b": 1})
	require.Error(t, err)
	require.Equal(t, before.Annotations, f.observe().LWS.Annotations)
}

func TestSubRoleExternalBaselineUsesEffectiveTargetAndFreezesAfterInterruption(t *testing.T) {
	for _, tc := range []struct {
		policy   disaggv1.ScalingDuringRolloutPolicy
		baseline int
	}{
		{disaggv1.ScalingDuringRolloutPolicyRolloutCoupled, 6},
		{disaggv1.ScalingDuringRolloutPolicyAdvanceRollout, 2},
	} {
		t.Run(string(tc.policy), func(t *testing.T) {
			ctx := t.Context()
			a := newNamedSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "old", "pool", "pool")
			b := newNamedSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "target", "pool", "pool", "pool", "pool", "pool", "pool")
			// Put both revisions' real leader/worker topology behind the same reader.
			for _, list := range []client.ObjectList{&leaderv1.LeaderWorkerSetList{}, &appsv1.StatefulSetList{}, &corev1.PodList{}} {
				require.NoError(t, a.manager.client.List(ctx, list))
				objects, err := meta.ExtractList(list)
				require.NoError(t, err)
				for _, object := range objects {
					obj := object.(client.Object)
					obj.SetResourceVersion("")
					require.NoError(t, b.manager.client.Create(ctx, obj))
				}
			}
			a.manager = b.manager
			b.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{
				Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{
					Name: "pool", Scaling: &disaggv1.RoleScaling{Mode: disaggv1.RoleScalingExternal},
				}},
			}}
			b.ds.Spec.ScalingPolicy = &disaggv1.DisaggregatedSetScalingPolicy{DuringRollout: tc.policy}
			for _, fixture := range []*subRoleFixture{a, b} {
				require.NoError(t, fixture.manager.patchSubRoleLWS(ctx, fixture.observe().LWS, func(lws *leaderv1.LeaderWorkerSet) {
					setInitialReplicasAnnotation(lws, 8)
					setSubRoleJSON(lws, disaggv1.InitialSubRoleReplicasAnnotationKey, map[string]int{"pool": 8})
				}))
			}
			physicalRevision := func(name string, fixture *subRoleFixture) disaggutils.RevisionRoles {
				return disaggutils.RevisionRoles{Revision: name, Roles: map[string]*leaderv1.LeaderWorkerSet{"model": fixture.observe().LWS}}
			}
			executor := newTestExecutor(b.manager.client)
			executor.LWSManager = b.manager // Use the real observer, not settled status counters.

			// A8 -> B8 reached A2/B6. External desired drops to 2, but the default
			// rollout policy clamps B's effective target to 6 while A still overlaps.
			_, _, err := executor.reconcileExistingRollout(ctx, b.ds,
				disaggutils.RevisionRolesList{physicalRevision("A", a)}, physicalRevision("B", b),
				map[string]int{"model": 2, "model/pool": 2})
			require.NoError(t, err)
			s := b.observe()
			initial, err := subRoleCounts(s.LWS, disaggv1.InitialSubRoleReplicasAnnotationKey)
			require.NoError(t, err)
			require.Equal(t, map[string]int{"pool": tc.baseline}, initial, "persist the policy's effective target, not the previous 8")
			parentInitial, ok := disaggutils.GetInitialReplicas(s.LWS)
			require.True(t, ok)
			require.EqualValues(t, tc.baseline, parentInitial)
			if tc.policy == disaggv1.ScalingDuringRolloutPolicyAdvanceRollout {
				return // Opt-in intentionally lowers the target; there is no clamp to preserve.
			}

			// C interrupts before the clamp is released. After A finishes and C has
			// one Ready group, B6 can safely drain to 5. Complete that real child plan.
			require.ErrorIs(t, b.manager.scaleSubRoles(ctx, b.ds, s.LWS, map[string]int{"pool": 5}), errReplicaGroupsPending)
			b.sync()   // Publish the prepared physical and logical target.
			b.native() // Acknowledge it and remove the high ordinal.
			b.sync()   // Complete the accepted transaction.
			s = b.observe()
			require.EqualValues(t, 5, getLWSReplicas(s.LWS))
			require.Empty(t, s.LWS.Annotations[subRolePlanAnnotation])
			_, settled, err := b.manager.prepareSubRoleRevisions(ctx, b.ds,
				disaggutils.RevisionRolesList{physicalRevision("B", b)}, disaggutils.RevisionRoles{Revision: "C"},
				map[string]int{"model": 6, "model/pool": 6})
			require.NoError(t, err)
			require.True(t, settled)
			bView, ready := virtualRevision(t, b.observe())
			bView.Revision = "B"
			frozen, _ := observeOldRevision(bView, []string{"model/pool"}, ready)
			require.Equal(t, RoleReplicaState{6}, frozen, "old history must not shrink with B's physical Spec")

			// Replay C's one Ready group. All six Ready groups are required: grow C
			// first, rather than spending a second B replica against a moving floor.
			cRole := &rolloutRole{LWS: &leaderv1.LeaderWorkerSet{ObjectMeta: metav1.ObjectMeta{Name: "next"}}, SubRole: "pool", Replicas: 1, InitialReplicas: 6}
			cView := rolloutRevision{Revision: "C", Roles: map[string]*rolloutRole{"model/pool": cRole}}
			ready[cRole.readinessKey()] = replicaReadiness{raw: 1, committed: 1}
			state := rolloutStateForRevision([]string{"model/pool"}, rolloutRevisionList{bView}, bView, cView,
				RoleReplicaState{6}, []RollingUpdateConfig{{MaxSurge: 1}}, ready)
			require.Equal(t, 6, availabilityFloor(snapshotForRolloutState(state)[0]))
			step := ComputeNextStep(state)
			require.NotNil(t, step)
			require.NoError(t, validateUpdateStep(state, step))
			require.Equal(t, RoleReplicaState{5}, step.Past)
			require.Equal(t, RoleReplicaState{2}, step.New)
		})
	}
}
