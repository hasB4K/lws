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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	disaggutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

func setCanaryPercent(ds *disaggv1.DisaggregatedSet, percent int32) {
	ds.Spec.RolloutStrategy = &disaggv1.DisaggregatedSetRolloutStrategy{CanaryPercent: ptr.To(percent)}
}

func canaryConfigs(surge, unavailable int, old RoleReplicaState) []RollingUpdateConfig {
	config := make([]RollingUpdateConfig, len(old))
	for i, replicas := range old {
		config[i] = RollingUpdateConfig{MaxSurge: surge, MaxUnavailable: unavailable, CanaryOldReplicas: replicas}
	}
	return config
}

func settleCanary(t *testing.T, state *RolloutState) {
	t.Helper()
	for range 100 {
		step := ComputeNextStep(*state)
		if step == nil {
			return
		}
		require.NoError(t, validateUpdateStep(*state, step))
		for i, desired := range state.Target.DesiredReplicas {
			ready := step.Past[i] + state.Target.ReadyReplicas[i]
			for _, parked := range state.ParkedOld {
				ready += parked.ReadyReplicas[i]
			}
			require.GreaterOrEqual(t, ready, desired-state.Config[i].MaxUnavailable, "role %d availability", i)
		}
		state.ActiveOld.observeFullyReady(step.Past)
		state.Target.observeFullyReady(step.New)
	}
	t.Fatal("canary did not settle")
}

func TestCanaryRoundingAndLogicalRoles(t *testing.T) {
	for _, tc := range [][3]int{{0, 25, 0}, {1, 25, 1}, {2, 1, 1}, {2, 99, 1}, {4, 0, 4}, {4, 25, 3}, {8, 25, 6}, {37, 25, 27}} {
		require.Equal(t, tc[2], canaryOldReplicas(tc[0], tc[1]))
	}
	ds := newDSWithRoles("ds", staticRole("prefill"), disaggv1.DisaggregatedRoleSpec{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}, {Name: "b"}}})
	before := disaggutils.ComputeRevision(ds.Spec.Roles)
	for _, percent := range []int32{0, 25, 50, 99, 100} {
		setCanaryPercent(ds, percent)
		config := extractRollingUpdateConfig(ds, []string{"prefill", "model/a", "model/b"}, map[string]int{"prefill": 4, "model/a": 4, "model/b": 8})
		for i, desired := range []int{4, 4, 8} {
			want := 0
			if percent < 100 {
				want = canaryOldReplicas(desired, int(percent))
			}
			require.Equal(t, want, config[i].CanaryOldReplicas)
		}
		require.Equal(t, before, disaggutils.ComputeRevision(ds.Spec.Roles), "promotion must not restart the revision")
	}
	ds.Spec.RolloutStrategy = nil
	_, active := canaryPercent(ds)
	require.False(t, active)
}

func TestCanaryPlannerPromotesThroughHolds(t *testing.T) {
	for _, budget := range [][2]int{{1, 0}, {0, 1}} {
		state := newSimulationState([]int{4, 8}, []int{4, 8}, canaryConfigs(budget[0], budget[1], []int{0, 0}))
		state.ProportionalCanary = true
		for _, percent := range []int{0, 25, 50, 75, 100} {
			for i, desired := range state.Target.DesiredReplicas {
				state.Config[i].CanaryOldReplicas = 0
				if percent < 100 {
					state.Config[i].CanaryOldReplicas = canaryOldReplicas(desired, percent)
				}
			}
			settleCanary(t, &state)
			for i, desired := range state.Target.DesiredReplicas {
				require.Equal(t, state.Config[i].CanaryOldReplicas, state.ActiveOld.SpecReplicas[i])
				require.Equal(t, desired-state.Config[i].CanaryOldReplicas, state.Target.SpecReplicas[i])
			}
		}
	}
}

func TestCanaryResizesBothRevisions(t *testing.T) {
	for _, identity := range []leaderv1.GroupIdentityType{leaderv1.GroupIdentityOrdinal, leaderv1.GroupIdentityHash} {
		for _, policy := range []disaggv1.RolloutScalingPolicy{disaggv1.RolloutScalingDeferred, disaggv1.RolloutScalingImmediate} {
			t.Run(fmt.Sprintf("%s/%s", identity, policy), func(t *testing.T) {
				ds := newDSWithRoles("test", staticRole("prefill"), staticRole("decode"))
				ds.UID = "uid"
				setCanaryPercent(ds, 25)
				ds.Spec.ScalingPolicy = &disaggv1.DisaggregatedSetScalingPolicy{ScaleUpDuringRollout: policy, ScaleDownDuringRollout: policy}
				objects := []client.Object{ds}
				for i, name := range testRoleNames() {
					ds.Spec.Roles[i].Spec.RolloutStrategy.RollingUpdateConfiguration = &leaderv1.RollingUpdateConfiguration{MaxSurge: intstr.FromInt(1)}
					for _, revision := range []string{"old", "new"} {
						n := int32(0)
						if revision == "old" {
							n = int32(4 * (i + 1))
						}
						lws := revisionLWS(revision, name, n, n, time.Now(), int32(4*(i+1)))
						lws.Spec.GroupIdentity = identity
						lws.Spec.RolloutStrategy.RollingUpdateConfiguration = &leaderv1.RollingUpdateConfiguration{MaxSurge: intstr.FromInt(2)}
						objects = append(objects, lws)
					}
				}
				c := newTestClient(objects...)
				executor := newTestExecutor(c)
				recorder := events.NewFakeRecorder(100)
				executor.Record = recorder
				settledCounts := func(desired map[string]int) (oldCounts, newCounts []int) {
					t.Helper()
					for range 100 {
						old, target, err := executor.LWSManager.GetRevisionRolesList(t.Context(), ds, 0, "new")
						require.NoError(t, err)
						previous := make([]int, 2)
						for i, name := range testRoleNames() {
							previous[i] = old.GetTotalReplicasPerRole(name) + int(getLWSReplicas(target.Roles[name]))
						}
						_, _, err = executor.reconcileExistingRollout(t.Context(), ds, old, *target, desired)
						require.NoError(t, err)
						require.Empty(t, executor.canaryBlocked)
						emitted := ""
						for len(recorder.Events) > 0 {
							emitted += <-recorder.Events
						}
						list := &leaderv1.LeaderWorkerSetList{}
						require.NoError(t, c.List(t.Context(), list))
						changed := false
						oldCounts, newCounts = make([]int, 2), make([]int, 2)
						readyBefore, readyAfter := make([]int, 2), make([]int, 2)
						for i := range list.Items {
							lws := &list.Items[i]
							n := *lws.Spec.Replicas
							role := 0
							if lws.Labels[disaggv1.RoleLabelKey] == "decode" {
								role = 1
							}
							if lws.Labels[disaggv1.RevisionLabelKey] == "old" {
								oldCounts[role] = int(n)
								require.Equal(t, intstr.FromInt(2), lws.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge, "old growth retains old budgets")
								require.Equal(t, fmt.Sprint(4*(role+1)), lws.Annotations[disaggv1.InitialReplicasAnnotationKey])
							} else {
								newCounts[role] = int(n)
							}
							changed = changed || n != lws.Status.ReadyReplicas
							readyBefore[role] += int(lws.Status.ReadyReplicas)
							readyAfter[role] += int(min(n, lws.Status.ReadyReplicas))
							lws.Status.Replicas, lws.Status.ReadyReplicas = n, n
							require.NoError(t, c.Status().Update(t.Context(), lws))
						}
						for i, name := range testRoleNames() {
							require.GreaterOrEqual(t, readyAfter[i], min(readyBefore[i], desired[name]), "scale requests preserve the availability floor before new groups are Ready")
							if oldCounts[i]+newCounts[i] > max(previous[i], desired[name]+1) {
								require.Contains(t, emitted, EventReasonBootstrapSurge, "exceeding ordinary surge must use the existing explicit recovery")
								require.Contains(t, emitted, name)
								require.LessOrEqual(t, oldCounts[i]+newCounts[i], desired[name]+2, "at most one extra bootstrap group")
							}
						}
						if !changed {
							return
						}
					}
					t.Fatal("canary did not settle")
					return
				}
				a, b := settledCounts(map[string]int{"prefill": 4, "decode": 8})
				require.Equal(t, []int{3, 6}, a)
				require.Equal(t, []int{1, 2}, b)
				a, b = settledCounts(map[string]int{"prefill": 8, "decode": 16})
				require.Equal(t, []int{6, 12}, a)
				require.Equal(t, []int{2, 4}, b)
				a, b = settledCounts(map[string]int{"prefill": 4, "decode": 8})
				if policy == disaggv1.RolloutScalingImmediate {
					require.Equal(t, []int{3, 6}, a)
					require.Equal(t, []int{1, 2}, b)
				} else {
					require.Equal(t, []int{2, 4}, a)
					require.Equal(t, []int{2, 4}, b, "Deferred retains started target groups")
				}
				a, b = settledCounts(map[string]int{"prefill": 20, "decode": 8})
				if policy == disaggv1.RolloutScalingImmediate {
					require.Equal(t, []int{15, 6}, a)
					require.Equal(t, []int{5, 2}, b)
				} else {
					require.Equal(t, []int{15, 4}, a)
					require.Equal(t, []int{5, 4}, b)
				}
				setCanaryPercent(ds, 50)
				a, b = settledCounts(map[string]int{"prefill": 20, "decode": 8})
				require.Equal(t, []int{10, 4}, a)
				require.Equal(t, []int{10, 4}, b)
				a, b = settledCounts(map[string]int{"prefill": 16, "decode": 8})
				if policy == disaggv1.RolloutScalingImmediate {
					require.Equal(t, []int{8, 4}, a)
					require.Equal(t, []int{8, 4}, b)
				} else {
					require.Equal(t, []int{6, 4}, a)
					require.Equal(t, []int{10, 4}, b)
				}
				setCanaryPercent(ds, 100)
				a, b = settledCounts(map[string]int{"prefill": 16, "decode": 8})
				require.Equal(t, []int{0, 0}, a)
				require.Equal(t, []int{16, 8}, b)
				setCanaryPercent(ds, 25)
				a, b = settledCounts(map[string]int{"prefill": 24, "decode": 16})
				require.Equal(t, []int{0, 0}, a, "fully drained history is not revived")
				require.Equal(t, []int{24, 16}, b, "without surviving old groups, either policy scales the target fully")
			})
		}
	}
}

func TestCanaryRestrictionsAndNoRewind(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		percent, desired, old, new int
		oldName, message           string
	}{
		{"lowering percentage keeps issued target groups", 25, 8, 4, 4, "prefill", ""},
		{"zero pauses without undoing progress", 0, 8, 4, 4, "prefill", ""},
		{"do not revive drained old history", 25, 8, 0, 4, "prefill", ""},
		{"single group cannot be split", 25, 1, 1, 0, "prefill", "single replica group"},
		{"zero holds a single old group", 0, 1, 1, 0, "prefill", ""},
		{"layout change is explicit", 25, 4, 4, 0, "previous", "same active roles"},
		{"promotion permits layout change", 100, 4, 4, 0, "previous", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := newDSWithRoles("test", staticRole("prefill"))
			ds.UID = "uid"
			setCanaryPercent(ds, int32(tc.percent))
			a := revisionLWS("old", tc.oldName, int32(tc.old), int32(tc.old), time.Now(), int32(tc.old))
			b := revisionLWS("new", "prefill", int32(tc.new), int32(tc.new), time.Now(), int32(tc.desired))
			old := disaggutils.RevisionRolesList{{Revision: "old", Roles: map[string]*leaderv1.LeaderWorkerSet{tc.oldName: a}}}
			target := disaggutils.RevisionRoles{Revision: "new", Roles: map[string]*leaderv1.LeaderWorkerSet{"prefill": b}}
			c := newTestClient(ds, a, b)
			executor := newTestExecutor(c)
			desired := map[string]int{"prefill": tc.desired}
			inputs, err := executor.prepareRolloutInputs(t.Context(), ds, old, target, desired)
			require.NoError(t, err)
			handled, err := executor.reconcileCanaryScaleUp(t.Context(), ds, old, target, desired, inputs)
			require.NoError(t, err)
			require.Equal(t, tc.message != "", handled)
			if tc.message != "" {
				require.Contains(t, executor.canaryBlocked, tc.message)
				r := newTestReconciler(c)
				require.NoError(t, r.updateStatus(t.Context(), ds, []string{"prefill"}, "new", desired, nil, false, executor.canaryBlocked))
				require.Equal(t, "CanaryBlocked", meta.FindStatusCondition(ds.Status.Conditions, "Progressing").Reason)
			}
			stored := &leaderv1.LeaderWorkerSet{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(a), stored))
			require.EqualValues(t, tc.old, *stored.Spec.Replicas)
		})
	}
}

func TestCanarySubRolesScaleSurvivingOldGroups(t *testing.T) {
	for _, identity := range []leaderv1.GroupIdentityType{leaderv1.GroupIdentityOrdinal, leaderv1.GroupIdentityHash} {
		t.Run(string(identity), func(t *testing.T) {
			f := newSubRoleFixtureForIdentity(t, identity, 0, "a", "b")
			old := f.withRevision("old", "a", "a", "a", "b", "b", "b")
			f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}, {Name: "b"}}}}
			setCanaryPercent(f.ds, 25)
			require.NoError(t, f.manager.client.Update(t.Context(), f.ds))
			initial := old.lws.Annotations[disaggv1.InitialSubRoleReplicasAnnotationKey]
			executor := &RollingUpdateExecutor{LWSManager: f.manager, Record: events.NewFakeRecorder(100)}
			for _, phase := range []struct {
				percent           int32
				desired, old, new [2]int
			}{
				{25, [2]int{8, 8}, [2]int{6, 6}, [2]int{2, 2}},
				{25, [2]int{20, 8}, [2]int{15, 6}, [2]int{5, 2}},
				{50, [2]int{20, 8}, [2]int{10, 4}, [2]int{10, 4}},
				{50, [2]int{16, 8}, [2]int{6, 4}, [2]int{10, 4}},
				{100, [2]int{16, 8}, [2]int{0, 0}, [2]int{16, 8}},
			} {
				setCanaryPercent(f.ds, phase.percent)
				desired := map[string]int{"model": phase.desired[0] + phase.desired[1], "model/a": phase.desired[0], "model/b": phase.desired[1]}
				for range 80 {
					_, _, err := executor.reconcileExistingRollout(t.Context(), f.ds, disaggutils.RevisionRolesList{old.revision("old")}, f.revision("model"), desired)
					require.NoError(t, err)
					require.Empty(t, executor.canaryBlocked)
					settled := true
					for index, fixture := range []*subRoleFixture{old, f} {
						fixture.native()
						fixture.sync()
						observed := fixture.observe()
						counts, err := subRoleCounts(observed.LWS, subRoleReplicasAnnotation)
						require.NoError(t, err)
						actual := [2]int{}
						for _, group := range activeSubRoleGroups(observed) {
							child, coherent := groupSubRole(group)
							settled = settled && coherent
							if child == "a" {
								actual[0]++
							} else if child == "b" {
								actual[1]++
							}
							for _, pod := range group.Pods {
								pod.Status.Phase = corev1.PodRunning
								pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
								require.NoError(t, f.manager.client.Status().Update(t.Context(), pod))
							}
						}
						want := [2][2]int{phase.old, phase.new}[index]
						settled = settled && actual == want && counts["a"] == want[0] && counts["b"] == want[1] && len(activeSubRoleGroups(observed)) == want[0]+want[1]
					}
					if settled {
						break
					}
				}
				for _, tc := range []struct {
					fixture *subRoleFixture
					count   [2]int
				}{{old, phase.old}, {f, phase.new}} {
					observed := tc.fixture.observe()
					counts, err := subRoleCounts(observed.LWS, subRoleReplicasAnnotation)
					require.NoError(t, err)
					require.Equal(t, map[string]int{"a": tc.count[0], "b": tc.count[1]}, counts, "percent=%d", phase.percent)
					actual := map[string]int{"a": 0, "b": 0}
					for _, group := range activeSubRoleGroups(observed) {
						child, coherent := groupSubRole(group)
						require.True(t, coherent)
						actual[child]++
					}
					require.Equal(t, counts, actual, "percent=%d desired=%v revision=%s", phase.percent, phase.desired, tc.fixture.lws.Name)
				}
			}
			require.Equal(t, initial, old.observe().LWS.Annotations[disaggv1.InitialSubRoleReplicasAnnotationKey])
		})
	}
}

func TestCanaryInitialCreationIgnoresHold(t *testing.T) {
	ds := newDSWithRoles("ds", staticRole("prefill"))
	ds.Spec.Roles[0].Spec.Replicas = ptr.To[int32](4)
	setCanaryPercent(ds, 0)
	ds.Annotations = map[string]string{disaggv1.RevisionHashVersionAnnotationKey: disaggv1.RevisionHashVersion}
	c := newTestClient(ds)
	r := newTestReconciler(c)
	for range 4 {
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ds)})
		require.NoError(t, err)
	}
	list := &leaderv1.LeaderWorkerSetList{}
	require.NoError(t, c.List(t.Context(), list))
	require.Len(t, list.Items, 1)
	require.EqualValues(t, 4, *list.Items[0].Spec.Replicas)
}

func TestCanaryStableGrowthCountsEveryOldRevision(t *testing.T) {
	ds := newDSWithRoles("test", staticRole("prefill"))
	ds.UID = "uid"
	setCanaryPercent(ds, 25)
	desired := map[string]int{"prefill": 8}
	old := disaggutils.RevisionRolesList{}
	objects := []client.Object{ds}
	for i, name := range []string{"stable", "previous-canary", "current"} {
		n := int32(1)
		if i == 0 {
			n = 2
		}
		lws := revisionLWS(name, "prefill", n, n, time.Unix(int64(i), 0), 4)
		objects = append(objects, lws)
		old = append(old, disaggutils.RevisionRoles{Revision: name, Roles: map[string]*leaderv1.LeaderWorkerSet{"prefill": lws}})
	}
	target, old := old[2], old[:2]
	c := newTestClient(objects...)
	executor := newTestExecutor(c)
	inputs, err := executor.prepareRolloutInputs(t.Context(), ds, old, target, desired)
	require.NoError(t, err)
	handled, err := executor.reconcileCanaryScaleUp(t.Context(), ds, old, target, desired, inputs)
	require.NoError(t, err)
	require.True(t, handled)
	for i, want := range []int32{5, 1} {
		stored := &leaderv1.LeaderWorkerSet{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(old[i].Roles["prefill"]), stored))
		require.Equal(t, want, *stored.Spec.Replicas, "replenish only the oldest survivor, counting the other old revision")
	}
}

func TestCanaryIndependentRoleScaleUpCanPromote(t *testing.T) {
	// At 25%, only Prefill scaled from 4 to 20 while Decode stayed at 8.
	// Stable growth left 15/6 old and 5/2 new. Now promote to 50%.
	for _, ordinary := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordinary=%t", ordinary), func(t *testing.T) {
			state := rolloutState([]int{4, 8}, []int{15, 6}, []int{15, 6}, nil, nil, []int{5, 2}, []int{5, 2}, []int{20, 8}, canaryConfigs(1, 0, []int{10, 4}))
			state.ProportionalCanary = true
			for range 100 {
				if ordinary {
					// Ordinary observations retain at least the current Spec as baseline.
					state.ActiveOld.InitialReplicas = []int{max(4, state.ActiveOld.SpecReplicas[0]), max(8, state.ActiveOld.SpecReplicas[1])}
				}
				step := ComputeNextStep(state)
				if step == nil {
					break
				}
				require.NoError(t, validateUpdateStep(state, step))
				state.ActiveOld.observeFullyReady(step.Past)
				state.Target.observeFullyReady(step.New)
			}
			require.Equal(t, []int{10, 4}, state.ActiveOld.SpecReplicas)
			require.Equal(t, []int{10, 4}, state.Target.SpecReplicas)
		})
	}
}

func TestCanaryCoordinatesAllOldRevisions(t *testing.T) {
	desired := []int{20, 8}
	old := [][]int{{12, 2}, {3, 4}}
	target := []int{5, 2}
	for range 100 {
		var selected *UpdateStep
		var selectedState RolloutState
		selectedIndex := 0
		for active := range old {
			parked := old[1-active]
			state := rolloutState([]int{4, 8}, old[active], old[active], parked, parked, target, target, desired, canaryConfigs(1, 0, []int{10, 4}))
			state.ProportionalCanary = true
			step := ComputeNextStep(state)
			if step == nil {
				continue
			}
			// Match executor priority: any ordinary candidate beats bootstrap surge.
			if selected == nil || step.Kind != BootstrapSurgeStep {
				selected, selectedState, selectedIndex = step, state, active
			}
			if step.Kind != BootstrapSurgeStep {
				break
			}
		}
		if selected == nil {
			break
		}
		require.NoError(t, validateUpdateStep(selectedState, selected))
		bounds, _, err := prepareRolloutPlan(selectedState).boundsForStep(selected)
		require.NoError(t, err)
		for i, d := range desired {
			parked := old[1-selectedIndex][i]
			require.GreaterOrEqual(t, selected.Past[i]+parked+target[i], d, "no new readiness is spent before it exists")
			require.LessOrEqual(t, selected.Past[i]+parked+selected.New[i], max(old[selectedIndex][i]+parked+target[i], bounds.surgeCeiling(i)))
		}
		old[selectedIndex], target = selected.Past, selected.New
	}
	require.Equal(t, []int{10, 4}, target)
	require.Equal(t, []int{10, 4}, []int{old[0][0] + old[1][0], old[0][1] + old[1][1]})
}

func TestCanaryHoldRequiresEveryGroupAndSlice(t *testing.T) {
	for _, tc := range []struct {
		oldReady, targetSpec, targetReady RoleReplicaState
		held                              bool
	}{
		{[]int{3, 6}, []int{1, 2}, []int{1, 2}, true},
		{[]int{3, 6}, []int{1, 2}, []int{1, 1}, false},
		{[]int{3, 5}, []int{1, 2}, []int{1, 2}, false},
		{[]int{3, 6}, []int{0, 2}, []int{0, 2}, false},
		{[]int{3, 6}, []int{2, 2}, []int{2, 2}, false},
	} {
		state := rolloutState([]int{4, 8}, []int{3, 6}, tc.oldReady, nil, nil, tc.targetSpec, tc.targetReady, []int{4, 8}, canaryConfigs(1, 0, []int{3, 6}))
		require.Equal(t, tc.held, heldAtCanary(state))
	}
	executor := &RollingUpdateExecutor{rollingSlices: 2, heldSlices: 1}
	require.False(t, executor.CanaryHeld())
	executor.heldSlices = 2
	require.True(t, executor.CanaryHeld())
	require.False(t, (&RollingUpdateExecutor{}).CanaryHeld())
}

func TestCanaryHoldCondition(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		percent                  int32
		held, available          bool
		replicas, ready, updated int32
		reason                   string
	}{
		{"ready hold", 25, true, false, 8, 8, 2, reasonCanaryHeld},
		{"zero hold across two slices", 0, true, false, 8, 8, 0, reasonCanaryHeld},
		{"counts alone are not a hold", 25, false, false, 8, 8, 2, "RolloutInProgress"},
		{"surge is still draining", 25, true, false, 9, 8, 2, "RolloutInProgress"},
		{"group is not ready", 25, true, false, 8, 7, 2, "RolloutInProgress"},
		{"no global hold", 100, true, false, 8, 8, 2, "RolloutInProgress"},
		{"completed rollout", 25, true, true, 8, 8, 8, "AllRolesReady"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := newDSWithRoles("ds", staticRole("prefill"))
			ds.Spec.Slices = ptr.To[int32](2)
			setCanaryPercent(ds, tc.percent)
			statuses := []disaggv1.RoleStatus{{Name: "prefill", DesiredReplicas: ptr.To[int32](8), Replicas: tc.replicas, ReadyReplicas: tc.ready, UpdatedReplicas: tc.updated}}
			condition := disaggregatedSetCondition(ds, statuses, tc.available, nil, tc.held)
			require.Equal(t, tc.reason, condition.Reason)
			if tc.reason == reasonCanaryHeld {
				require.Contains(t, condition.Message, fmt.Sprintf("canaryPercent %d", tc.percent))
				require.Contains(t, condition.Message, fmt.Sprintf("prefill %d/8", tc.updated))
			}
		})
	}
}

func TestCanaryLoweringPercentDuringSurgeAndRollback(t *testing.T) {
	for _, immediate := range []bool{false, true} {
		// Lowering to25% during a surge does not rewind progress unless Immediate
		// can remove excess above desired. Either way the total returns to four.
		state := rolloutState([]int{4, 4}, []int{3, 3}, []int{3, 3}, nil, nil, []int{2, 2}, []int{2, 2}, []int{4, 4}, canaryConfigs(1, 0, []int{3, 3}))
		state.ScaleDownDuringRollout, state.ProportionalCanary = immediate, true
		settleCanary(t, &state)
		if immediate {
			require.Equal(t, []int{3, 3}, state.ActiveOld.SpecReplicas)
			require.Equal(t, []int{1, 1}, state.Target.SpecReplicas)
		} else {
			require.Equal(t, []int{2, 2}, state.ActiveOld.SpecReplicas)
			require.Equal(t, []int{2, 2}, state.Target.SpecReplicas)
		}
	}
	// Restore the original template: its three groups are target again. The
	// canary's one group remains old until an explicit full promotion.
	state := rolloutState([]int{1, 1}, []int{1, 1}, []int{1, 1}, nil, nil, []int{3, 3}, []int{3, 3}, []int{4, 4}, canaryConfigs(1, 0, []int{3, 3}))
	state.ProportionalCanary = true
	settleCanary(t, &state)
	require.Equal(t, []int{1, 1}, state.ActiveOld.SpecReplicas)
	state.Config, state.ProportionalCanary = canaryConfigs(1, 0, []int{0, 0}), false
	settleCanary(t, &state)
	require.Equal(t, []int{0, 0}, state.ActiveOld.SpecReplicas)
	require.Equal(t, []int{4, 4}, state.Target.SpecReplicas)
}
