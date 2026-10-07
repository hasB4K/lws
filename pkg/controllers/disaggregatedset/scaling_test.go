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
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	disaggregatedsetutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

func TestScalingDuringRolloutDecisions(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		old, oldReady, current, ready, desired []int
		initial                                []int
		surge, unavailable                     []int
		wantOld, wantNew                       []int
		bootstrap                              bool
	}{
		{
			name: "old first", old: []int{4, 4}, oldReady: []int{4, 4},
			current: []int{5, 5}, ready: []int{5, 5}, desired: []int{4, 4},
			wantOld: []int{0, 0}, wantNew: []int{5, 5},
		}, {
			name: "unready target absorbs excess", old: []int{4, 4}, oldReady: []int{4, 4},
			current: []int{5, 5}, ready: []int{0, 0}, desired: []int{4, 4},
			wantOld: []int{4, 4}, wantNew: []int{1, 1},
		}, {
			name:    "exact target after old drains",
			current: []int{5, 3}, ready: []int{5, 3}, desired: []int{4, 2}, wantNew: []int{4, 2},
		}, {
			name:    "a zero target really reaches zero",
			current: []int{2, 3}, ready: []int{2, 3}, desired: []int{0, 0}, wantNew: []int{0, 0},
		}, {
			name:    "first unready role cannot hide a later drain",
			current: []int{1, 5}, ready: []int{0, 4}, desired: []int{1, 4},
		}, {
			name:    "per-role readiness remains independent",
			current: []int{1, 5}, ready: []int{0, 5}, desired: []int{1, 4}, wantNew: []int{1, 4},
		}, {
			name:    "growth creates no readiness credit",
			current: []int{1, 5}, ready: []int{0, 4}, desired: []int{2, 4}, wantNew: []int{2, 5},
		}, {
			name:    "opposing target changes",
			current: []int{4, 8}, ready: []int{4, 8}, desired: []int{8, 4}, wantNew: []int{8, 4},
		}, {
			name: "raised floor preserves old ready", old: []int{4, 4}, oldReady: []int{4, 4},
			current: []int{1, 1}, ready: []int{1, 1}, desired: []int{8, 8},
			wantOld: []int{4, 4}, wantNew: []int{2, 2},
		}, {
			name: "grow while previous batch is unready", old: []int{8, 8}, oldReady: []int{8, 8},
			current: []int{2, 2}, ready: []int{0, 0}, desired: []int{12, 12},
			surge: []int{2, 2}, unavailable: []int{2, 2}, wantOld: []int{8, 8}, wantNew: []int{4, 4},
		}, {
			name: "one emergency coordination slot", initial: []int{2, 3}, old: []int{2, 2}, oldReady: []int{2, 2},
			current: []int{4, 2}, ready: []int{4, 2}, desired: []int{6, 4},
			surge: []int{2, 0}, unavailable: []int{0, 2}, wantOld: []int{2, 2}, wantNew: []int{6, 3}, bootstrap: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.old == nil {
				tc.old, tc.oldReady = []int{0, 0}, []int{0, 0}
			}
			if tc.wantOld == nil {
				tc.wantOld = tc.old
			}
			if tc.surge == nil {
				tc.surge = []int{1, 1}
			}
			if tc.unavailable == nil {
				tc.unavailable = []int{0, 0}
			}
			state := rolloutState(tc.old, tc.old, tc.oldReady, nil, nil, tc.current, tc.ready, tc.desired, configs(tc.surge, tc.unavailable))
			if tc.initial != nil {
				state.ActiveOld.InitialReplicas = tc.initial
			}
			state.ScaleDuringRollout = true
			step := ComputeNextStep(state)
			if tc.wantNew == nil {
				require.Nil(t, step)
				return
			}
			require.NotNil(t, step)
			assert.Equal(t, tc.wantOld, step.Past)
			assert.Equal(t, tc.wantNew, step.New)
			assert.Equal(t, tc.bootstrap, step.UsesBootstrapSurge)
			require.NoError(t, validateUpdateStep(state, step))
			assertScalingSafety(t, state, step)
		})
	}
}

func TestTargetDrainUsesRetainedNotRawReadiness(t *testing.T) {
	state := rolloutState([]int{0, 0}, []int{0, 0}, []int{0, 0}, nil, nil,
		[]int{5, 5}, []int{4, 4}, []int{3, 3}, configs([]int{1, 1}, []int{0, 0}))
	state.Target.RawReadyReplicas = []int{5, 5} // One victim per role is already reserved.
	state.ScaleDuringRollout = true
	step := ComputeNextStep(state)
	require.NotNil(t, step)
	assert.Equal(t, []int{4, 4}, step.New)
	require.NoError(t, validateUpdateStep(state, step))
	require.Error(t, validateUpdateStep(state, &UpdateStep{Past: []int{0, 0}, New: []int{3, 3}}))

	state.ScaleDuringRollout = false
	assert.Nil(t, ComputeNextStep(state), "default remains growth-only")
	require.Error(t, validateUpdateStep(state, step), "the executor independently rejects opt-out target shrink")
}

// This oracle does not call the production availability or validator helpers.
// Treat every newly deleted replica as Ready, remove unusable revisions, then
// check serving capacity and each structurally complete role independently.
func assertScalingSafety(t *testing.T, state RolloutState, step *UpdateStep) {
	t.Helper()
	type revision struct {
		required          []bool
		spec, ready, next []int
	}
	revisions := []revision{
		{state.ActiveOld.RequiredRoles, state.ActiveOld.SpecReplicas, state.ActiveOld.ReadyReplicas, step.Past},
		{state.Target.RequiredRoles, state.Target.SpecReplicas, state.Target.ReadyReplicas, step.New},
	}
	for _, parked := range state.ParkedOld {
		revisions = append(revisions, revision{parked.RequiredRoles, parked.SpecReplicas, parked.ReadyReplicas, parked.SpecReplicas})
	}
	n := len(state.Config)
	beforeServing, afterServing, beforeRole, afterRole := make([]int, n), make([]int, n), make([]int, n), make([]int, n)
	for _, rev := range revisions {
		for phase := range 2 {
			spec, ready := slices.Clone(rev.spec), slices.Clone(rev.ready)
			if phase == 1 {
				for i := range spec {
					spec[i] = min(spec[i], rev.next[i]) // New growth is never Ready yet.
					ready[i] = max(0, ready[i]-(rev.spec[i]-spec[i]))
				}
			}
			present, serving, required := true, true, false
			for i, needed := range rev.required {
				if needed {
					required = true
					present = present && spec[i] > 0
					serving = serving && ready[i] > 0
				}
			}
			for i := range spec {
				if present && required {
					if phase == 0 {
						beforeRole[i] += ready[i]
					} else {
						afterRole[i] += ready[i]
					}
				}
				if serving && required {
					if phase == 0 {
						beforeServing[i] += ready[i]
					} else {
						afterServing[i] += ready[i]
					}
				}
			}
		}
	}
	for i, config := range state.Config {
		floor := max(0, state.Target.DesiredReplicas[i]-config.MaxUnavailable)
		if step.UsesUnavailableFallback && floor > 1 {
			floor--
		}
		require.GreaterOrEqual(t, afterServing[i], min(beforeServing[i], floor), "serving role %d: state=%+v step=%+v", i, state, step)
		// Draining another role cannot remove this role's physical replicas;
		// losing structural credit alone is covered by the serving check above.
		if step.Past[i] < state.ActiveOld.SpecReplicas[i] || step.New[i] < state.Target.SpecReplicas[i] {
			require.GreaterOrEqual(t, afterRole[i], min(beforeRole[i], floor), "per-role %d: state=%+v step=%+v", i, state, step)
		}
		old := state.ActiveOld.SpecReplicas[i]
		for _, parked := range state.ParkedOld {
			old += parked.SpecReplicas[i]
		}
		if step.New[i] > state.Target.SpecReplicas[i] {
			ceiling := state.Target.DesiredReplicas[i] + config.MaxSurge
			if step.UsesBootstrapSurge {
				ceiling = max(ceiling+1, old+1)
			}
			require.LessOrEqual(t, old+step.New[i], ceiling)
		}
	}
}

func TestScalingDuringRolloutIndependentSafety(t *testing.T) {
	rng := rand.New(rand.NewSource(1023))
	for scenario := range 20000 {
		n := 2 + rng.Intn(2)
		initial := make([]int, n)
		for i := range initial {
			initial[i] = 5
		}
		state := rolloutState(initial, make([]int, n), make([]int, n), make([]int, n), make([]int, n),
			make([]int, n), make([]int, n), make([]int, n), make([]RollingUpdateConfig, n))
		state.ScaleDuringRollout = true
		for i := range n {
			state.ActiveOld.SpecReplicas[i] = rng.Intn(6)
			state.ParkedOld[0].SpecReplicas[i] = rng.Intn(4)
			state.Target.SpecReplicas[i] = rng.Intn(8)
			state.Target.DesiredReplicas[i] = rng.Intn(7)
			state.Config[i] = RollingUpdateConfig{MaxSurge: rng.Intn(3), MaxUnavailable: rng.Intn(3)}
			for _, counts := range []struct{ spec, raw, retained []int }{
				{state.ActiveOld.SpecReplicas, state.ActiveOld.RawReadyReplicas, state.ActiveOld.ReadyReplicas},
				{state.ParkedOld[0].SpecReplicas, state.ParkedOld[0].RawReadyReplicas, state.ParkedOld[0].ReadyReplicas},
				{state.Target.SpecReplicas, state.Target.RawReadyReplicas, state.Target.ReadyReplicas},
			} {
				counts.raw[i] = rng.Intn(counts.spec[i] + 1)
				counts.retained[i] = rng.Intn(counts.raw[i] + 1)
			}
		}
		state.Target.RequiredRoles = requiredRoles(state.Target.DesiredReplicas)
		state.ParkedOld[0].RequiredRoles = requiredRoles(initial)
		if step := ComputeNextStep(state); step != nil {
			require.NoError(t, validateUpdateStep(state, step), "scenario %d state=%+v step=%+v", scenario, state, step)
			assertScalingSafety(t, state, step)
		}
	}
}

func TestScalingDuringRolloutDelayedConvergence(t *testing.T) {
	rng := rand.New(rand.NewSource(81259))
	for scenario := range 1000 {
		initial := []int{rng.Intn(6), rng.Intn(6)}
		state := rolloutState(initial, initial, initial, []int{1, 1}, []int{1, 1}, []int{0, 0}, []int{0, 0}, initial,
			configs([]int{rng.Intn(3), rng.Intn(3)}, []int{rng.Intn(3), rng.Intn(3)}))
		for i := range state.Config {
			if state.Config[i].MaxSurge+state.Config[i].MaxUnavailable == 0 {
				state.Config[i].MaxSurge = 1
			}
		}
		state.ScaleDuringRollout = true
		for reconcile := range 120 {
			if reconcile < 4 {
				state.Target.DesiredReplicas = []int{rng.Intn(8), rng.Intn(8)}
				state.Target.RequiredRoles = requiredRoles(state.Target.DesiredReplicas)
			}
			// Native deletes and new readiness catch up separately. Raw Ready
			// stays stale for two reconciles after an issued drain.
			if reconcile%3 == 2 {
				state.ActiveOld.RawReadyReplicas = slices.Clone(state.ActiveOld.SpecReplicas)
				state.ActiveOld.ReadyReplicas = slices.Clone(state.ActiveOld.SpecReplicas)
				state.Target.RawReadyReplicas = slices.Clone(state.Target.SpecReplicas)
				state.Target.ReadyReplicas = slices.Clone(state.Target.SpecReplicas)
			}
			if step := ComputeNextStep(state); step != nil {
				require.NoError(t, validateUpdateStep(state, step), "scenario %d reconcile %d", scenario, reconcile)
				assertScalingSafety(t, state, step)
				for i := range 2 {
					state.ActiveOld.ReadyReplicas[i] = max(0, state.ActiveOld.ReadyReplicas[i]-(state.ActiveOld.SpecReplicas[i]-step.Past[i]))
					state.Target.ReadyReplicas[i] = max(0, state.Target.ReadyReplicas[i]-max(0, state.Target.SpecReplicas[i]-step.New[i]))
				}
				state.ActiveOld.SpecReplicas, state.Target.SpecReplicas = step.Past, step.New
			}
			if slices.Equal(state.ActiveOld.SpecReplicas, []int{0, 0}) && len(state.ParkedOld) > 0 {
				parked := state.ParkedOld[0]
				state.ActiveOld = ActiveRevisionState{RequiredRoles: parked.RequiredRoles, InitialReplicas: []int{1, 1},
					SpecReplicas: parked.SpecReplicas, RawReadyReplicas: parked.RawReadyReplicas, ReadyReplicas: parked.ReadyReplicas}
				state.ParkedOld = nil
			}
		}
		require.Equal(t, []int{0, 0}, state.ActiveOld.SpecReplicas, "scenario %d state=%+v", scenario, state)
		require.Empty(t, state.ParkedOld)
		require.Equal(t, state.Target.DesiredReplicas, state.Target.SpecReplicas, "scenario %d state=%+v", scenario, state)
	}
}

func TestScalingDuringRolloutExecutorPolicies(t *testing.T) {
	for _, policy := range []*disaggregatedsetv1.DisaggregatedSetScalingPolicy{
		nil, {}, {DuringRollout: disaggregatedsetv1.ScalingDuringRolloutPolicyRolloutCoupled},
		{DuringRollout: disaggregatedsetv1.ScalingDuringRolloutPolicyAdvanceRollout},
	} {
		ds := newTwoRoleTestDisaggregatedSet([2]int32{4, 2}, [2]int{1, 1}, [2]int{})
		revision := disaggregatedsetutils.ComputeRevision(ds.Spec.Roles)
		ds.Spec.ScalingPolicy = policy
		assert.Equal(t, revision, disaggregatedsetutils.ComputeRevision(ds.Spec.Roles), "policy edits do not roll Pods")
		objects := revisionLWSObjects("old", [2]int32{}, [2]int32{}, [2]int32{5, 3}, time.Now())
		objects = append(objects, revisionLWSObjects(revision, [2]int32{5, 3}, [2]int32{5, 3}, [2]int32{5, 3}, time.Now())...)
		c := newTestClient(objects...)
		r := newTestReconciler(c)
		_, err := r.reconcileSlice(context.Background(), r.createRollingUpdateExecutor(), ds, 0, revision, resolveDesiredReplicasByRole(ds, nil))
		require.NoError(t, err)
		want := [2]int32{5, 3}
		if scalingDuringRolloutEnabled(ds) {
			want = [2]int32{4, 2}
		}
		assertRevisionReplicas(t, c, revision, want)
	}
}

func TestScalingDuringRolloutPrefersAnyOldDrainOverTargetShrink(t *testing.T) {
	now := time.Now()
	objects := revisionLWSObjects("hashB", [2]int32{1, 1}, [2]int32{1, 0}, [2]int32{1, 1}, now)
	objects = append(objects, revisionLWSObjects("hashA", [2]int32{2, 2}, [2]int32{2, 2}, [2]int32{2, 2}, now.Add(time.Hour))...)
	objects = append(objects, revisionLWSObjects("hashC", [2]int32{3, 3}, [2]int32{}, [2]int32{3, 3}, now.Add(2*time.Hour))...)
	c := newTestClient(objects...)
	ds := newTwoRoleTestDisaggregatedSet([2]int32{2, 2}, [2]int{1, 1}, [2]int{})
	ds.Spec.ScalingPolicy = &disaggregatedsetv1.DisaggregatedSetScalingPolicy{DuringRollout: disaggregatedsetv1.ScalingDuringRolloutPolicyAdvanceRollout}
	reconcileExistingForTest(t, newTestExecutor(c), ds, "hashC")
	assertRevisionReplicas(t, c, "hashA", [2]int32{2, 2})
	assertRevisionReplicas(t, c, "hashB", [2]int32{})
	assertRevisionReplicas(t, c, "hashC", [2]int32{3, 3})
}
