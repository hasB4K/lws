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
	"fmt"
	"maps"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
)

type subRoleFixture struct {
	t       *testing.T
	manager *LeaderWorkerSetManager
	ds      *disaggv1.DisaggregatedSet
	lws     *leaderv1.LeaderWorkerSet
}

func newSubRoleFixture(t *testing.T, identity leaderv1.GroupIdentityType, assignments ...string) *subRoleFixture {
	return newNamedSubRoleFixture(t, identity, "model", assignments...)
}

func newNamedSubRoleFixture(t *testing.T, identity leaderv1.GroupIdentityType, name string, assignments ...string) *subRoleFixture {
	t.Helper()
	require.Equal(t, leaderv1.GroupIdentityOrdinal, identity)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, leaderv1.AddToScheme(scheme))
	require.NoError(t, disaggv1.AddToScheme(scheme))
	f := &subRoleFixture{t: t, ds: &disaggv1.DisaggregatedSet{ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "test", UID: "ds-uid"}}}
	metadata := func(objectName string, owner client.Object, kind schema.GroupVersionKind) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: objectName, Namespace: "test", UID: types.UID(objectName + "-uid"), Generation: 1, Labels: map[string]string{leaderv1.SetNameLabelKey: name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, kind)}}
	}
	n := int32(len(assignments))
	f.lws = &leaderv1.LeaderWorkerSet{ObjectMeta: metadata(name, f.ds, disaggv1.GroupVersion.WithKind("DisaggregatedSet")), Spec: leaderv1.LeaderWorkerSetSpec{GroupIdentity: identity, Replicas: &n, LeaderWorkerTemplate: leaderv1.LeaderWorkerTemplate{Size: ptr.To[int32](2)}}, Status: leaderv1.LeaderWorkerSetStatus{ObservedGeneration: 1}}
	counts := map[string]int{}
	for _, child := range assignments {
		counts[child]++
	}
	setSubRoleJSON(f.lws, subRoleReplicasAnnotation, counts)
	setSubRoleJSON(f.lws, disaggv1.InitialSubRoleReplicasAnnotationKey, counts)
	objects := []client.Object{f.ds, f.lws}
	owner := &appsv1.StatefulSet{ObjectMeta: metadata(name, f.lws, leaderv1.GroupVersion.WithKind("LeaderWorkerSet")), Spec: appsv1.StatefulSetSpec{Replicas: &n}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, Replicas: n}}
	kind := appsv1.SchemeGroupVersion.WithKind("StatefulSet")
	objects = append(objects, owner)
	ready := corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	for i, child := range assignments {
		leader := &corev1.Pod{ObjectMeta: metadata(fmt.Sprintf("%s-%d", name, i), owner, kind), Status: ready}
		leader.Labels[leaderv1.WorkerIndexLabelKey] = "0"
		leader.Labels[disaggv1.SubRoleLabelKey] = child
		leader.Annotations = map[string]string{leaderv1.SizeAnnotationKey: "2"}
		workers := &appsv1.StatefulSet{ObjectMeta: metadata(leader.Name, leader, corev1.SchemeGroupVersion.WithKind("Pod")), Spec: appsv1.StatefulSetSpec{Replicas: ptr.To[int32](1), Ordinals: &appsv1.StatefulSetOrdinals{Start: 1}}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, AvailableReplicas: 1, CurrentRevision: "a", UpdateRevision: "a"}}
		workers.UID = types.UID(leader.Name + "-workers")
		worker := &corev1.Pod{ObjectMeta: metadata(leader.Name+"-1", workers, appsv1.SchemeGroupVersion.WithKind("StatefulSet")), Status: ready}
		worker.Labels[leaderv1.WorkerIndexLabelKey] = "1"
		worker.Labels[disaggv1.SubRoleLabelKey] = child
		objects = append(objects, leader, workers, worker)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&leaderv1.LeaderWorkerSet{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}, &appsv1.ReplicaSet{}, &corev1.Pod{}).Build()
	f.manager = NewLeaderWorkerSetManager(c)
	return f
}

func (f *subRoleFixture) observe() *replicagroups.Snapshot {
	f.t.Helper()
	s, err := replicagroups.Observe(context.Background(), f.manager.apiReader, f.lws)
	require.NoError(f.t, err)
	require.NotNil(f.t, s)
	f.lws = s.LWS
	return s
}

func (f *subRoleFixture) sync() {
	f.t.Helper()
	_, _, err := f.manager.syncSubRoleGroups(context.Background(), f.observe().LWS)
	require.NoError(f.t, err)
}

// Simulate native StatefulSet target acknowledgement and high-suffix deletion.
func (f *subRoleFixture) native() {
	f.t.Helper()
	ctx := context.Background()
	s := f.observe()
	n := getLWSReplicas(s.LWS)
	sts := s.LeaderStatefulSet
	sts.Spec.Replicas = ptr.To(n)
	require.NoError(f.t, f.manager.client.Update(ctx, sts))
	for _, g := range s.Groups {
		if g.Ordinal >= int(n) {
			require.NoError(f.t, f.manager.client.Delete(ctx, g.Leader))
		}
	}
	s = f.observe()
	s.LeaderStatefulSet.Status.Replicas = int32(len(s.Groups))
	require.NoError(f.t, f.manager.client.Status().Update(ctx, s.LeaderStatefulSet))
}

func TestSubRoleAllocationPassOrder(t *testing.T) {
	type group struct {
		child string
		ready bool
	}
	for _, tc := range []struct {
		name          string
		groups        []group
		counts, floor map[string]int
		want          map[types.UID]string
	}{
		{
			name:   "Ready labels are kept before filling Ready slots",
			groups: []group{{"", true}, {"a", true}, {"b", true}},
			counts: map[string]int{"a": 1, "b": 1}, floor: map[string]int{"a": 1, "b": 1},
			want: map[types.UID]string{"1": "a", "2": "b"},
		},
		{
			name:   "Ready reservation takes priority over count-only stickiness",
			groups: []group{{"a", false}, {"b", true}, {"b", true}},
			counts: map[string]int{"a": 1, "b": 1}, floor: map[string]int{"a": 1, "b": 1},
			want: map[types.UID]string{"1": "b", "2": "a"},
		},
		{
			name:   "count-only labels are kept before new assignments",
			groups: []group{{"", false}, {"b", false}},
			counts: map[string]int{"a": 1, "b": 1},
			want:   map[types.UID]string{"0": "a", "1": "b"},
		},
		{
			name:   "Ready reservations consume their share of the total count",
			groups: []group{{"b", true}, {"b", false}, {"a", true}},
			counts: map[string]int{"a": 2, "b": 1}, floor: map[string]int{"a": 2},
			want: map[types.UID]string{"0": "a", "1": "b", "2": "a"},
		},
		{
			name:   "an unmet Ready floor is infeasible",
			groups: []group{{"a", false}, {"b", true}},
			counts: map[string]int{"a": 1, "b": 1}, floor: map[string]int{"a": 1, "b": 1},
		},
		{
			name:   "a Ready floor cannot exceed the child count",
			groups: []group{{"a", true}, {"a", true}},
			counts: map[string]int{"a": 1}, floor: map[string]int{"a": 2},
		},
		{
			name:   "zero target has an empty valid assignment",
			groups: []group{{"a", true}}, counts: map[string]int{"a": 0},
			want: map[types.UID]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var groups []replicagroups.Group
			for i, input := range tc.groups {
				leader := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(fmt.Sprint(i)), Labels: map[string]string{disaggv1.SubRoleLabelKey: input.child},
				}}
				groups = append(groups, replicagroups.Group{Leader: leader, Ready: input.ready})
			}
			counts, floor := maps.Clone(tc.counts), maps.Clone(tc.floor)
			require.Equal(t, tc.want, allocateSubRoleReadiness(groups, counts, floor))
			require.Equal(t, tc.counts, counts, "allocation must not mutate accepted counts")
			require.Equal(t, tc.floor, floor, "allocation must not mutate the persisted Ready floor")
		})
	}
}

func TestSubRoleOrdinalKeepsChildReadyOnActualRetainedPrefix(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "a", "a", "a", "b")
	s := f.observe()
	s.Groups[0].Ready = false
	current := map[string]int{"a": 5, "b": 1}
	plan := newSubRolePlan(s, current, map[string]int{"a": 4, "b": 1})
	require.NotNil(t, plan)
	require.Equal(t, map[string]int{"a": 3, "b": 1}, plan.ReadyFloor)
	assignments := subRolePlanAssignments(s, plan)
	require.Equal(t, "a", assignments[s.Groups[0].Leader.UID], "unready low ordinal cannot replace Ready child b")
	ready := map[string]int{}
	for _, group := range s.Groups {
		if child, assigned := assignments[group.Leader.UID]; assigned && retainedSubRoleEligible(group) {
			ready[child]++
		}
	}
	require.Equal(t, plan.ReadyFloor, ready)
	require.NotContains(t, assignments, s.Groups[5].Leader.UID)
	for _, change := range []func(*replicagroups.Group){
		func(g *replicagroups.Group) {
			g.Leader.Annotations = map[string]string{leaderv1.GroupRestartBudgetExhaustedAnnotationKey: "true"}
		},
		func(g *replicagroups.Group) { g.WorkerStatefulSet.Generation++ },
		func(g *replicagroups.Group) { g.Terminating = true },
	} {
		s = f.observe()
		s.Groups[0].Ready = false
		change(&s.Groups[1])
		require.Nil(t, newSubRolePlan(s, current, map[string]int{"a": 4, "b": 1}), "three eligible Ready retained groups cannot preserve the four-Ready floor")
	}
}

func TestSubRoleOrdinalOccupiedSlotsDoNotSupplyReadyCredit(t *testing.T) {
	for _, failedOrdinal := range []int{0, 1} {
		t.Run(fmt.Sprint(failedOrdinal), func(t *testing.T) {
			f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "cold", "hot")
			ctx := context.Background()
			leader := f.observe().Groups[failedOrdinal].Leader
			leader.Finalizers = []string{"test/retain-terminated-group"}
			require.NoError(t, f.manager.client.Update(ctx, leader))
			require.NoError(t, f.manager.client.Delete(ctx, leader))
			s := f.observe()
			require.True(t, subRoleNativeSettled(s), "a terminating Pod still occupies its exact ordinal")
			counts := map[string]int{"cold": 1, "hot": 0}
			if failedOrdinal == 0 {
				counts = map[string]int{"cold": 0, "hot": 1}
				require.Nil(t, newSubRolePlan(s, map[string]int{"cold": 1, "hot": 1}, counts), "the doomed retained prefix cannot replace the Ready high suffix")
				return
			}
			require.ErrorIs(t, f.manager.scaleSubRoles(ctx, f.ds, s.LWS, counts), errReplicaGroupsPending)
			for range 3 {
				f.sync()
			}
			s = f.observe()
			require.EqualValues(t, 1, getLWSReplicas(s.LWS), "draining the already-doomed high ordinal must not wait for recovery")
			require.True(t, s.Groups[0].Ready)
		})
	}
}

func TestSubRoleOrdinalOnlyRequiresItsRetainedPrefix(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "cold", "hot")
	s := f.observe()
	// Native still reports two Pods while replacing a hole at ordinal 1.
	// Only ordinal 0 will survive this drain; an unrelated high Pod cannot
	// invalidate its known Ready floor or force us to await a new ordinal 1.
	s.Groups[1].Ordinal = 2
	current := map[string]int{"cold": 1, "hot": 1}
	plan := newSubRolePlan(s, current, map[string]int{"cold": 1, "hot": 0})
	require.NotNil(t, plan)
	require.NoError(t, f.manager.reconcileSubRolePlan(t.Context(), s, plan))
	require.EqualValues(t, 1, getLWSReplicas(f.observe().LWS))
	s.Groups = s.Groups[1:]
	require.Nil(t, newSubRolePlan(s, current, map[string]int{"cold": 0, "hot": 1}), "an actual retained-prefix hole cannot meet its child's Ready floor")
}

func TestSubRoleTransactionsAndRemoval(t *testing.T) {
	for _, desired := range []map[string]int{{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"": 3}} {
		t.Run(fmt.Sprint(desired), func(t *testing.T) {
			f := newNamedSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "ds-0-current-model", "a", "a", "b")
			ctx := context.Background()
			_, removing := desired[""]
			request := func() error { return f.manager.scaleSubRoles(ctx, f.ds, f.observe().LWS, desired) }
			if removing {
				reconciler := &DisaggregatedSetReconciler{LWSManager: f.manager}
				request = func() error {
					return reconciler.reconcileCurrentRevisionRole(ctx, f.ds, 0, "model", &disaggv1.DisaggregatedRoleSpec{Name: "model"}, "current", map[string]int{"model": 3})
				}
				// Routing cleanup does not require healthy groups or native
				// acknowledgement, and must not release lifecycle finalizers.
				leader := f.observe().Groups[2].Leader
				leader.Finalizers = []string{"test/retain-terminated-group"}
				require.NoError(t, f.manager.client.Update(ctx, leader))
				require.NoError(t, f.manager.client.Delete(ctx, leader))
				f.lws.Status.ObservedGeneration = 0
				require.NoError(t, f.manager.client.Status().Update(ctx, f.lws))
			}
			err := request()
			require.ErrorIs(t, err, errReplicaGroupsPending, "initial writes must be observed before preparation is settled")
			accepted, err := readSubRolePlan(f.observe().LWS)
			require.NoError(t, err)
			require.Equal(t, !removing, accepted != nil, "metadata cleanup needs no scaling plan")
			// A new target cannot rewrite an already accepted transaction.
			require.ErrorIs(t, f.manager.scaleSubRoles(ctx, f.ds, f.observe().LWS, map[string]int{"b": 3}), errReplicaGroupsPending)
			for attempt := 0; attempt < 8; attempt++ {
				if removing {
					err := request()
					require.True(t, err == nil || errors.Is(err, errReplicaGroupsPending), "%v", err)
				} else {
					f.sync()
				}
				f.native()
			}
			s := f.observe()
			require.Empty(t, s.LWS.Annotations[subRolePlanAnnotation])
			actual := map[string]int{}
			for _, g := range s.Groups {
				name, coherent := groupSubRole(g)
				require.True(t, coherent)
				actual[name]++
			}
			require.Equal(t, desired, actual)
			if removing {
				require.EqualValues(t, 3, getLWSReplicas(s.LWS))
				require.Equal(t, []string{"test/retain-terminated-group"}, s.Groups[2].Leader.Finalizers)
				require.Empty(t, s.LWS.Annotations[subRoleReplicasAnnotation])
				require.Empty(t, s.LWS.Annotations[disaggv1.InitialSubRoleReplicasAnnotationKey])
				changed, err := f.manager.initializeSubRoles(ctx, s.LWS, map[string]int{"a": 1, "b": 2})
				require.NoError(t, err)
				require.True(t, changed)
				for range 3 {
					f.sync()
				}
				for _, g := range f.observe().Groups {
					name, coherent := groupSubRole(g)
					require.True(t, coherent)
					require.NotEmpty(t, name)
				}
			}
		})
	}
}

func TestSubRolePlanSurvivesPartialLabelAndSpecWriteFailures(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	require.ErrorIs(t, f.manager.scaleSubRoles(context.Background(), f.ds, f.observe().LWS, map[string]int{"a": 1, "b": 1}), errReplicaGroupsPending)
	base := f.manager.client
	failLeader, failSpec := true, true
	f.manager.client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if pod, ok := obj.(*corev1.Pod); ok && pod.Labels[leaderv1.WorkerIndexLabelKey] == "0" && failLeader {
			failLeader = false
			return errors.New("leader patch failed")
		}
		if lws, ok := obj.(*leaderv1.LeaderWorkerSet); ok && getLWSReplicas(lws) == 2 && failSpec {
			failSpec = false
			return errors.New("Spec patch failed")
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	_, _, err := f.manager.syncSubRoleGroups(context.Background(), f.observe().LWS)
	require.ErrorContains(t, err, "leader patch failed")
	_, coherent := groupSubRole(f.observe().Groups[1])
	require.False(t, coherent)
	for attempt := 0; attempt < 8; attempt++ {
		_, _, err = f.manager.syncSubRoleGroups(context.Background(), f.observe().LWS)
		if err != nil {
			require.ErrorContains(t, err, "Spec patch failed")
			require.EqualValues(t, 3, getLWSReplicas(f.observe().LWS))
		}
		f.native()
	}
	require.Empty(t, f.observe().LWS.Annotations[subRolePlanAnnotation])
	require.False(t, failSpec)
}

func TestSubRoleScaleRejectsStaleLogicalSpec(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	observed := f.observe().LWS.DeepCopy()
	require.NoError(t, f.manager.patchSubRoleLWS(context.Background(), f.lws, func(l *leaderv1.LeaderWorkerSet) {
		setSubRoleJSON(l, subRoleReplicasAnnotation, map[string]int{"a": 1, "b": 2})
	}))
	require.ErrorIs(t, f.manager.scaleSubRoles(context.Background(), f.ds, observed, map[string]int{"a": 1, "b": 1}), errReplicaGroupsPending)
	require.Empty(t, f.observe().LWS.Annotations[subRolePlanAnnotation])
	counts, err := subRoleCounts(f.lws, subRoleReplicasAnnotation)
	require.NoError(t, err)
	require.True(t, maps.Equal(counts, map[string]int{"a": 1, "b": 2}))
}

func TestSubRoleCorruptIntentFailsClosed(t *testing.T) {
	for _, counts := range []map[string]int{{"a": -1}, {"a": math.MaxInt32, "b": 1}} {
		lws := &leaderv1.LeaderWorkerSet{}
		setSubRoleJSON(lws, subRoleReplicasAnnotation, counts)
		_, err := subRoleCounts(lws, subRoleReplicasAnnotation)
		require.Error(t, err)
	}
	for _, change := range []func(*subRolePlan){
		func(p *subRolePlan) { p.ReadyFloor["a"] = 2 },
		func(p *subRolePlan) { p.ReadyFloor = nil },
		func(p *subRolePlan) { p.Counts["a"] = -1 },
	} {
		p := &subRolePlan{Counts: map[string]int{"a": 1}, ReadyFloor: map[string]int{"a": 1}}
		change(p)
		lws := &leaderv1.LeaderWorkerSet{}
		setSubRoleJSON(lws, subRolePlanAnnotation, p)
		_, err := readSubRolePlan(lws)
		require.Error(t, err)
	}
}

func TestSubRolePlanReplayRejectsChangedOwnerBeforeWriting(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	ctx := context.Background()
	require.ErrorIs(t, f.manager.scaleSubRoles(ctx, f.ds, f.observe().LWS, map[string]int{"a": 1, "b": 1}), errReplicaGroupsPending)
	observed := f.observe().LWS.DeepCopy()
	require.NoError(t, f.manager.patchSubRoleLWS(ctx, observed, func(l *leaderv1.LeaderWorkerSet) { l.OwnerReferences[0].UID = "other-disaggregatedset" }))
	_, _, err := f.manager.syncSubRoleGroups(ctx, observed)
	require.ErrorIs(t, err, errReplicaGroupsPending)
	s := f.observe()
	require.Equal(t, observed.Annotations[subRolePlanAnnotation], s.LWS.Annotations[subRolePlanAnnotation])
	for i, g := range s.Groups {
		name, coherent := groupSubRole(g)
		require.True(t, coherent)
		require.Equal(t, []string{"a", "a", "b"}[i], name)
	}
}

func TestSubRoleArmedPlanDoesNotWaitForReplacementReadiness(t *testing.T) {
	f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "a", "b")
	ctx := context.Background()
	require.ErrorIs(t, f.manager.scaleSubRoles(ctx, f.ds, f.observe().LWS, map[string]int{"a": 1, "b": 1}), errReplicaGroupsPending)
	f.sync() // Prepare the retained prefix.
	f.sync() // Publish Spec, but native target propagation is still pending.
	s := f.observe()
	plan, err := readSubRolePlan(s.LWS)
	require.NoError(t, err)
	require.True(t, plan.Armed)
	replacement := s.Groups[0].Leader.DeepCopy()
	require.NoError(t, f.manager.client.Delete(ctx, replacement))
	replacement.ResourceVersion = ""
	replacement.UID = "replacement-unready"
	replacement.Status.Conditions = nil
	delete(replacement.Labels, disaggv1.SubRoleLabelKey)
	require.NoError(t, f.manager.client.Create(ctx, replacement))
	f.sync()
	s = f.observe()
	require.Equal(t, "a", s.Groups[0].Leader.Labels[disaggv1.SubRoleLabelKey], "repair does not wait for native target propagation")
	require.NotEmpty(t, s.LWS.Annotations[subRolePlanAnnotation], "completion still waits for native acknowledgement")
	f.native()
	f.sync()
	s = f.observe()
	require.Empty(t, s.LWS.Annotations[subRolePlanAnnotation])
	require.Len(t, s.Groups, 2)
	require.False(t, s.Groups[0].Ready)
	require.True(t, s.Groups[1].Ready)
}
