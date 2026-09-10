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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/test/wrappers"
)

func TestAllocateSubRolesPreservesValidAssignmentsAndFillsDeficits(t *testing.T) {
	groups := []replicaGroup{
		{id: "0", leader: assignmentPod(0, 0, "short")},
		{id: "1", leader: assignmentPod(1, 0, "invalid")},
		{id: "2", leader: assignmentPod(2, 0, "long")},
	}

	got := allocateSubRoles(groups, []string{"short", "long"}, map[string]int{"short": 2, "long": 1})
	assert.Equal(t, map[string]string{"0": "short", "1": "short", "2": "long"}, got)
}

func TestSubRoleAssignmentReconcilerMirrorsAndRemovesLabels(t *testing.T) {
	leader := assignmentPod(0, 0, "short")
	worker := assignmentPod(0, 1, "long")
	cl := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(leader, worker).Build()
	reconciler := NewSubRoleAssignmentReconciler(cl)

	changed, _, err := reconciler.Reconcile(context.Background(), "default", "lws", []string{"short", "long"}, map[string]int{"short": 1})
	require.NoError(t, err)
	assert.True(t, changed)
	got := &corev1.Pod{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: worker.Name}, got))
	assert.Equal(t, "short", got.Labels[disaggregatedsetv1.SubRoleLabelKey])

	changed, _, err = reconciler.Reconcile(context.Background(), "default", "lws", nil, nil)
	require.NoError(t, err)
	assert.True(t, changed)
	for _, name := range []string{leader.Name, worker.Name} {
		got = &corev1.Pod{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, got))
		assert.NotContains(t, got.Labels, disaggregatedsetv1.SubRoleLabelKey)
	}
}

func TestSubRoleAssignmentReconcilerPreparesOrdinalVictim(t *testing.T) {
	objects := []client.Object{
		assignmentPod(0, 0, "short"),
		assignmentPod(1, 0, "short"),
		assignmentPod(2, 0, "long"),
	}
	cl := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(objects...).Build()
	reconciler := NewSubRoleAssignmentReconciler(cl)

	ready, err := reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityOrdinal, []string{"short", "long"}, map[string]int{"short": 1, "long": 1})
	require.NoError(t, err)
	assert.False(t, ready)
	for index, want := range map[int]string{0: "short", 1: "long", 2: "short"} {
		got := &corev1.Pod{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("lws-%d-0", index)}, got))
		assert.Equal(t, want, got.Labels[disaggregatedsetv1.SubRoleLabelKey])
	}
	ready, err = reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityOrdinal, []string{"short", "long"}, map[string]int{"short": 1, "long": 1})
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestSubRoleAssignmentReconcilerPreparesHashVictimAndRestoresMetadata(t *testing.T) {
	shortA := hashAssignmentPod("a", "short", true)
	shortA.Annotations = map[string]string{corev1.PodDeletionCost: "7"}
	longB := hashAssignmentPod("b", "long", true)
	shortC := hashAssignmentPod("c", "short", false)
	objects := []client.Object{shortA, longB, shortC}
	cl := fake.NewClientBuilder().
		WithScheme(wrappers.DisaggregatedSetTestScheme()).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(objects...).Build()
	reconciler := NewSubRoleAssignmentReconciler(cl)

	// The naturally NotReady group c is the ReplicaSet's preferred physical
	// victim. Relabel it to long so the requested logical sub-role is removed.
	ready, err := reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityHash, []string{"short", "long"}, map[string]int{"short": 2})
	require.NoError(t, err)
	assert.False(t, ready)

	wantRoles := map[string]string{"a": "short", "b": "short", "c": "long"}
	for id, wantRole := range wantRoles {
		got := &corev1.Pod{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "lws-" + id + "-0"}, got))
		assert.Equal(t, wantRole, got.Labels[disaggregatedsetv1.SubRoleLabelKey])
		assert.Equal(t, "2", got.Annotations[scaleDownTargetAnnotationKey])
		if id == "c" {
			assert.Equal(t, "2", got.Annotations[leaderworkersetv1.GroupDrainingAnnotationKey])
			assert.Equal(t, preferredDeletionCost, got.Annotations[corev1.PodDeletionCost])
		} else {
			assert.NotContains(t, got.Annotations, leaderworkersetv1.GroupDrainingAnnotationKey)
			assert.Equal(t, protectedDeletionCost, got.Annotations[corev1.PodDeletionCost])
		}
	}

	ready, err = reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityHash, []string{"short", "long"}, map[string]int{"short": 2})
	require.NoError(t, err)
	assert.True(t, ready)

	require.NoError(t, cl.Delete(context.Background(), shortC))
	changed, err := reconciler.CleanupScaleDown(context.Background(), "default", "lws", 2)
	require.NoError(t, err)
	assert.True(t, changed)
	for id, wantCost := range map[string]string{"a": "7", "b": ""} {
		got := &corev1.Pod{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "lws-" + id + "-0"}, got))
		assert.Equal(t, wantCost, got.Annotations[corev1.PodDeletionCost])
		assert.NotContains(t, got.Annotations, scaleDownTargetAnnotationKey)
		assert.NotContains(t, got.Annotations, originalDeletionCostAnnotationKey)
	}
}

func TestSubRoleAssignmentReconcilerWaitsForHashVictimToBecomeNotReady(t *testing.T) {
	objects := []client.Object{
		hashAssignmentPod("a", "short", true),
		hashAssignmentPod("b", "long", true),
	}
	cl := fake.NewClientBuilder().
		WithScheme(wrappers.DisaggregatedSetTestScheme()).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(objects...).Build()
	reconciler := NewSubRoleAssignmentReconciler(cl)

	ready, err := reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityHash, []string{"short", "long"}, map[string]int{"long": 1})
	require.NoError(t, err)
	assert.False(t, ready)
	ready, err = reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityHash, []string{"short", "long"}, map[string]int{"long": 1})
	require.NoError(t, err)
	assert.False(t, ready, "metadata convergence alone must not permit the scale-down")

	victim := &corev1.Pod{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "lws-a-0"}, victim))
	victim.Status.Conditions[0].Status = corev1.ConditionFalse
	require.NoError(t, cl.Status().Update(context.Background(), victim))

	ready, err = reconciler.PrepareScaleDown(context.Background(), "default", "lws", leaderworkersetv1.GroupIdentityHash, []string{"short", "long"}, map[string]int{"long": 1})
	require.NoError(t, err)
	assert.True(t, ready)
}

func assignmentPod(group, worker int, subRole string) *corev1.Pod {
	return assignmentPodForGroup(fmt.Sprint(group), worker, subRole, true)
}

func hashAssignmentPod(group, subRole string, ready bool) *corev1.Pod {
	return assignmentPodForGroup(group, 0, subRole, ready)
}

func assignmentPodForGroup(group string, worker int, subRole string, ready bool) *corev1.Pod {
	const lws = "lws"
	labels := map[string]string{
		leaderworkersetv1.SetNameLabelKey:     lws,
		leaderworkersetv1.GroupIndexLabelKey:  group,
		leaderworkersetv1.WorkerIndexLabelKey: fmt.Sprint(worker),
	}
	if subRole != "" {
		labels[disaggregatedsetv1.SubRoleLabelKey] = subRole
	}
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s-%d", lws, group, worker), Namespace: "default", Labels: labels},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: readyStatus,
		}}},
	}
}
