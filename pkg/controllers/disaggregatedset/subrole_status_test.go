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
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

func TestObserveSubRoleStatusesAggregatesAndRefreshes(t *testing.T) {
	f := newNamedSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "current-slice-0", "a", "b")
	old := newNamedSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "old-slice-1", "a")
	f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{
		{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "b"}, {Name: "a"}}},
		{Name: "empty", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "c"}}},
		{Name: "ordinary"},
	}
	for i, fixture := range []*subRoleFixture{f, old} {
		fixture.lws.Labels[disaggv1.RoleLabelKey] = "model"
		fixture.lws.Labels[disaggv1.SliceLabelKey] = []string{"0", "1"}[i]
		fixture.lws.Labels[disaggv1.RevisionLabelKey] = []string{"target", "old"}[i]
		require.NoError(t, fixture.manager.client.Update(t.Context(), fixture.lws))
	}
	// Put both independent slices/revisions in the same fake API server.
	for _, list := range []client.ObjectList{&leaderv1.LeaderWorkerSetList{}, &appsv1.StatefulSetList{}, &corev1.PodList{}} {
		require.NoError(t, old.manager.client.List(t.Context(), list))
		objects, err := meta.ExtractList(list)
		require.NoError(t, err)
		for _, object := range objects {
			obj := object.(client.Object)
			obj.SetResourceVersion("")
			require.NoError(t, f.manager.client.Create(t.Context(), obj))
		}
	}
	var podLists int
	f.manager.apiReader = interceptor.NewClient(f.manager.client.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				podLists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	workloads := []*leaderv1.LeaderWorkerSet{f.lws, old.lws,
		{ObjectMeta: metav1.ObjectMeta{Name: "not-observed", Labels: map[string]string{disaggv1.RoleLabelKey: "ordinary"}}},
	}
	want := map[string]subRoleStatusObservation{
		"model": {children: []disaggv1.SubRoleStatus{
			{Name: "b", Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1},
			{Name: "a", Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 1},
		}, assigned: true},
		"empty": {children: []disaggv1.SubRoleStatus{{Name: "c"}}, assigned: true},
	}
	observed, err := f.manager.observeSubRoleStatuses(t.Context(), f.ds, workloads, "target")
	require.NoError(t, err)
	require.Equal(t, want, observed)
	require.Equal(t, 2, podLists, "each partitioned LWS is observed once")

	// A later collection must see fresh Pod state without changing the aggregate
	// already handed to the status writers. Partial assignment earns no credit.
	worker := &corev1.Pod{}
	require.NoError(t, f.manager.client.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "current-slice-0-0-1"}, worker))
	worker.Status.Conditions = nil
	require.NoError(t, f.manager.client.Status().Update(t.Context(), worker))
	require.NoError(t, f.manager.client.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "current-slice-0-1-1"}, worker))
	delete(worker.Labels, disaggv1.SubRoleLabelKey)
	require.NoError(t, f.manager.client.Update(t.Context(), worker))

	// Both writers consume the same earlier observation without listing Pods.
	scaler := &disaggv1.DisaggregatedSetRoleScaler{ObjectMeta: metav1.ObjectMeta{Name: "ds-model-a", Namespace: "test"}}
	statusClient := fake.NewClientBuilder().WithScheme(f.manager.client.Scheme()).WithObjects(f.ds, scaler).
		WithStatusSubresource(&disaggv1.DisaggregatedSet{}, &disaggv1.DisaggregatedSetRoleScaler{}).Build()
	require.NoError(t, statusClient.Get(t.Context(), client.ObjectKeyFromObject(f.ds), f.ds))
	r := &DisaggregatedSetReconciler{Client: statusClient, LWSManager: f.manager, ScalerManager: NewScalerManager(statusClient, nil)}
	require.NoError(t, r.updateScalerStatus(t.Context(), f.ds, map[string]*disaggv1.DisaggregatedSetRoleScaler{"model/a": scaler}, workloads, observed))
	require.NoError(t, r.updateStatus(t.Context(), f.ds, []string{"model", "empty", "ordinary"}, "target",
		map[string]int{"model": 3, "model/a": 2, "model/b": 1, "empty": 0, "empty/c": 0, "ordinary": 0}, workloads, observed))
	require.NoError(t, statusClient.Get(t.Context(), client.ObjectKeyFromObject(scaler), scaler))
	require.EqualValues(t, 2, scaler.Status.Replicas)
	require.Equal(t, observed["model"].children, f.ds.Status.RoleStatuses[0].SubRoleStatuses)
	require.Equal(t, 2, podLists)

	refreshed, err := f.manager.observeSubRoleStatuses(t.Context(), f.ds, workloads, "target")
	require.NoError(t, err)
	require.Equal(t, 4, podLists)
	require.Equal(t, want, observed, "the earlier aggregate remains immutable")
	require.False(t, refreshed["model"].assigned)
	require.Equal(t, []disaggv1.SubRoleStatus{
		{Name: "b"}, {Name: "a", Replicas: 2, ReadyReplicas: 1, UpdatedReplicas: 1},
	}, refreshed["model"].children)
}

func TestObserveSubRoleStatusesUnknownIsNotZero(t *testing.T) {
	failure := errors.New("cannot list Pods")
	for _, unavailable := range []bool{false, true} {
		name := "replaced LWS"
		wantErr := errReplicaGroupsPending
		if unavailable {
			name, wantErr = "API error", failure
		}
		t.Run(name, func(t *testing.T) {
			f := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a")
			f.ds.Spec.Roles = []disaggv1.DisaggregatedRoleSpec{{Name: "model", SubRoles: []disaggv1.DisaggregatedSubRoleSpec{{Name: "a"}}}}
			f.lws.Labels[disaggv1.RoleLabelKey] = "model"
			if unavailable {
				f.manager.apiReader = interceptor.NewClient(f.manager.client.(client.WithWatch), interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*corev1.PodList); ok {
							return failure
						}
						return c.List(ctx, list, opts...)
					},
				})
			} else {
				f.lws.UID = "stale-uid"
			}
			observed, err := f.manager.observeSubRoleStatuses(t.Context(), f.ds, []*leaderv1.LeaderWorkerSet{f.lws}, "")
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, observed, "an incomplete observation must not publish zero counts")
		})
	}
}
