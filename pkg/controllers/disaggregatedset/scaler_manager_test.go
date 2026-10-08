/*
Copyright 2025 The Kubernetes Authors.

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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/test/wrappers"
)

func newDSWithRoles(name string, roles ...disaggregatedsetv1.DisaggregatedRoleSpec) *disaggregatedsetv1.DisaggregatedSet {
	return &disaggregatedsetv1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec:       disaggregatedsetv1.DisaggregatedSetSpec{Roles: roles},
	}
}

func externalRole(name string) disaggregatedsetv1.DisaggregatedRoleSpec {
	return disaggregatedsetv1.DisaggregatedRoleSpec{
		Name:    name,
		Scaling: &disaggregatedsetv1.RoleScaling{Mode: disaggregatedsetv1.RoleScalingExternal},
	}
}

func staticRole(name string) disaggregatedsetv1.DisaggregatedRoleSpec {
	return disaggregatedsetv1.DisaggregatedRoleSpec{Name: name}
}

func TestScalerManagerReconcileCreatesMissing(t *testing.T) {
	ds := newDSWithRoles("myds", externalRole("prefill"), staticRole("decode"))
	cl := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(ds).Build()
	m := NewScalerManager(cl, events.NewFakeRecorder(10))

	scalers, err := m.Reconcile(context.TODO(), ds, nil)
	require.NoError(t, err)
	require.Contains(t, scalers, "prefill")
	require.NotContains(t, scalers, "decode")

	got := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: "myds-prefill", Namespace: "default"}, got))
	assert.Equal(t, "prefill", got.Labels[disaggregatedsetv1.RoleLabelKey])
	assert.Equal(t, "myds", got.Labels[disaggregatedsetv1.SetNameLabelKey])
	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, ds.UID, got.OwnerReferences[0].UID)
	assert.Equal(t, ptr.To(true), got.OwnerReferences[0].Controller)
}

func TestScalerManagerReconcileDeletesOrphaned(t *testing.T) {
	ds := newDSWithRoles("myds", externalRole("prefill"), staticRole("decode"))
	stale := &disaggregatedsetv1.DisaggregatedSetRoleScaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: "myds-old-role", Namespace: "default",
			Labels: map[string]string{
				disaggregatedsetv1.SetNameLabelKey: "myds",
				disaggregatedsetv1.RoleLabelKey:    "old-role",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: disaggregatedsetv1.GroupVersion.String(), Kind: "DisaggregatedSet",
				Name: "myds", UID: ds.UID, Controller: ptr.To(true),
			}},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(ds, stale).Build()
	m := NewScalerManager(cl, events.NewFakeRecorder(10))

	_, err := m.Reconcile(context.TODO(), ds, nil)
	require.NoError(t, err)

	assert.True(t, apierrorsIsNotFound(cl.Get(context.TODO(), types.NamespacedName{Name: "myds-old-role", Namespace: "default"}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{})))
}

func TestScalerManagerReconcileRefusesForeignScaler(t *testing.T) {
	ds := newDSWithRoles("myds", externalRole("prefill"), staticRole("decode"))
	foreign := &disaggregatedsetv1.DisaggregatedSetRoleScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "myds-prefill", Namespace: "default"},
	}
	cl := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(ds, foreign).Build()
	rec := events.NewFakeRecorder(10)
	m := NewScalerManager(cl, rec)

	scalers, err := m.Reconcile(context.TODO(), ds, nil)
	require.NoError(t, err)
	assert.NotContains(t, scalers, "prefill")

	got := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: "myds-prefill", Namespace: "default"}, got))
	assert.Empty(t, got.OwnerReferences, "foreign scaler must not be adopted")

	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, EventReasonScalerConflict)
	default:
		t.Fatal("expected conflict event")
	}
}

func TestResolveDesiredReplicasByRole(t *testing.T) {
	cases := []struct {
		name         string
		role         disaggregatedsetv1.DisaggregatedRoleSpec
		scalerHas    bool
		scalerVal    int32
		wantResolved bool
		wantReplicas int
	}{
		{"static default", staticRole("r"), false, 0, true, 1},
		{"static explicit", roleWithReplicas("r", 4), false, 0, true, 4},
		{"external + scaler seeded at 0", externalRole("r"), true, 0, true, 0},
		{"external + scaler written to 7", externalRole("r"), true, 7, true, 7},
		{"external + scaler missing leaves target unresolved", externalRole("r"), false, 0, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := newDSWithRoles("d", tc.role)
			scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{}
			if tc.scalerHas {
				scalers["r"] = &disaggregatedsetv1.DisaggregatedSetRoleScaler{
					Spec: disaggregatedsetv1.DisaggregatedSetRoleScalerSpec{Replicas: tc.scalerVal},
				}
			}
			desiredReplicasByRole := resolveDesiredReplicasByRole(ds, scalers)
			replicas, resolved := desiredReplicasByRole["r"]
			assert.Equal(t, tc.wantResolved, resolved)
			if tc.wantResolved {
				assert.Equal(t, tc.wantReplicas, replicas)
			}
		})
	}
}

func roleWithReplicas(name string, replicas int32) disaggregatedsetv1.DisaggregatedRoleSpec {
	r := staticRole(name)
	r.Spec.Replicas = &replicas
	return r
}

func TestScalerManagerWriteStatus(t *testing.T) {
	ds := newDSWithRoles("myds", externalRole("prefill"))
	scaler := &disaggregatedsetv1.DisaggregatedSetRoleScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "myds-prefill", Namespace: "default", Generation: 2},
		Spec:       disaggregatedsetv1.DisaggregatedSetRoleScalerSpec{Replicas: 5},
	}
	cl := fake.NewClientBuilder().
		WithScheme(wrappers.DisaggregatedSetTestScheme()).
		WithObjects(scaler).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	m := NewScalerManager(cl, events.NewFakeRecorder(10))

	require.NoError(t, m.WriteStatus(context.TODO(), ds, map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{"prefill": scaler}, map[string]int32{"prefill": 4}))

	got := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: "myds-prefill", Namespace: "default"}, got))
	assert.EqualValues(t, 4, got.Status.Replicas)
	assert.Equal(t, "disaggregatedset.x-k8s.io/name=myds,disaggregatedset.x-k8s.io/role=prefill,leaderworkerset.sigs.k8s.io/worker-index=0", got.Status.Selector)
	require.Len(t, got.Status.Conditions, 1)
	assert.Equal(t, metav1.ConditionTrue, got.Status.Conditions[0].Status)
}

// apierrorsIsNotFound is a tiny local helper to avoid importing apierrors just
// for one call in tests.
func apierrorsIsNotFound(err error) bool {
	return err != nil && client.IgnoreNotFound(err) == nil
}

func TestResolveSubRoleTargets(t *testing.T) {
	parent := staticRole("pool")
	parent.Spec.Replicas = ptr.To[int32](99) // Ignored for partitioned roles.
	parent.SubRoles = []disaggregatedsetv1.DisaggregatedSubRoleSpec{
		{Name: "default"}, {Name: "paused", Replicas: ptr.To[int32](0)},
		{Name: "external", Scaling: &disaggregatedsetv1.RoleScaling{Mode: disaggregatedsetv1.RoleScalingExternal}},
	}
	for _, tc := range []struct {
		name     string
		external *int32
		want     map[string]int
	}{
		{"missing child", nil, map[string]int{"pool/default": 1, "pool/paused": 0}},
		{"known zero child", ptr.To[int32](0), map[string]int{"pool/default": 1, "pool/paused": 0, "pool/external": 0, "pool": 1}},
		{"known positive child", ptr.To[int32](3), map[string]int{"pool/default": 1, "pool/paused": 0, "pool/external": 3, "pool": 4}},
		{"overflow omits parent", ptr.To[int32](math.MaxInt32), map[string]int{"pool/default": 1, "pool/paused": 0, "pool/external": math.MaxInt32}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{}
			if tc.external != nil {
				scalers["pool/external"] = &disaggregatedsetv1.DisaggregatedSetRoleScaler{Spec: disaggregatedsetv1.DisaggregatedSetRoleScalerSpec{Replicas: *tc.external}}
			}
			assert.Equal(t, tc.want, resolveDesiredReplicasByRole(newDSWithRoles("d", parent), scalers))
		})
	}
}

func TestSubRoleScalerLifecycleAndSelector(t *testing.T) {
	parent := staticRole("pool")
	parent.SubRoles = []disaggregatedsetv1.DisaggregatedSubRoleSpec{
		{Name: "hot", Scaling: &disaggregatedsetv1.RoleScaling{Mode: disaggregatedsetv1.RoleScalingExternal}},
		{Name: "cold", Replicas: ptr.To[int32](0)},
	}
	ds := newDSWithRoles("d", parent)
	c := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(ds).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSetRoleScaler{}).Build()
	m := NewScalerManager(c, events.NewFakeRecorder(10))
	scalers, err := m.Reconcile(t.Context(), ds, func(key string) int32 {
		require.Equal(t, "pool/hot", key)
		return 3
	})
	require.NoError(t, err)
	require.Len(t, scalers, 1)
	scaler := scalers["pool/hot"]
	require.NotNil(t, scaler)
	assert.Equal(t, "d-pool-hot", scaler.Name)
	assert.EqualValues(t, 3, scaler.Spec.Replicas)
	assert.Equal(t, "pool", scaler.Labels[disaggregatedsetv1.RoleLabelKey])
	assert.Equal(t, "hot", scaler.Labels[disaggregatedsetv1.SubRoleLabelKey])
	require.NoError(t, m.WriteStatus(t.Context(), ds, scalers, map[string]int32{"pool/hot": 2}))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(scaler), scaler))
	assert.EqualValues(t, 2, scaler.Status.Replicas)
	selector, err := labels.Parse(scaler.Status.Selector)
	require.NoError(t, err)
	podLabels := labels.Set{disaggregatedsetv1.SetNameLabelKey: ds.Name, disaggregatedsetv1.RoleLabelKey: "pool", disaggregatedsetv1.SubRoleLabelKey: "hot", leaderworkersetv1.WorkerIndexLabelKey: "0"}
	assert.True(t, selector.Matches(podLabels))
	podLabels[leaderworkersetv1.WorkerIndexLabelKey] = "1"
	assert.False(t, selector.Matches(podLabels), "workers must not enter HPA's group denominator")
	podLabels[leaderworkersetv1.WorkerIndexLabelKey], podLabels[disaggregatedsetv1.SubRoleLabelKey] = "0", "cold"
	assert.False(t, selector.Matches(podLabels), "sibling pool must not enter the selector")
	ds.Spec.Roles[0].SubRoles[0].Scaling = nil
	scalers, err = m.Reconcile(t.Context(), ds, nil)
	require.NoError(t, err)
	assert.Empty(t, scalers)
	assert.True(t, apierrorsIsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(scaler), scaler)))
}

func TestSubRoleScalerDoesNotAdoptDeletingParentNameCollision(t *testing.T) {
	ds := newDSWithRoles("d", externalRole("pool-hot"))
	c := fake.NewClientBuilder().WithScheme(wrappers.DisaggregatedSetTestScheme()).WithObjects(ds).Build()
	m := NewScalerManager(c, events.NewFakeRecorder(10))
	scalers, err := m.Reconcile(t.Context(), ds, func(string) int32 { return 9 })
	require.NoError(t, err)
	old := scalers["pool-hot"]
	old.Finalizers = []string{"test/hold"}
	require.NoError(t, c.Update(t.Context(), old))
	// These two topologies are individually valid but reuse one DNS name.
	ds.Spec.Roles = []disaggregatedsetv1.DisaggregatedRoleSpec{{Name: "pool", SubRoles: []disaggregatedsetv1.DisaggregatedSubRoleSpec{{Name: "hot", Scaling: externalRole("").Scaling}}}}
	scalers, err = m.Reconcile(t.Context(), ds, func(string) int32 { return 2 })
	require.NoError(t, err)
	assert.Empty(t, scalers, "new child's target remains unresolved until the old scaler is gone")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(old), old))
	assert.False(t, old.DeletionTimestamp.IsZero())
	assert.EqualValues(t, 9, old.Spec.Replicas, "old parent target must not seed the new child")
}
