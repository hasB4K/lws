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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	disaggregatedsetutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
	"sigs.k8s.io/lws/test/wrappers"
)

// --- Helpers ---

func reqFor(ds *disaggregatedsetv1.DisaggregatedSet) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: ds.Namespace,
		Name:      ds.Name,
	}}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// getLWSReplicasForRole reads the current-revision LWS spec.replicas for the
// given role of the given DS from the fake client. It looks up the LWS by
// the deterministic name generated from (ds.Name, role, revision).
func getLWSReplicasForRole(t *testing.T, fakeClient client.Client, ds *disaggregatedsetv1.DisaggregatedSet, role string) int32 {
	t.Helper()
	revision := disaggregatedsetutils.ComputeRevision(ds.Spec.Roles)
	name := disaggregatedsetutils.GenerateName(ds.Name, role, revision)
	return getTestLWSReplicas(fakeClient, ds.Namespace, name)
}

// --- getTargetReplicas ---

func TestGetTargetReplicas_Static(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleNoReplicas("prefill", "img"). // no Replicas => default 1
		WithRole("decode", 4, "img").
		Obj()

	got, err := getTargetReplicas(ds, "prefill", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, got, "unset replicas should default to 1")

	got, err = getTargetReplicas(ds, "decode", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, got, "should read inline spec.replicas")
}

func TestGetTargetReplicas_External(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternal("prefill", "img").
		Obj()

	// No scaler map at all -> sentinel error.
	_, err := getTargetReplicas(ds, "prefill", nil)
	require.ErrorIs(t, err, ErrScalerNotReady)

	// Scaler with nil replicas -> sentinel error.
	scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{
		"prefill": wrappers.BuildDisaggregatedSetRoleScaler("s", "default").
			WithTargetRef("myds", "prefill").Obj(),
	}
	_, err = getTargetReplicas(ds, "prefill", scalers)
	require.ErrorIs(t, err, ErrScalerNotReady)

	// Scaler with replicas set -> that value.
	scalers["prefill"].Spec.Replicas = ptr.To[int32](7)
	got, err := getTargetReplicas(ds, "prefill", scalers)
	require.NoError(t, err)
	assert.Equal(t, 7, got)
}

func TestGetTargetReplicas_StaticIgnoresScaler(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRole("decode", 3, "img").
		Obj()
	scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{
		"decode": wrappers.BuildDisaggregatedSetRoleScaler("s", "default").
			WithTargetRef("ds", "decode").WithReplicas(99).Obj(),
	}
	got, err := getTargetReplicas(ds, "decode", scalers)
	require.NoError(t, err)
	assert.Equal(t, 3, got, "Static role must ignore scaler value")
}

// --- scalerToDSKey mapper ---

func TestScalerToDSKey(t *testing.T) {
	s := wrappers.BuildDisaggregatedSetRoleScaler("s", "ns").
		WithTargetRef("myds", "prefill").Obj()

	reqs := scalerToDSKey(context.Background(), s)
	require.Len(t, reqs, 1)
	assert.Equal(t, "ns", reqs[0].Namespace)
	assert.Equal(t, "myds", reqs[0].Name)
}

func TestScalerToDSKey_EmptyTarget(t *testing.T) {
	s := wrappers.BuildDisaggregatedSetRoleScaler("s", "ns").Obj()
	reqs := scalerToDSKey(context.Background(), s)
	assert.Empty(t, reqs, "empty targetRef.name should map to no request")
}

// --- loadScalersForDS ---

func TestLoadScalersForDS_FiltersByTargetName(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").Obj()

	scaler1 := wrappers.BuildDisaggregatedSetRoleScaler("s1", "default").
		WithTargetRef("myds", "prefill").WithReplicas(3).Obj()
	scaler2 := wrappers.BuildDisaggregatedSetRoleScaler("s2", "default").
		WithTargetRef("myds", "decode").WithReplicas(2).Obj()
	scalerOther := wrappers.BuildDisaggregatedSetRoleScaler("s3", "default").
		WithTargetRef("some-other-ds", "prefill").WithReplicas(5).Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(scaler1, scaler2, scalerOther).
		Build()
	r := newTestReconciler(fakeClient)

	scalers, all, err := r.loadScalersForDS(context.Background(), ds)
	require.NoError(t, err)
	assert.Len(t, all, 2, "should include both scalers targeting myds")
	assert.Len(t, scalers, 2)
	assert.Equal(t, "s1", scalers["prefill"].Name)
	assert.Equal(t, "s2", scalers["decode"].Name)
}

func TestLoadScalersForDS_DropsConflicts(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").Obj()

	scalerA := wrappers.BuildDisaggregatedSetRoleScaler("a", "default").
		WithTargetRef("myds", "prefill").WithReplicas(3).Obj()
	scalerB := wrappers.BuildDisaggregatedSetRoleScaler("b", "default").
		WithTargetRef("myds", "prefill").WithReplicas(9).Obj() // duplicate role

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(scalerA, scalerB).
		Build()
	r := newTestReconciler(fakeClient)

	scalers, all, err := r.loadScalersForDS(context.Background(), ds)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.NotContains(t, scalers, "prefill", "conflicting scalers must be dropped from the map")
}

// --- Reconcile end-to-end with autocreate scaler ---

func TestReconcile_AutoCreatesScaler_NoInitialReplicas(t *testing.T) {
	// Without InitialReplicas the auto-created scaler has spec.replicas=nil;
	// the role holds at 0 with WaitingForScaler=True until an autoscaler writes.
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternal("prefill", "img").
		WithRole("decode", 2, "img").
		Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(ds).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSet{}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	r := newTestReconciler(fakeClient)

	_, err := r.Reconcile(context.Background(), reqFor(ds))
	require.NoError(t, err)

	// Scaler is auto-created with deterministic name.
	scalerName := ScalerName(ds.Name, "prefill")
	created := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, created))
	assert.Equal(t, ds.Name, created.Spec.TargetRef.Name)
	assert.Equal(t, "prefill", created.Spec.TargetRef.Role)
	assert.Nil(t, created.Spec.Replicas, "no InitialReplicas => nil")
	require.Len(t, created.OwnerReferences, 1)
	assert.True(t, created.OwnerReferences[0].Controller != nil && *created.OwnerReferences[0].Controller,
		"scaler must be controller-owned by the DS")
	assert.Equal(t, ds.UID, created.OwnerReferences[0].UID)

	// Role holds at 0; decode unaffected.
	assert.Equal(t, int32(2), getLWSReplicasForRole(t, fakeClient, ds, "decode"))
	assert.Equal(t, int32(0), getLWSReplicasForRole(t, fakeClient, ds, "prefill"))

	current := &disaggregatedsetv1.DisaggregatedSet{}
	require.NoError(t, fakeClient.Get(context.Background(), reqFor(ds).NamespacedName, current))
	waiting := findCondition(current.Status.Conditions, ConditionWaitingForScaler)
	require.NotNil(t, waiting)
	assert.Equal(t, metav1.ConditionTrue, waiting.Status)
}

func TestReconcile_AutoCreatesScaler_WithInitialReplicas(t *testing.T) {
	// InitialReplicas=3 seeds the auto-created scaler; the role scales to 3
	// immediately and WaitingForScaler stays False.
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternalInitial("prefill", "img", 3).
		WithRole("decode", 2, "img").
		Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(ds).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSet{}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	r := newTestReconciler(fakeClient)

	_, err := r.Reconcile(context.Background(), reqFor(ds))
	require.NoError(t, err)

	scalerName := ScalerName(ds.Name, "prefill")
	created := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, created))
	require.NotNil(t, created.Spec.Replicas)
	assert.Equal(t, int32(3), *created.Spec.Replicas)

	assert.Equal(t, int32(3), getLWSReplicasForRole(t, fakeClient, ds, "prefill"))
	assert.Equal(t, int32(2), getLWSReplicasForRole(t, fakeClient, ds, "decode"))

	current := &disaggregatedsetv1.DisaggregatedSet{}
	require.NoError(t, fakeClient.Get(context.Background(), reqFor(ds).NamespacedName, current))
	if waiting := findCondition(current.Status.Conditions, ConditionWaitingForScaler); waiting != nil {
		assert.Equal(t, metav1.ConditionFalse, waiting.Status)
	}
}

func TestReconcile_ExistingScalerReplicasDriveLWS(t *testing.T) {
	// After the scaler is auto-created, an external autoscaler updates
	// spec.replicas via /scale. The next reconcile drives the LWS to that value.
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternal("prefill", "img").
		WithRole("decode", 2, "img").
		Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(ds).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSet{}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	r := newTestReconciler(fakeClient)

	// First reconcile creates the scaler.
	_, err := r.Reconcile(context.Background(), reqFor(ds))
	require.NoError(t, err)

	// Simulate HPA writing to /scale.
	scalerName := ScalerName(ds.Name, "prefill")
	created := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, created))
	created.Spec.Replicas = ptr.To[int32](5)
	require.NoError(t, fakeClient.Update(context.Background(), created))

	// Second reconcile picks up the new value and scales the LWS.
	_, err = r.Reconcile(context.Background(), reqFor(ds))
	require.NoError(t, err)
	assert.Equal(t, int32(5), getLWSReplicasForRole(t, fakeClient, ds, "prefill"))
}

func TestReconcile_DeletesScalerWhenRoleFlipsToStatic(t *testing.T) {
	// A role that starts External and then flips to Static must have its
	// auto-created scaler garbage collected.
	dsExt := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternalInitial("prefill", "img", 2).
		WithRole("decode", 2, "img").
		Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(dsExt).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSet{}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	r := newTestReconciler(fakeClient)

	_, err := r.Reconcile(context.Background(), reqFor(dsExt))
	require.NoError(t, err)

	scalerName := ScalerName(dsExt.Name, "prefill")
	created := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, created),
		"scaler must exist after the first reconcile of an External role")

	// Flip the role to Static (drop the Scaling block, set inline replicas).
	current := &disaggregatedsetv1.DisaggregatedSet{}
	require.NoError(t, fakeClient.Get(context.Background(), reqFor(dsExt).NamespacedName, current))
	current.Spec.Roles[0].Scaling = nil
	current.Spec.Roles[0].Spec.Replicas = ptr.To[int32](2)
	require.NoError(t, fakeClient.Update(context.Background(), current))

	_, err = r.Reconcile(context.Background(), reqFor(dsExt))
	require.NoError(t, err)

	// Scaler should be gone.
	err = fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, created)
	require.Error(t, err, "scaler must be deleted when the role is no longer External")
}

func TestReconcile_ScalerStatusWriteback(t *testing.T) {
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternalInitial("prefill", "img", 3).
		WithRole("decode", 2, "img").
		Obj()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testSchemeForUnit()).
		WithObjects(ds).
		WithStatusSubresource(&disaggregatedsetv1.DisaggregatedSet{}, &disaggregatedsetv1.DisaggregatedSetRoleScaler{}).
		Build()
	r := newTestReconciler(fakeClient)

	_, err := r.Reconcile(context.Background(), reqFor(ds))
	require.NoError(t, err)

	// Scaler status should reflect the new-revision LWS and carry the
	// selector pointing at that LWS name.
	scalerName := ScalerName(ds.Name, "prefill")
	currentScaler := &disaggregatedsetv1.DisaggregatedSetRoleScaler{}
	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: scalerName}, currentScaler))

	revision := disaggregatedsetutils.ComputeRevision(ds.Spec.Roles)
	expectedLWSName := disaggregatedsetutils.GenerateName(ds.Name, "prefill", revision)

	assert.Contains(t, currentScaler.Status.Selector, expectedLWSName,
		"selector must reference the new-revision LWS name")

	ready := findCondition(currentScaler.Status.Conditions, disaggregatedsetv1.ScalerReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
}

func TestBuildPlannerState_MonotonicityGuard(t *testing.T) {
	// External role with a mid-rollout state:
	//   - old revision is on its drain trajectory (5 replicas)
	//   - new revision has grown to 3 replicas
	//   - HPA has just written scaler.spec.replicas = 2 (shrink)
	// The guard should clamp targetNew to currentNew (=3), preventing a
	// mid-rollout scale-down flip.
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternal("prefill", "img").
		Obj()
	scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{
		"prefill": wrappers.BuildDisaggregatedSetRoleScaler("s", "default").
			WithTargetRef("myds", "prefill").WithReplicas(2).Obj(),
	}

	allRoles := []string{"prefill"}
	specRoleSet := map[string]bool{"prefill": true}

	oldRevisions := disaggregatedsetutils.RevisionRolesList{{
		Revision: "old",
		Roles: map[string]*leaderworkersetv1.LeaderWorkerSet{
			"prefill": makeLWS(withReplicas(5), withReadyReplicas(5), withInitialReplicasAnnotation(5)),
		},
	}}
	newRevision := disaggregatedsetutils.RevisionRoles{
		Revision: "new",
		Roles: map[string]*leaderworkersetv1.LeaderWorkerSet{
			"prefill": makeLWS(withReplicas(3), withReadyReplicas(3)),
		},
	}

	_, _, currentNew, targetNew := buildPlannerState(ds, allRoles, specRoleSet, oldRevisions, newRevision, scalers)
	assert.Equal(t, 3, currentNew[0])
	assert.Equal(t, 3, targetNew[0],
		"monotonicity guard: targetNew must not fall below currentNew for External roles")
}

func TestBuildPlannerState_GuardReleasesOnceStable(t *testing.T) {
	// Once the new revision reaches the HPA's target and stabilizes, the
	// guard has no effect: targetNew tracks the scaler value exactly.
	ds := wrappers.BuildDisaggregatedSet("myds", "default").
		WithRoleExternal("prefill", "img").
		Obj()
	scalers := map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler{
		"prefill": wrappers.BuildDisaggregatedSetRoleScaler("s", "default").
			WithTargetRef("myds", "prefill").WithReplicas(7).Obj(),
	}

	allRoles := []string{"prefill"}
	specRoleSet := map[string]bool{"prefill": true}

	oldRevisions := disaggregatedsetutils.RevisionRolesList{}
	newRevision := disaggregatedsetutils.RevisionRoles{
		Revision: "new",
		Roles: map[string]*leaderworkersetv1.LeaderWorkerSet{
			"prefill": makeLWS(withReplicas(5), withReadyReplicas(5)),
		},
	}

	_, _, currentNew, targetNew := buildPlannerState(ds, allRoles, specRoleSet, oldRevisions, newRevision, scalers)
	assert.Equal(t, 5, currentNew[0])
	assert.Equal(t, 7, targetNew[0], "guard must not clamp when scaler target > currentNew")
}

func TestErrScalerNotReadyIsSentinel(t *testing.T) {
	// Regression: callers rely on errors.Is to detect the sentinel.
	err := ErrScalerNotReady
	assert.True(t, errors.Is(err, ErrScalerNotReady))
	assert.False(t, errors.Is(errors.New("something else"), ErrScalerNotReady))
}
