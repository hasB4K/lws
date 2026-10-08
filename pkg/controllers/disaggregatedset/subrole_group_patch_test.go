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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
)

func TestSubRoleGroupPatchResumesAtLeaderCommit(t *testing.T) {
	for _, failPod := range []string{"worker-1", "worker-2", "leader"} {
		t.Run(failPod, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			group := replicagroups.Group{Ready: true}
			for _, name := range []string{"leader", "worker-1", "worker-2"} {
				group.Pods = append(group.Pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: "test", UID: types.UID(name),
					Labels: map[string]string{disaggv1.SubRoleLabelKey: "a", "user-label": "preserved"},
				}})
			}
			group.Leader = group.Pods[0]
			group.Leader.Annotations = map[string]string{"user-annotation": "preserved"}
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(group.Pods[0], group.Pods[1], group.Pods[2]).Build()
			refresh := func() {
				for _, pod := range group.Pods {
					require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
				}
			}
			refresh()
			failure := errors.New("interrupted group relabel")
			var patches []string
			m := NewLeaderWorkerSetManager(interceptor.NewClient(base, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches = append(patches, obj.GetName())
					if obj.GetName() == failPod {
						return failure
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			}))
			changed, err := m.patchSubRoleGroup(t.Context(), group, "b")
			require.ErrorIs(t, err, failure)
			require.Equal(t, failPod != "worker-1", changed)
			for _, pod := range group.Pods {
				require.Equal(t, "a", pod.Labels[disaggv1.SubRoleLabelKey], "read-only snapshot was mutated")
			}
			refresh()
			require.Equal(t, "a", group.Leader.Labels[disaggv1.SubRoleLabelKey], "leader is the assignment commit point")
			_, coherent := groupSubRole(group)
			require.Equal(t, failPod == "worker-1", coherent, "partially relabeled groups must lose logical Ready credit")

			// A fresh manager resumes from live state after a process restart.
			m = NewLeaderWorkerSetManager(base)
			changed, err = m.patchSubRoleGroup(t.Context(), group, "b")
			require.NoError(t, err)
			require.True(t, changed)
			refresh()
			for _, pod := range group.Pods {
				require.Equal(t, "b", pod.Labels[disaggv1.SubRoleLabelKey])
				require.Equal(t, "preserved", pod.Labels["user-label"])
			}
			require.Equal(t, "preserved", group.Leader.Annotations["user-annotation"])
			changed, err = m.patchSubRoleGroup(t.Context(), group, "b")
			require.NoError(t, err)
			require.False(t, changed)
			changed, err = m.patchSubRoleGroup(t.Context(), group, "")
			require.NoError(t, err)
			require.True(t, changed)
			refresh()
			for _, pod := range group.Pods {
				require.Empty(t, pod.Labels[disaggv1.SubRoleLabelKey])
			}
			require.Equal(t, "worker-1", patches[0])
		})
	}
}

func TestSubRoleGroupPatchRejectsStaleResourceVersion(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	leader := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "leader", Namespace: "test", UID: "leader", Labels: map[string]string{disaggv1.SubRoleLabelKey: "a"}}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(leader).Build()
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(leader), leader))
	concurrent := leader.DeepCopy()
	concurrent.Labels["user-label"] = "new"
	require.NoError(t, base.Update(t.Context(), concurrent))
	changed, err := NewLeaderWorkerSetManager(base).patchSubRoleGroup(t.Context(), replicagroups.Group{Leader: leader, Pods: []*corev1.Pod{leader}}, "b")
	require.True(t, apierrors.IsConflict(err), "%v", err)
	require.False(t, changed)
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(leader), leader))
	require.Equal(t, "a", leader.Labels[disaggv1.SubRoleLabelKey])
	require.Equal(t, "new", leader.Labels["user-label"])
}
