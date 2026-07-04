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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

func leaderworkersetTemplateWithReplicas(n int32) leaderworkerset.LeaderWorkerSetTemplateSpec {
	return leaderworkerset.LeaderWorkerSetTemplateSpec{
		Spec: leaderworkerset.LeaderWorkerSetSpec{
			Replicas: ptr.To(n),
		},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, disaggv1.AddToScheme(scheme))
	return scheme
}

func makeScaler(name, namespace, dsName, role string, replicas *int32) *disaggv1.DisaggregatedSetRoleScaler {
	return &disaggv1.DisaggregatedSetRoleScaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
		},
		Spec: disaggv1.DisaggregatedSetRoleScalerSpec{
			TargetRef: disaggv1.DisaggregatedSetRoleRef{Name: dsName, Role: role},
			Replicas:  replicas,
		},
	}
}

func TestScalerWebhook_ValidateCreate(t *testing.T) {
	tests := []struct {
		name       string
		scaler     *disaggv1.DisaggregatedSetRoleScaler
		existing   []runtime.Object
		wantErrSub string // substring the aggregated error message must contain; "" for no error
	}{
		{
			name:   "valid",
			scaler: makeScaler("s", "default", "myds", "prefill", ptr.To[int32](3)),
		},
		{
			name:       "empty target name",
			scaler:     makeScaler("s", "default", "", "prefill", ptr.To[int32](1)),
			wantErrSub: "targetRef.name",
		},
		{
			name:       "empty target role",
			scaler:     makeScaler("s", "default", "myds", "", ptr.To[int32](1)),
			wantErrSub: "targetRef.role",
		},
		{
			name:       "negative replicas",
			scaler:     makeScaler("s", "default", "myds", "prefill", ptr.To[int32](-1)),
			wantErrSub: "must be non-negative",
		},
		{
			name:   "unset replicas is allowed",
			scaler: makeScaler("s", "default", "myds", "prefill", nil),
		},
		{
			name:   "duplicate targetRef in same namespace",
			scaler: makeScaler("s2", "default", "myds", "prefill", ptr.To[int32](5)),
			existing: []runtime.Object{
				makeScaler("s1", "default", "myds", "prefill", ptr.To[int32](3)),
			},
			wantErrSub: "already targets",
		},
		{
			name:   "same target in different namespace is fine",
			scaler: makeScaler("s2", "ns-b", "myds", "prefill", ptr.To[int32](5)),
			existing: []runtime.Object{
				makeScaler("s1", "ns-a", "myds", "prefill", ptr.To[int32](3)),
			},
		},
		{
			name:   "same DS different role is fine",
			scaler: makeScaler("decode-scaler", "default", "myds", "decode", ptr.To[int32](2)),
			existing: []runtime.Object{
				makeScaler("prefill-scaler", "default", "myds", "prefill", ptr.To[int32](3)),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(testScheme(t))
			if len(tc.existing) > 0 {
				builder = builder.WithRuntimeObjects(tc.existing...)
			}
			w := &DisaggregatedSetRoleScalerWebhook{Client: builder.Build()}

			_, err := w.ValidateCreate(context.Background(), tc.scaler)
			if tc.wantErrSub == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
			}
		})
	}
}

func TestScalerWebhook_ValidateUpdate_ExcludesSelfFromUniquenessCheck(t *testing.T) {
	// A scaler being updated must not collide with itself.
	existing := makeScaler("s1", "default", "myds", "prefill", ptr.To[int32](3))
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(existing).Build()
	w := &DisaggregatedSetRoleScalerWebhook{Client: fakeClient}

	newScaler := existing.DeepCopy()
	newScaler.Spec.Replicas = ptr.To[int32](10) // just changing replicas
	_, err := w.ValidateUpdate(context.Background(), existing, newScaler)
	require.NoError(t, err, "updating own replicas must not trip the uniqueness check")
}

func TestDisaggregatedSetWebhook_ScalingWarning(t *testing.T) {
	w := &DisaggregatedSetWebhook{}
	ctx := context.Background()

	ds := &disaggv1.DisaggregatedSet{
		ObjectMeta: metav1.ObjectMeta{Name: "myds", Namespace: "default"},
		Spec: disaggv1.DisaggregatedSetSpec{
			Roles: []disaggv1.DisaggregatedRoleSpec{
				{
					Name:    "prefill",
					Scaling: &disaggv1.RoleScaling{Mode: disaggv1.RoleScalingExternal},
					// intentionally leave replicas unset -> no warning
				},
				{
					Name:    "decode",
					Scaling: &disaggv1.RoleScaling{Mode: disaggv1.RoleScalingExternal},
					LeaderWorkerSetTemplateSpec: leaderworkersetTemplateWithReplicas(4),
				},
			},
		},
	}
	warns, err := w.ValidateCreate(ctx, ds)
	require.NoError(t, err, "the warning path must not produce an error (CEL enforces the hard rule)")
	require.Len(t, warns, 1, "only the External role with non-zero spec.replicas should warn")
	assert.Contains(t, warns[0], "decode")
	assert.Contains(t, warns[0], "scaling.mode: External")
}
