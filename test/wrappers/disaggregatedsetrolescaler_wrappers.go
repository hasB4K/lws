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
package wrappers

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
)

// DisaggregatedSetRoleScalerWrapper is a fluent builder for
// DisaggregatedSetRoleScaler test fixtures.
type DisaggregatedSetRoleScalerWrapper struct {
	disaggregatedsetv1.DisaggregatedSetRoleScaler
}

// BuildDisaggregatedSetRoleScaler creates a wrapper with metadata but no
// targetRef or replicas — configure them via WithTargetRef / WithReplicas.
func BuildDisaggregatedSetRoleScaler(name, namespace string) *DisaggregatedSetRoleScalerWrapper {
	return &DisaggregatedSetRoleScalerWrapper{
		DisaggregatedSetRoleScaler: disaggregatedsetv1.DisaggregatedSetRoleScaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}

// WithTargetRef points the scaler at a DisaggregatedSet + role.
func (w *DisaggregatedSetRoleScalerWrapper) WithTargetRef(dsName, roleName string) *DisaggregatedSetRoleScalerWrapper {
	w.Spec.TargetRef = disaggregatedsetv1.DisaggregatedSetRoleRef{
		Name: dsName,
		Role: roleName,
	}
	return w
}

// WithReplicas sets spec.replicas (simulating an HPA/KEDA write).
func (w *DisaggregatedSetRoleScalerWrapper) WithReplicas(n int32) *DisaggregatedSetRoleScalerWrapper {
	w.Spec.Replicas = ptr.To(n)
	return w
}

// Obj returns the underlying object suitable for use with the fake client.
func (w *DisaggregatedSetRoleScalerWrapper) Obj() *disaggregatedsetv1.DisaggregatedSetRoleScaler {
	return &w.DisaggregatedSetRoleScaler
}
