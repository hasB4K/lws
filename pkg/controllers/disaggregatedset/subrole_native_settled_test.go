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
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	leaderv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/replicagroups"
)

func TestSubRoleNativeObservationBarrier(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*replicagroups.Snapshot)
		settled bool
	}{
		{name: "acknowledged even while a group is unready", settled: true, change: func(s *replicagroups.Snapshot) { s.Groups[0].Ready = false }},
		{name: "LWS generation pending", change: func(s *replicagroups.Snapshot) { s.LWS.Generation++ }},
		{name: "native generation pending", change: func(s *replicagroups.Snapshot) { s.LeaderStatefulSet.Generation++ }},
		{name: "native cache still contains a deleted Pod", change: func(s *replicagroups.Snapshot) { s.LeaderStatefulSet.Status.Replicas++ }},
		{name: "native target pending", change: func(s *replicagroups.Snapshot) { s.LeaderStatefulSet.Spec.Replicas = ptr.To[int32](3) }},
		{name: "native deleting", change: func(s *replicagroups.Snapshot) { s.LeaderStatefulSet.DeletionTimestamp = ptr.To(metav1.Now()) }},
		{name: "native absent", change: func(s *replicagroups.Snapshot) { s.LeaderStatefulSet = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSubRoleFixture(t, leaderv1.GroupIdentityOrdinal, "a", "b").observe()
			tc.change(s)
			require.Equal(t, tc.settled, subRoleNativeSettled(s))
		})
	}
}
