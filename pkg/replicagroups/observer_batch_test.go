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

package replicagroups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// Run the existing ownership, readiness, deletion-credit and absent-object cases
// through both APIs, without maintaining a second copy of those fixtures.
func observeWithBatchParity(t *testing.T, reader client.Reader, expected *leaderworkersetv1.LeaderWorkerSet) (*Snapshot, error) {
	t.Helper()
	single, err := Observe(t.Context(), reader, expected)
	batch, batchErr := ObserveMany(t.Context(), reader, []*leaderworkersetv1.LeaderWorkerSet{expected})
	require.Equal(t, err == nil, batchErr == nil, "single=%v; batch=%v", err, batchErr)
	if err != nil {
		require.Nil(t, batch)
	} else {
		require.Equal(t, single, batch[expected.UID])
	}
	return single, err
}

func TestObserveManyScopedPaginatedSnapshot(t *testing.T) {
	a := newFixture(leaderworkersetv1.GroupIdentityOrdinal, 2, 3, "a")
	b := newFixture(leaderworkersetv1.GroupIdentityHash, 2, 3, "b")
	for _, f := range []*fixture{a, b} {
		f.lws.Labels = map[string]string{"slice": "selected"}
		f.scale(1)
	}
	holdTermination(b.workers[1][0])
	expected := []*leaderworkersetv1.LeaderWorkerSet{a.lws.DeepCopy(), b.lws.DeepCopy()}
	a.lws.Generation++ // The batch returns the live generation, not its input.
	a.lws.Spec.Replicas = ptr.To[int32](2)
	excluded := newFixture(leaderworkersetv1.GroupIdentityHash, 1, 1, "excluded")
	excluded.leaders[0].Labels[leaderworkersetv1.SetNameLabelKey] = a.lws.Name // Wrong owner despite matching label.
	expected = append(expected, excluded.lws)                                  // The caller's selector must still exclude this expected LWS.
	unrequested := newFixture(leaderworkersetv1.GroupIdentityOrdinal, 1, 1, "unrequested")
	unrequested.lws.Labels = a.lws.Labels
	unrequested.leaders[0].Annotations = nil // Matching labels cannot put an unrequested LWS into the batch.
	otherNamespace := a.leaders[0].DeepCopy()
	otherNamespace.Namespace = "elsewhere"
	objects := append(a.objects(), b.objects()...)
	objects = append(objects, excluded.objects()...)
	objects = append(objects, unrequested.objects()...)
	objects = append(objects, otherNamespace)
	base := newReader(t, objects...)
	var calls []string
	reader := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("correctly labelled workloads must not require individual GETs")
			return nil
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			options := (&client.ListOptions{}).ApplyOptions(opts)
			require.Equal(t, "test", options.Namespace)
			require.Positive(t, options.Limit)
			start := 0
			if options.Continue != "" {
				var err error
				start, err = strconv.Atoi(options.Continue)
				require.NoError(t, err)
				assert.Empty(t, options.AsListOptions().ResourceVersion, "continued pages use their token, not the initial resource version")
			}
			options.Continue, options.Limit = "", 0
			page := list.DeepCopyObject().(client.ObjectList)
			require.NoError(t, c.List(ctx, page, options))
			items, err := meta.ExtractList(page)
			require.NoError(t, err)
			end := min(start+1, len(items)) // Force every object onto a separate page.
			require.NoError(t, meta.SetList(page, items[start:end]))
			page.SetContinue("") // The final response omits "continue", as the real API does.
			if end < len(items) {
				page.SetContinue(strconv.Itoa(end))
			}
			data, err := json.Marshal(page)
			require.NoError(t, err)
			// Unlike the fake client, a real decoder can reuse the destination's
			// backing array and overwrite objects retained from earlier pages.
			require.NoError(t, json.Unmarshal(data, list))
			calls = append(calls, fmt.Sprintf("%T", list))
			require.Less(t, len(calls), 100, "final-page metadata must not retain the previous continuation token")
			return nil
		},
	})
	raw := &metav1.ListOptions{ResourceVersion: "42"}
	batch, err := ObserveMany(t.Context(), reader, expected, client.MatchingLabels{"slice": "selected"}, &client.ListOptions{Raw: raw})
	require.NoError(t, err)
	assert.Equal(t, &metav1.ListOptions{ResourceVersion: "42"}, raw, "pagination must not mutate the caller's options")
	require.Len(t, batch, 2)
	for _, lws := range expected[:2] {
		single, err := Observe(t.Context(), base, lws)
		require.NoError(t, err)
		assert.Equal(t, single, batch[lws.UID])
	}
	assert.Equal(t, int64(7), expected[0].Generation, "input is read-only")
	assert.Equal(t, int64(8), batch[a.lws.UID].LWS.Generation)
	assert.Equal(t, int32(2), *batch[a.lws.UID].LWS.Spec.Replicas)
	phase := map[string]int{"*v1.LeaderWorkerSetList": 0, "*v1.StatefulSetList": 1, "*v1.DeploymentList": 1, "*v1.ReplicaSetList": 1, "*v1.PodList": 2}
	for i := 1; i < len(calls); i++ {
		assert.LessOrEqual(t, phase[calls[i-1]], phase[calls[i]], "LWS before workloads before Pods: %v", calls)
	}
}

func TestObserveManyLeaderLookupFallback(t *testing.T) {
	for _, identity := range []leaderworkersetv1.GroupIdentityType{leaderworkersetv1.GroupIdentityOrdinal, leaderworkersetv1.GroupIdentityHash} {
		for _, label := range []string{"", "wrong"} {
			t.Run(string(identity)+"/label="+label, func(t *testing.T) {
				f := newFixture(identity, 1, 3)
				f.workload.SetLabels(map[string]string{leaderworkersetv1.SetNameLabelKey: label})
				base := newReader(t, f.objects()...)
				var calls []string
				reader := interceptor.NewClient(base, interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						calls = append(calls, fmt.Sprintf("get %T", obj))
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						calls = append(calls, fmt.Sprintf("list %T", list))
						return c.List(ctx, list, opts...)
					},
				})
				batch, err := ObserveMany(t.Context(), reader, []*leaderworkersetv1.LeaderWorkerSet{f.lws})
				require.NoError(t, err)
				observed, err := Observe(t.Context(), base, f.lws)
				require.NoError(t, err)
				assert.Equal(t, observed, batch[f.lws.UID])
				assert.EqualValues(t, 1, observed.Availability().RetainedReadyReplicas)
				want := []string{"list *v1.LeaderWorkerSetList", "list *v1.StatefulSetList", "get *v1.StatefulSet", "list *v1.PodList"}
				if identity == leaderworkersetv1.GroupIdentityHash {
					want = []string{"list *v1.LeaderWorkerSetList", "list *v1.StatefulSetList", "list *v1.DeploymentList", "get *v1.Deployment", "list *v1.ReplicaSetList", "list *v1.PodList"}
				}
				assert.Equal(t, want, calls, "fallback leaders must be read before their descendants")
			})
		}
	}
}

func TestObserveManyFailureReturnsNoPartialBatch(t *testing.T) {
	for _, failureAt := range []string{"*v1.LeaderWorkerSetList", "*v1.StatefulSetList", "*v1.DeploymentList", "*v1.ReplicaSetList", "*v1.PodList", "fallback", "metadata", "next page", "expired page"} {
		t.Run(failureAt, func(t *testing.T) {
			a := newFixture(leaderworkersetv1.GroupIdentityOrdinal, 1, 1, "a")
			b := newFixture(leaderworkersetv1.GroupIdentityHash, 1, 1, "b")
			if failureAt == "fallback" {
				b.workload.SetLabels(nil)
			}
			if failureAt == "metadata" {
				b.leaders[0].Annotations = nil
			}
			failure := errors.New("API unavailable")
			if failureAt == "expired page" {
				failure = apierrors.NewResourceExpired("the observation's page token expired")
			}
			paged := failureAt == "next page" || failureAt == "expired page"
			reader := interceptor.NewClient(newReader(t, append(a.objects(), b.objects()...)...), interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return failure
				},
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if fmt.Sprintf("%T", list) == failureAt {
						return failure
					}
					options := (&client.ListOptions{}).ApplyOptions(opts)
					if paged && options.Continue != "" {
						return failure
					}
					err := c.List(ctx, list, opts...)
					if paged && fmt.Sprintf("%T", list) == "*v1.PodList" {
						list.SetContinue("next")
					}
					return err
				},
			})
			batch, err := ObserveMany(t.Context(), reader, []*leaderworkersetv1.LeaderWorkerSet{a.lws, b.lws})
			if failureAt == "metadata" {
				assert.ErrorContains(t, err, "invalid group size")
			} else {
				assert.ErrorIs(t, err, failure)
			}
			assert.Nil(t, batch, "a successful earlier LWS is not a usable partial batch")
		})
	}
}

func TestObserveManyValidatesExpectedIdentities(t *testing.T) {
	lws := newFixture(leaderworkersetv1.GroupIdentityOrdinal, 1, 1).lws
	for _, change := range []func(*leaderworkersetv1.LeaderWorkerSet){
		func(lws *leaderworkersetv1.LeaderWorkerSet) { lws.UID = "" },
		func(lws *leaderworkersetv1.LeaderWorkerSet) { lws.Name = "" },
		func(lws *leaderworkersetv1.LeaderWorkerSet) { lws.Namespace = "" },
		func(lws *leaderworkersetv1.LeaderWorkerSet) { lws.Namespace = "other" },
		func(lws *leaderworkersetv1.LeaderWorkerSet) { lws.UID = "conflicting" },
	} {
		invalid := lws.DeepCopy()
		change(invalid)
		batch, err := ObserveMany(t.Context(), nil, []*leaderworkersetv1.LeaderWorkerSet{lws, invalid})
		assert.Error(t, err)
		assert.Nil(t, batch)
	}
	batch, err := ObserveMany(t.Context(), nil, []*leaderworkersetv1.LeaderWorkerSet{nil})
	assert.Error(t, err)
	assert.Nil(t, batch)
	batch, err = ObserveMany(t.Context(), nil, nil)
	assert.NoError(t, err)
	assert.Empty(t, batch)
}

func TestObserveManyDiscardsEarlierChunksOnFailure(t *testing.T) {
	expected, objects := batchFixtures(101, leaderworkersetv1.GroupIdentityOrdinal, 1, 1)
	for _, object := range objects {
		if pod, ok := object.(*corev1.Pod); ok && pod.Labels[leaderworkersetv1.SetNameLabelKey] == expected[100].Name {
			pod.Annotations = nil // Sorted last, after the first 100 snapshots have been assembled.
		}
	}
	podLists := 0
	reader := interceptor.NewClient(newReader(t, objects...), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				podLists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	batch, err := ObserveMany(t.Context(), reader, expected)
	assert.ErrorContains(t, err, "invalid group size")
	assert.Nil(t, batch, "an incomplete slice must not become input to the planner")
	assert.Equal(t, 2, podLists, "the first chunk completed before the failed second chunk")
}

func batchFixtures(n int, identity leaderworkersetv1.GroupIdentityType, replicas, size int) ([]*leaderworkersetv1.LeaderWorkerSet, []client.Object) {
	var expected []*leaderworkersetv1.LeaderWorkerSet
	var objects []client.Object
	for i := range n {
		f := newFixture(identity, replicas, size, fmt.Sprintf("role-%03d", i))
		expected = append(expected, f.lws)
		objects = append(objects, f.objects()...)
	}
	return expected, objects
}

func TestObserveManyRequestCount(t *testing.T) {
	for _, identity := range []leaderworkersetv1.GroupIdentityType{leaderworkersetv1.GroupIdentityOrdinal, leaderworkersetv1.GroupIdentityHash} {
		for _, n := range []int{1, 24, 101} {
			t.Run(fmt.Sprintf("%s/%d", identity, n), func(t *testing.T) {
				expected, objects := batchFixtures(n, identity, 1, 1) // Below the page limit; pagination is tested separately.
				lists := 0
				reader := interceptor.NewClient(newReader(t, objects...), interceptor.Funcs{
					Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
						t.Fatal("normal batching must not read individual objects")
						return nil
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						lists++
						return c.List(ctx, list, opts...)
					},
				})
				batch, err := ObserveMany(t.Context(), reader, expected)
				require.NoError(t, err)
				require.Len(t, batch, n)
				for _, lws := range expected {
					require.NotNil(t, batch[lws.UID])
					assert.Equal(t, lws.UID, batch[lws.UID].LWS.UID)
					assert.Len(t, batch[lws.UID].Groups, 1)
					assert.Equal(t, Availability{ReadyReplicas: 1, RetainedReadyReplicas: 1}, batch[lws.UID].Availability())
				}
				perChunk := 2 // StatefulSets and Pods; Hash also needs Deployments and ReplicaSets.
				if identity == leaderworkersetv1.GroupIdentityHash {
					perChunk = 4
				}
				assert.Equal(t, 1+perChunk*((n+99)/100), lists)
			})
		}
	}
}

func BenchmarkObserveMany(b *testing.B) {
	for _, n := range []int{24, 96} {
		expected, objects := batchFixtures(n, leaderworkersetv1.GroupIdentityHash, 4, 3)
		for _, batched := range []bool{false, true} {
			b.Run(fmt.Sprintf("LWS=%d/batched=%t", n, batched), func(b *testing.B) {
				reader := newReader(b, objects...)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if batched {
						_, err := ObserveMany(b.Context(), reader, expected)
						require.NoError(b, err)
					} else {
						for _, lws := range expected {
							_, err := Observe(b.Context(), reader, lws)
							require.NoError(b, err)
						}
					}
				}
			})
		}
	}
}
