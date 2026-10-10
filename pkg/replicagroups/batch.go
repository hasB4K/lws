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
	"fmt"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/pager"
	"sigs.k8s.io/controller-runtime/pkg/client"

	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// Bound selector length independently of the number of roles or revisions.
const maxLWSPerBatch = 100

// ObserveMany is Observe's batched counterpart for LWS in one namespace. It
// first lists live LWS, then reads native workloads followed by Pods for each
// bounded batch.
// options scope the LWS list (for example, to one owner's slice); only objects
// matching an expected name and UID are observed. Missing or replaced LWS have
// no map entry: consumers must treat that as unknown, not zero readiness.
//
// Like Observe, this requires an uncached reader for safety-sensitive decisions,
// returns read-only objects, and is ordered rather than atomic. Consumers must
// compare generations against their earlier intents and guard writes. All list
// pages are read; any read or metadata error returns no partial result. Callers
// needing independent failure domains should use separate batches for them.
func ObserveMany(ctx context.Context, reader client.Reader, expected []*leaderworkersetv1.LeaderWorkerSet, options ...client.ListOption) (map[types.UID]*Snapshot, error) {
	result := make(map[types.UID]*Snapshot, len(expected))
	if len(expected) == 0 {
		return result, nil
	}
	namespace := ""
	wanted := make(map[string]types.UID, len(expected))
	for _, lws := range expected {
		if lws == nil || lws.UID == "" || lws.Name == "" || lws.Namespace == "" {
			return nil, fmt.Errorf("observing replica groups requires an LWS namespace, name and UID")
		}
		if namespace != "" && namespace != lws.Namespace {
			return nil, fmt.Errorf("batched LWS observations require one namespace")
		}
		if uid, found := wanted[lws.Name]; found && uid != lws.UID {
			return nil, fmt.Errorf("conflicting expected UIDs for LeaderWorkerSet %s", lws.Name)
		}
		namespace = lws.Namespace
		wanted[lws.Name] = lws.UID
	}
	var live leaderworkersetv1.LeaderWorkerSetList
	if err := listAll(ctx, reader, &live, append(slices.Clone(options), client.InNamespace(namespace))...); err != nil {
		return nil, err
	}
	var matched []*leaderworkersetv1.LeaderWorkerSet
	for i := range live.Items {
		lws := &live.Items[i]
		if uid, found := wanted[lws.Name]; found && uid == lws.UID {
			matched = append(matched, lws)
		}
	}
	for batch := range slices.Chunk(matched, maxLWSPerBatch) {
		if err := observeBatch(ctx, reader, batch, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type batchResources struct {
	lws         *leaderworkersetv1.LeaderWorkerSet
	leader      client.Object
	replicaSets []appsv1.ReplicaSet
	workerSets  []appsv1.StatefulSet
	pods        []corev1.Pod
}

func observeBatch(ctx context.Context, reader client.Reader, live []*leaderworkersetv1.LeaderWorkerSet, result map[types.UID]*Snapshot) error {
	byName := make(map[string]*batchResources, len(live))
	names, hasHash := make([]string, 0, len(live)), false
	for _, lws := range live {
		byName[lws.Name] = &batchResources{lws: lws}
		names = append(names, lws.Name)
		hasHash = hasHash || lws.Spec.GroupIdentity == leaderworkersetv1.GroupIdentityHash
	}
	requirement, err := labels.NewRequirement(leaderworkersetv1.SetNameLabelKey, selection.In, names)
	if err != nil {
		return err
	}
	options := []client.ListOption{client.InNamespace(live[0].Namespace), client.MatchingLabelsSelector{Selector: labels.NewSelector().Add(*requirement)}}

	// Partition each resource once; snapshot assembly never scans another LWS's
	// Pods. Leaders are also indexed by name, independently of their label.
	var statefulSets appsv1.StatefulSetList
	if err := listAll(ctx, reader, &statefulSets, options...); err != nil {
		return err
	}
	for i := range statefulSets.Items {
		sts := &statefulSets.Items[i]
		if resources := byName[sts.Labels[leaderworkersetv1.SetNameLabelKey]]; resources != nil {
			resources.workerSets = append(resources.workerSets, *sts)
		}
		if resources := byName[sts.Name]; resources != nil && resources.lws.Spec.GroupIdentity != leaderworkersetv1.GroupIdentityHash {
			resources.leader = sts
		}
	}
	if hasHash {
		var deployments appsv1.DeploymentList
		if err := listAll(ctx, reader, &deployments, options...); err != nil {
			return err
		}
		for i := range deployments.Items {
			deployment := &deployments.Items[i]
			if resources := byName[deployment.Name]; resources != nil && resources.lws.Spec.GroupIdentity == leaderworkersetv1.GroupIdentityHash {
				resources.leader = deployment
			}
		}
	}
	// Observe finds leaders by name even when their label is missing or wrong.
	// Preserve that contract, reading fallback Deployments before ReplicaSets.
	for _, resources := range byName {
		if resources.leader != nil {
			continue
		}
		var leader client.Object = &appsv1.StatefulSet{}
		if resources.lws.Spec.GroupIdentity == leaderworkersetv1.GroupIdentityHash {
			leader = &appsv1.Deployment{}
		}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(resources.lws), leader); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("reading leader workload: %w", err)
			}
			continue
		}
		resources.leader = leader
	}
	if hasHash {
		var replicaSets appsv1.ReplicaSetList
		if err := listAll(ctx, reader, &replicaSets, options...); err != nil {
			return err
		}
		for _, rs := range replicaSets.Items {
			if resources := byName[rs.Labels[leaderworkersetv1.SetNameLabelKey]]; resources != nil {
				resources.replicaSets = append(resources.replicaSets, rs)
			}
		}
	}
	var pods corev1.PodList
	if err := listAll(ctx, reader, &pods, options...); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if resources := byName[pod.Labels[leaderworkersetv1.SetNameLabelKey]]; resources != nil {
			resources.pods = append(resources.pods, pod)
		}
	}
	for _, resources := range byName {
		snapshot, err := buildSnapshot(resources.lws, resources.leader, resources.replicaSets, resources.workerSets, resources.pods)
		if err != nil {
			return err
		}
		result[resources.lws.UID] = snapshot
	}
	return nil
}

// listAll bounds individual responses without treating a partial page as a
// complete observation (which could grant credit to groups awaiting deletion).
func listAll(ctx context.Context, reader client.Reader, list client.ObjectList, options ...client.ListOption) error {
	query := (&client.ListOptions{}).ApplyOptions(options)
	if query.Raw != nil {
		query.Raw = query.Raw.DeepCopy()
	}
	query.Limit, query.Continue = 500, ""
	pages := pager.New(func(ctx context.Context, pageOptions metav1.ListOptions) (runtime.Object, error) {
		// A fresh destination keeps prior Items and continuation metadata out
		// of the next decode. list stays empty until every page has been read.
		page := list.DeepCopyObject().(client.ObjectList)
		request := *query
		request.Raw = &pageOptions
		request.Limit, request.Continue = pageOptions.Limit, pageOptions.Continue
		return page, reader.List(ctx, page, &request)
	})
	// An expired observation needs a retry, not an unbounded replacement read.
	pages.FullListIfExpired = false
	all, _, err := pages.List(ctx, *query.AsListOptions())
	if err != nil {
		return fmt.Errorf("listing %T: %w", list, err)
	}
	items, err := meta.ExtractList(all)
	if err != nil {
		return err
	}
	return meta.SetList(list, items)
}
