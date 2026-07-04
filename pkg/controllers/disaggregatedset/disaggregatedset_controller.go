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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	disaggregatedsetutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

// DisaggregatedSetReconciler reconciles a DisaggregatedSet object
type DisaggregatedSetReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Record         events.EventRecorder
	LWSManager     *LeaderWorkerSetManager
	ServiceManager *ServiceManager
}

// +kubebuilder:rbac:groups=disaggregatedset.x-k8s.io,resources=disaggregatedsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=disaggregatedset.x-k8s.io,resources=disaggregatedsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=disaggregatedset.x-k8s.io,resources=disaggregatedsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=disaggregatedset.x-k8s.io,resources=disaggregatedsetrolescalers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=disaggregatedset.x-k8s.io,resources=disaggregatedsetrolescalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *DisaggregatedSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	disaggregatedSet := &disaggregatedsetv1.DisaggregatedSet{}
	if err := r.Get(ctx, req.NamespacedName, disaggregatedSet); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling DisaggregatedSet", "name", disaggregatedSet.Name, "namespace", disaggregatedSet.Namespace)

	// Reconcile proceeds in five steps:
	// 1. Compute the target revision from the current spec.
	// 2. Load scalers targeting this DisaggregatedSet (one per External role).
	// 3. Clean up fully-drained old revisions (all roles at 0 replicas).
	// 4. Reconcile LWS objects — either a rolling update (if old revisions with
	//    replicas exist) or a simple create/scale to the target revision.
	// 5. Reconcile services so that ready revisions get headless services.
	// 6. Write back scaler status (observed replicas, selector, conditions) and
	//    update the DisaggregatedSet's WaitingForScaler condition.

	// Step 1: Compute the target revision hash from the spec's role templates.
	revision := disaggregatedsetutils.ComputeRevision(disaggregatedSet.Spec.Roles)

	// Step 2: Load scalers. `allScalers` is the full list (used for status
	// writeback and owner-ref bookkeeping); `scalers` is keyed by role with
	// conflicting entries removed (used for replica resolution).
	scalers, allScalers, err := r.loadScalersForDS(ctx, disaggregatedSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileScalerOwnerRefs(ctx, disaggregatedSet, allScalers); err != nil {
		return ctrl.Result{}, err
	}

	// Step 3: Delete LWS objects for old revisions where every role has been
	// drained to 0 replicas. This runs before the rolling update so that
	// completed drains are finalized promptly.
	if err := r.cleanupDrainedLWS(ctx, disaggregatedSet, revision); err != nil {
		return ctrl.Result{}, err
	}

	// Step 4: Reconcile LWS objects.
	executor := r.createRollingUpdateExecutor()

	oldRevisions, _, err := executor.LWSManager.GetRevisionRolesList(ctx, disaggregatedSet.Namespace, disaggregatedSet.Name, revision)
	if err != nil {
		return ctrl.Result{}, err
	}

	var result ctrl.Result
	roleNames := disaggregatedsetutils.GetRoleNames(disaggregatedSet)
	totalOldReplicas := 0
	for _, roleName := range roleNames {
		totalOldReplicas += oldRevisions.GetTotalReplicasPerRole(roleName)
	}
	// If old revisions exist with running replicas, a rolling update is in
	// progress or needs to start. Otherwise, reconcileSimple creates/scales
	// LWS objects directly for the target revision (steady-state path).
	if len(oldRevisions) > 0 && totalOldReplicas > 0 {
		result, err = executor.ReconcileRollingUpdateNew(ctx, disaggregatedSet, revision, scalers)
		if err != nil {
			return result, err
		}
	} else {
		result, err = r.reconcileSimple(ctx, disaggregatedSet, revision, scalers)
		if err != nil {
			return result, err
		}
	}

	// Step 5: Reconcile headless services for revisions that are ready on all roles.
	allLWS, err := r.LWSManager.List(ctx, disaggregatedSet.Namespace, disaggregatedSet.Name, "")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list LWS for service reconciliation: %w", err)
	}
	revisionRoles := disaggregatedsetutils.GroupByRevision(allLWS)

	if err := r.ServiceManager.ReconcileServices(ctx, disaggregatedSet, revisionRoles, revision); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile services: %w", err)
	}

	// Step 6: Refresh scaler status + WaitingForScaler condition on the
	// DisaggregatedSet. Selector is rewritten here on every reconcile, which
	// is how HPA/KEDA metrics remain correct across rolling updates.
	newRevision := findNewRevisionRoles(revisionRoles, revision)
	if err := r.writeBackScalerStatus(ctx, disaggregatedSet, allScalers, newRevision); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.updateDSWaitingCondition(ctx, disaggregatedSet, scalers); err != nil {
		return ctrl.Result{}, err
	}

	return result, nil
}

// findNewRevisionRoles returns the RevisionRoles entry matching `revision`,
// or nil if the new revision has not been materialized yet.
func findNewRevisionRoles(all disaggregatedsetutils.RevisionRolesList, revision string) *disaggregatedsetutils.RevisionRoles {
	for i := range all {
		if all[i].Revision == revision {
			return &all[i]
		}
	}
	return nil
}

// reconcileScalerOwnerRefs ensures each scaler carries a non-controller
// owner reference to the DisaggregatedSet, so scaler CRs are GC'd on set
// deletion without transferring ownership away from the user.
func (r *DisaggregatedSetReconciler) reconcileScalerOwnerRefs(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	scalers []*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) error {
	for _, s := range scalers {
		if err := r.ensureScalerOwnerRef(ctx, ds, s); err != nil {
			return err
		}
	}
	return nil
}

// updateDSWaitingCondition sets the WaitingForScaler condition on the
// DisaggregatedSet reflecting whether every External-mode role has a
// matching ready scaler. Uses a single status patch.
func (r *DisaggregatedSetReconciler) updateDSWaitingCondition(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	scalers map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) error {
	waiting := false
	var missing []string
	for _, role := range ds.Spec.Roles {
		if role.Scaling == nil || role.Scaling.Mode != disaggregatedsetv1.RoleScalingExternal {
			continue
		}
		s, ok := scalers[role.Name]
		if !ok || s.Spec.Replicas == nil {
			waiting = true
			missing = append(missing, role.Name)
		}
	}

	message := ""
	if waiting {
		message = fmt.Sprintf("Roles awaiting a ready DisaggregatedSetRoleScaler: %v", missing)
	}
	updated := ds.DeepCopy()
	setDSWaitingForScalerCondition(updated, waiting, message)
	if conditionSlicesEqual(ds.Status.Conditions, updated.Status.Conditions) {
		return nil
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFrom(ds)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to update DisaggregatedSet WaitingForScaler condition: %w", err)
	}
	return nil
}

// conditionSlicesEqual reports whether two condition slices carry the same
// (type, status, reason, message) tuples. LastTransitionTime is ignored.
func conditionSlicesEqual(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	byType := make(map[string]metav1.Condition, len(a))
	for _, c := range a {
		byType[c.Type] = c
	}
	for _, c := range b {
		other, ok := byType[c.Type]
		if !ok {
			return false
		}
		if other.Status != c.Status || other.Reason != c.Reason || other.Message != c.Message {
			return false
		}
	}
	return true
}

// scalerNotReadyOrErr returns nil if err is ErrScalerNotReady, otherwise err.
func scalerNotReadyOrErr(err error) error {
	if errors.Is(err, ErrScalerNotReady) {
		return nil
	}
	return err
}

func (r *DisaggregatedSetReconciler) createRollingUpdateExecutor() *RollingUpdateExecutor {
	return &RollingUpdateExecutor{
		Client:     r.Client,
		Record:     r.Record,
		LWSManager: r.LWSManager,
	}
}

//nolint:unparam // Result is always empty but signature matches controller-runtime pattern
func (r *DisaggregatedSetReconciler) reconcileSimple(
	ctx context.Context,
	disaggregatedSet *disaggregatedsetv1.DisaggregatedSet,
	revision string,
	scalers map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) (ctrl.Result, error) {
	roleConfigs := disaggregatedsetutils.GetRoleConfigs(disaggregatedSet)

	for role, config := range roleConfigs {
		if err := r.reconcileRoleSimple(ctx, disaggregatedSet, role, config, revision, scalers); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to reconcile %s role: %w", role, err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *DisaggregatedSetReconciler) reconcileRoleSimple(
	ctx context.Context,
	disaggregatedSet *disaggregatedsetv1.DisaggregatedSet,
	role string,
	config *disaggregatedsetv1.DisaggregatedRoleSpec,
	revision string,
	scalers map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) error {
	log := logf.FromContext(ctx)

	lwsName := disaggregatedsetutils.GenerateName(disaggregatedSet.Name, role, revision)
	labels := disaggregatedsetutils.GenerateLabels(disaggregatedSet.Name, role, revision)

	existing, err := r.LWSManager.Get(ctx, disaggregatedSet.Namespace, lwsName)
	if err != nil {
		return fmt.Errorf("failed to get LWS %s: %w", lwsName, err)
	}

	// Resolve the desired replica count. For Static roles this reads the
	// inline spec.replicas; for External roles it reads the scaler.
	targetReplicas, err := getTargetReplicas(disaggregatedSet, role, scalers)
	if err != nil {
		// External role without a ready scaler.
		if existing == nil {
			// Create the LWS at 0 replicas so labels/services are set up.
			// The next reconcile (triggered by scaler creation) will scale
			// it up. Non-External roles never reach this branch.
			log.Info("Creating LWS at 0 replicas (waiting for scaler)", "role", role, "name", lwsName)
			return r.LWSManager.Create(ctx, disaggregatedsetutils.CreateParams{
				DisaggregatedSet: disaggregatedSet,
				Role:             role,
				Config:           config,
				Revision:         revision,
				Labels:           labels,
				Replicas:         0,
			})
		}
		// Scaler was deleted after the LWS came up; hold at current count
		// rather than scale to 0. User must recreate the scaler or switch
		// the role back to Static.
		return nil
	}

	desiredReplicas := int32(targetReplicas)

	if existing == nil {
		log.Info("Creating LWS", "role", role, "name", lwsName, "replicas", desiredReplicas)
		return r.LWSManager.Create(ctx, disaggregatedsetutils.CreateParams{
			DisaggregatedSet: disaggregatedSet,
			Role:             role,
			Config:           config,
			Revision:         revision,
			Labels:           labels,
			Replicas:         int(desiredReplicas),
		})
	}

	existingReplicas := int32(1)
	if existing.Spec.Replicas != nil {
		existingReplicas = *existing.Spec.Replicas
	}
	if existingReplicas != desiredReplicas {
		log.Info("Scaling LWS", "role", role, "name", lwsName, "from", existingReplicas, "to", desiredReplicas)
		if err := r.LWSManager.Scale(ctx, disaggregatedSet.Namespace, lwsName, int(desiredReplicas)); err != nil {
			return fmt.Errorf("failed to scale LWS %s: %w", lwsName, err)
		}
	}

	return nil
}

// cleanupDrainedLWS deletes all LWS objects for old revisions where every role
// has been drained to 0 replicas. This ensures coordinated cleanup: we only
// delete a revision's LWS objects when ALL roles (prefill, decode, etc.) have
// finished draining, preventing partial teardown during rolling updates.
func (r *DisaggregatedSetReconciler) cleanupDrainedLWS(ctx context.Context, disaggregatedSet *disaggregatedsetv1.DisaggregatedSet, revision string) error {
	log := logf.FromContext(ctx)

	lwsList, err := r.LWSManager.List(ctx, disaggregatedSet.Namespace, disaggregatedSet.Name, "")
	if err != nil {
		return fmt.Errorf("failed to list LWS for cleanup: %w", err)
	}

	// revisionReplicas maps revision -> role -> replica count.
	// Used to check if all roles of a revision have been drained to 0.
	revisionReplicas := make(map[string]map[string]int)
	for _, lws := range lwsList {
		lwsRevision := lws.Labels[disaggregatedsetv1.RevisionLabelKey]
		if lwsRevision == revision {
			continue
		}
		if revisionReplicas[lwsRevision] == nil {
			revisionReplicas[lwsRevision] = make(map[string]int)
		}
		lwsRole := lws.Labels[disaggregatedsetv1.RoleLabelKey]
		if _, exists := revisionReplicas[lwsRevision][lwsRole]; exists {
			log.Info("WARNING: multiple LWS found for same role and revision",
				"role", lwsRole, "revision", lwsRevision, "lws", lws.Name)
		}
		lwsReplicas := 0
		if lws.Spec.Replicas != nil {
			lwsReplicas = int(*lws.Spec.Replicas)
		}
		revisionReplicas[lwsRevision][lwsRole] = lwsReplicas
	}

	for oldRevision, roles := range revisionReplicas {
		allDrained := true
		for _, replicas := range roles {
			if replicas != 0 {
				allDrained = false
				break
			}
		}
		if !allDrained {
			continue
		}

		for roleName := range roles {
			lwsName := disaggregatedsetutils.GenerateName(disaggregatedSet.Name, roleName, oldRevision)
			log.Info("Deleting drained LWS", "name", lwsName)
			if err := r.LWSManager.Delete(ctx, disaggregatedSet.Namespace, lwsName); err != nil {
				return fmt.Errorf("failed to delete LWS %s: %w", lwsName, err)
			}
			r.Record.Eventf(disaggregatedSet, nil, corev1.EventTypeNormal, EventReasonLWSDeleted,
				"Delete", "Deleted drained LWS %s", lwsName)
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DisaggregatedSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.LWSManager == nil {
		r.LWSManager = NewLeaderWorkerSetManager(mgr.GetClient())
	}

	if r.ServiceManager == nil {
		r.ServiceManager = NewServiceManager(mgr.GetClient(), mgr.GetScheme())
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&disaggregatedsetv1.DisaggregatedSet{}).
		Owns(&leaderworkersetv1.LeaderWorkerSet{}).
		Watches(
			&disaggregatedsetv1.DisaggregatedSetRoleScaler{},
			handler.EnqueueRequestsFromMapFunc(scalerToDSKey),
		).
		Named("disaggregatedset").
		Complete(r)
}
