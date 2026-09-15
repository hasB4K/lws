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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	disaggregatedsetv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	disaggregatedsetutils "sigs.k8s.io/lws/pkg/utils/disaggregatedset"
)

const (
	EventReasonRollingUpdateStarted   = "RollingUpdateStarted"
	EventReasonRollingUpdateCompleted = "RollingUpdateCompleted"
	EventReasonScalingUp              = "ScalingUp"
	EventReasonScalingDown            = "ScalingDown"
	EventReasonLWSDeleted             = "LWSDeleted"
	EventReasonWaitingForServiceGC    = "WaitingForServiceGarbageCollection"
)

type RollingUpdateExecutor struct {
	Record     events.EventRecorder
	LWSManager *LeaderWorkerSetManager
}

// roleRolloutSnapshot is rebuilt from the observed LWS objects on every
// reconcile. It is not persisted by the controller.
//
// OldIntendedReplicas is the largest intended-replicas value for this role
// across all old revisions. It is the stable old capacity that the rollout
// must replace. The Spec fields count work sent to the cluster, including pods
// that are still starting. The Ready fields count serving capacity and exclude
// replicas already committed to termination.
type roleRolloutSnapshot struct {
	OldIntendedReplicas int
	OldSpecReplicas     int
	OldReadyReplicas    int
	NewSpecReplicas     int
	NewReadyReplicas    int
	NewTargetReplicas   int
	Config              RollingUpdateConfig
}

// rolloutSnapshot is index-aligned with the role-name slice used by the caller.
type rolloutSnapshot []roleRolloutSnapshot

// ReconcileRollingUpdateNew is the entry point for rolling update reconciliation.
// It fetches current cluster state and either:
//  1. Starts a new rolling update (initRollingUpdate) if no LWS for the target
//     revision exist yet, or
//  2. Continues an in-progress rolling update (ReconcileRollingUpdate) by
//     computing and executing the next scale step.
//
// scalersByRole contains the controller-owned scaler for each External role.
// Static roles do not use this map.
func (executor *RollingUpdateExecutor) ReconcileRollingUpdateNew(
	ctx context.Context,
	disaggregatedSet *disaggregatedsetv1.DisaggregatedSet,
	slice int,
	revision string,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	roleNames := disaggregatedsetutils.GetRoleNames(disaggregatedSet)
	roleConfigs := disaggregatedsetutils.GetRoleConfigs(disaggregatedSet)

	oldRevisions, newRevision, err := executor.LWSManager.GetRevisionRolesList(ctx, disaggregatedSet, slice, revision)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(oldRevisions) == 0 {
		return ctrl.Result{}, nil
	}
	// Normalize the replica target stored on every old LWS before planning:
	//  1. Keep an existing intended-replicas value.
	//  2. Otherwise, copy the legacy initial-replicas value to intended-replicas.
	//  3. If neither value is valid, use the current Spec as the best available
	//     fallback.
	//
	// The legacy annotation remains in place for compatibility. This must happen
	// before draining because an old revision's Spec may already be smaller than
	// the replica count it was originally intended to reach.
	if err := executor.ensureOldIntendedReplicas(ctx, disaggregatedSet, oldRevisions); err != nil {
		return ctrl.Result{}, err
	}

	addedRoles, removedRoles := detectRoleChanges(roleNames, oldRevisions)
	if len(addedRoles) > 0 || len(removedRoles) > 0 {
		log.Info("Role changes detected", "added", addedRoles, "removed", removedRoles)
	}

	if newRevision == nil {
		return executor.initRollingUpdate(ctx, disaggregatedSet, slice, revision, roleNames, roleConfigs, scalersByRole)
	}

	// Continuing a rollout updates the LWS objects discovered above by their
	// actual names. The slice is only needed to find or create those objects.
	return executor.ReconcileRollingUpdate(ctx, disaggregatedSet, oldRevisions, *newRevision, scalersByRole)
}

func (executor *RollingUpdateExecutor) initRollingUpdate(
	ctx context.Context,
	disaggregatedSet *disaggregatedsetv1.DisaggregatedSet,
	slice int,
	revision string,
	roleNames []string,
	roleConfigs map[string]*disaggregatedsetv1.DisaggregatedRoleSpec,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Initiating new rolling update", "revision", revision)
	executor.Record.Eventf(disaggregatedSet, nil, corev1.EventTypeNormal, EventReasonRollingUpdateStarted,
		"Update", "Started rolling update to revision %s", revision)

	// Create one LWS per role for the target revision:
	//  1. Start the LWS at 0 replicas. The executor grows it on later reconciles.
	//  2. Store the role's final target in intended-replicas.
	//
	// If another rollout interrupts this one, the stored target becomes this
	// revision's contribution to the old-side baseline. Its partially scaled
	// Spec does not replace that target.
	for _, roleName := range roleNames {
		intendedReplicas := getTargetReplicas(disaggregatedSet, roleName, scalersByRole, 0)
		if _, err := executor.ensureNewLWSExists(ctx, disaggregatedSet, slice, revision, roleName, roleConfigs[roleName], 0, intendedReplicas); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// ReconcileRollingUpdate executes one step of an in-progress rolling update:
//  1. Refresh the current revision's intended replica targets.
//  2. Build a snapshot of issued and Ready replicas for every role.
//  3. Ask the planner for the next old and new Spec counts.
//  4. Limit that plan using current readiness, surge, and availability.
//  5. Drain old revisions newest-first, then grow the current revision.
//
// Object updates and a one-second timer trigger the next step. The rollout is
// complete only after the old Specs reach zero and the target revision is Ready.
func (executor *RollingUpdateExecutor) ReconcileRollingUpdate(
	ctx context.Context,
	disaggregatedSet *disaggregatedsetv1.DisaggregatedSet,
	oldRevisions disaggregatedsetutils.RevisionRolesList,
	newRevision disaggregatedsetutils.RevisionRoles,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	specRoleNames := disaggregatedsetutils.GetRoleNames(disaggregatedSet)
	specRoleSet, oldRoleSet := buildRoleSets(specRoleNames, oldRevisions)
	if err := executor.syncTargetIntendedReplicas(ctx, disaggregatedSet, specRoleNames, newRevision, scalersByRole); err != nil {
		return ctrl.Result{}, err
	}

	allRoleNames := append(slices.Clone(specRoleNames), removedRoleNames(oldRoleSet, specRoleSet)...)
	config := extractRollingUpdateConfig(disaggregatedSet, allRoleNames, scalersByRole)
	snapshot := buildRolloutSnapshot(disaggregatedSet, allRoleNames, specRoleSet, oldRevisions, newRevision, scalersByRole, config)
	initialOld, currentOld, currentNewSpec, targetNew := plannerInputs(snapshot)

	if isComplete(currentOld, currentNewSpec, targetNew) {
		if !isRolloutReady(snapshot) {
			log.V(1).Info("Waiting for target revision to become ready")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		log.Info("Rolling update complete")
		executor.Record.Eventf(disaggregatedSet, nil, corev1.EventTypeNormal, EventReasonRollingUpdateCompleted,
			"Update", "Completed rolling update to revision %s", newRevision.Revision)
		return ctrl.Result{}, nil
	}
	nextStep := ComputeNextStep(initialOld, currentOld, currentNewSpec, targetNew, config)
	if nextStep == nil {
		log.Info("Rolling update is temporarily blocked; waiting for state to change")
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	nextStep.New = boundNewReplicaTargets(snapshot, nextStep.New)
	ensureExecutableStep(snapshot, nextStep)

	log.Info("Next step computed", buildStepLogArgs(allRoleNames, nextStep)...)
	newGrowthPlanned, newReadinessPending := false, false
	for i := range allRoleNames {
		if nextStep.New[i] > snapshot[i].NewSpecReplicas {
			newGrowthPlanned = true
		}
		newReadinessPending = newReadinessPending || snapshot[i].NewReadyReplicas < snapshot[i].NewSpecReplicas
	}

	// Scale down old replicas before scaling up new ones. This ordering ensures
	// the total replica count never exceeds the surge limit between the two
	// API calls: e.g. with surge=0, scaling up first would briefly make
	// (currentOld + nextStep.New) exceed the target before scaleDownOld brings
	// currentOld down.
	allowUncoordinatedDrain := !newGrowthPlanned && !newReadinessPending
	if err := executor.scaleDownOld(ctx, disaggregatedSet, oldRevisions, allRoleNames, snapshot, nextStep.Past, allowUncoordinatedDrain); err != nil {
		return ctrl.Result{}, err
	}
	if err := executor.scaleUpNew(ctx, disaggregatedSet, newRevision, specRoleNames, nextStep.New); err != nil {
		return ctrl.Result{}, err
	}

	// Object updates normally trigger the next reconcile immediately. The
	// timer also covers a legal no-op while pending replicas become Ready.
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// --- Helpers ---

func buildRoleSets(specRoleNames []string, oldRevisions disaggregatedsetutils.RevisionRolesList) (spec, old map[string]bool) {
	spec = make(map[string]bool, len(specRoleNames))
	for _, name := range specRoleNames {
		spec[name] = true
	}
	old = make(map[string]bool)
	for _, wl := range oldRevisions {
		for name := range wl.Roles {
			old[name] = true
		}
	}
	return spec, old
}

func removedRoleNames(oldRoleSet, specRoleSet map[string]bool) []string {
	var removed []string
	for role := range oldRoleSet {
		if !specRoleSet[role] {
			removed = append(removed, role)
		}
	}
	slices.Sort(removed)
	return removed
}

func buildRolloutSnapshot(
	ds *disaggregatedsetv1.DisaggregatedSet,
	allRoleNames []string,
	specRoleSet map[string]bool,
	oldRevisions disaggregatedsetutils.RevisionRolesList,
	newRevision disaggregatedsetutils.RevisionRoles,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
	config []RollingUpdateConfig,
) rolloutSnapshot {
	snapshot := make(rolloutSnapshot, len(allRoleNames))

	for i, roleName := range allRoleNames {
		roleState := roleRolloutSnapshot{
			OldIntendedReplicas: oldRevisions.GetMaxIntendedReplicasPerRole(roleName),
			OldSpecReplicas:     oldRevisions.GetTotalReplicasPerRole(roleName),
			Config:              config[i],
		}
		for _, revision := range oldRevisions {
			if lws := revision.Roles[roleName]; lws != nil {
				roleState.OldReadyReplicas += committedReadyReplicas(lws)
			}
		}

		if specRoleSet[roleName] {
			lws := newRevision.Roles[roleName]
			if lws != nil {
				roleState.NewSpecReplicas = int(getLWSReplicas(lws))
				roleState.NewReadyReplicas = committedReadyReplicas(lws)
			}
			roleState.NewTargetReplicas = getTargetReplicas(ds, roleName, scalersByRole, roleState.NewSpecReplicas)
			// No-shrink guard: an External role mid-rollout must not shrink the
			// new-revision fleet if HPA writes a smaller value while the old
			// revision is still draining. Releases once the rollout completes.
			if isExternal(ds, roleName) && len(oldRevisions) > 0 && lws != nil {
				roleState.NewTargetReplicas = max(roleState.NewTargetReplicas, roleState.NewSpecReplicas)
			}
		}
		snapshot[i] = roleState
	}

	return snapshot
}

// plannerInputs projects the rollout snapshot onto the four replica vectors
// used by the planner. Ready counts and safety limits remain in the executor.
func plannerInputs(snapshot rolloutSnapshot) (initialOld, currentOld, currentNew, targetNew RoleReplicaState) {
	initialOld = make(RoleReplicaState, len(snapshot))
	currentOld = make(RoleReplicaState, len(snapshot))
	currentNew = make(RoleReplicaState, len(snapshot))
	targetNew = make(RoleReplicaState, len(snapshot))
	for i, role := range snapshot {
		initialOld[i] = role.OldIntendedReplicas
		currentOld[i] = role.OldSpecReplicas
		currentNew[i] = role.NewSpecReplicas
		targetNew[i] = role.NewTargetReplicas
	}
	return
}

// getTargetReplicas resolves the desired replica count. External roles read
// spec.replicas from the scaler (always materialised since the CRD defaults it
// to 0 and the controller seeds it at creation to avoid draining a running
// Static→External flip).
func getTargetReplicas(ds *disaggregatedsetv1.DisaggregatedSet, roleName string, scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler, currentNewSpec int) int {
	for _, p := range ds.Spec.Roles {
		if p.Name != roleName {
			continue
		}
		if p.Scaling != nil && p.Scaling.Mode == disaggregatedsetv1.RoleScalingExternal {
			if s := scalersByRole[roleName]; s != nil {
				return int(s.Spec.Replicas)
			}
			return currentNewSpec
		}
		if p.Spec.Replicas == nil {
			return 1
		}
		return int(*p.Spec.Replicas)
	}
	return 1
}

func isExternal(ds *disaggregatedsetv1.DisaggregatedSet, roleName string) bool {
	for _, p := range ds.Spec.Roles {
		if p.Name == roleName {
			return p.Scaling != nil && p.Scaling.Mode == disaggregatedsetv1.RoleScalingExternal
		}
	}
	return false
}

func extractRollingUpdateConfig(
	ds *disaggregatedsetv1.DisaggregatedSet,
	allRoleNames []string,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) []RollingUpdateConfig {
	config := make([]RollingUpdateConfig, len(allRoleNames))
	roleIndex := make(map[string]int, len(allRoleNames))
	for i, name := range allRoleNames {
		config[i].MaxSurge = 1
		roleIndex[name] = i
	}

	for _, role := range ds.Spec.Roles {
		if rc := role.Spec.RolloutStrategy.RollingUpdateConfiguration; rc != nil {
			// For External roles this returns the scaler value (or currentNewSpec=0
			// if none is available); percentages against 0 collapse to 0, which
			// matches how a paused rollout should behave.
			replicas := getTargetReplicas(ds, role.Name, scalersByRole, 0)
			// Use GetScaledValueFromIntOrPercent to handle both integers and percentages.
			// For maxSurge, round up (true); for maxUnavailable, round down (false).
			surge, _ := intstr.GetScaledValueFromIntOrPercent(&rc.MaxSurge, replicas, true)
			unavail, _ := intstr.GetScaledValueFromIntOrPercent(&rc.MaxUnavailable, replicas, false)
			cfg := RollingUpdateConfig{MaxSurge: 1, MaxUnavailable: 0}
			if unavail > 0 {
				cfg.MaxUnavailable = unavail
				cfg.MaxSurge = surge
			} else if surge > 0 {
				cfg.MaxSurge = surge
			}
			config[roleIndex[role.Name]] = cfg
		}
	}
	return config
}

func buildStepLogArgs(roleNames []string, step *UpdateStep) []interface{} {
	args := make([]interface{}, 0, len(roleNames)*4)
	for i, name := range roleNames {
		args = append(args,
			"past_"+name, step.Past[i],
			"new_"+name, step.New[i],
		)
	}
	return args
}

// committedReadyReplicas is the availability that may safely authorize
// another scale-down. Status can temporarily report more Ready replicas than
// Spec after a previous scale-down; those excess replicas are already
// committed to termination and must not be spent a second time.
func committedReadyReplicas(lws *leaderworkersetv1.LeaderWorkerSet) int {
	if lws == nil {
		return 0
	}
	return max(0, min(int(lws.Status.ReadyReplicas), int(getLWSReplicas(lws))))
}

func isRolloutReady(snapshot rolloutSnapshot) bool {
	for _, role := range snapshot {
		if role.OldSpecReplicas != 0 || role.NewReadyReplicas < role.NewTargetReplicas {
			return false
		}
	}
	return true
}

// boundNewReplicaTargets applies the executor's hard limits to a planner
// proposal. A role may only consume surge headroom that exists in the current
// Spec footprint, and may only have its pending allowance issued-but-not-ready
// replicas. Existing Spec is never reduced here, even if an externally
// modified object is already outside either bound.
func boundNewReplicaTargets(
	snapshot rolloutSnapshot,
	proposed RoleReplicaState,
) RoleReplicaState {
	provisional := make(RoleReplicaState, len(snapshot))
	budgetSteps := 0
	for _, role := range snapshot {
		budgetSteps = max(budgetSteps, role.OldIntendedReplicas, role.NewTargetReplicas)
	}
	for i, roleState := range snapshot {
		roleSize := max(roleState.OldIntendedReplicas, roleState.NewTargetReplicas)
		maxBySurge := roleSize + roleState.Config.MaxSurge - roleState.OldSpecReplicas
		pendingAllowance := projectBudget(
			roleSize,
			roleState.Config.MaxSurge+roleState.Config.MaxUnavailable,
			budgetSteps,
		)
		maxByPending := roleState.NewReadyReplicas + pendingAllowance
		upperBound := max(roleState.NewSpecReplicas, min(maxBySurge, maxByPending))
		provisional[i] = max(roleState.NewSpecReplicas, min(proposed[i], upperBound))
	}

	// Per-role readiness clamps can trim different parts of the proposal. Keep
	// the resulting progress within one largestReplicaFraction of the slowest
	// role. Integer cross-products keep this exact without rational-number state.
	slowCount, slowTarget, minTarget := 0, 0, 0
	for i, role := range snapshot {
		target := role.NewTargetReplicas
		if target <= 0 {
			continue
		}
		if minTarget == 0 || target < minTarget {
			minTarget = target
		}
		if slowTarget == 0 || int64(provisional[i])*int64(slowTarget) < int64(slowCount)*int64(target) {
			slowCount, slowTarget = provisional[i], target
		}
	}
	bounded := make(RoleReplicaState, len(snapshot))
	for i, roleState := range snapshot {
		coordinatedTarget := roleState.NewTargetReplicas
		if minTarget > 0 {
			coordinatedTarget = replicaLimit(roleState.NewTargetReplicas, slowCount, slowTarget, minTarget)
		}
		bounded[i] = max(roleState.NewSpecReplicas, min(provisional[i], coordinatedTarget))
	}
	return bounded
}

// replicaLimit returns floor(target * (slowCount/slowTarget + 1/minTarget)).
// Each multiplication is bounded by two replica counts, which originate from
// int32 API fields and therefore fit in int64.
func replicaLimit(target, slowCount, slowTarget, minTarget int) int {
	base := int64(target) * int64(slowCount)
	whole, remainder := base/int64(slowTarget), base%int64(slowTarget)
	extra := (remainder*int64(minTarget) + int64(target)*int64(slowTarget)) /
		(int64(slowTarget) * int64(minTarget))
	return min(target, int(whole+extra))
}

// maxSafeDrain returns the number of old replicas that may be removed without
// crossing the raw MaxUnavailable availability floor. Ready is capped at Spec
// while the snapshot is built, so terminating replicas cannot be spent twice.
func maxSafeDrain(role roleRolloutSnapshot) int {
	floor := max(0, min(role.OldIntendedReplicas, role.NewTargetReplicas)-role.Config.MaxUnavailable)
	return min(role.OldSpecReplicas, max(0, role.OldReadyReplicas+role.NewReadyReplicas-floor))
}

// The planner may pair a drain with growth that readiness/coordination bounds
// later remove. If that leaves a permanent no-op, spend one safe drain to open
// replacement capacity.
func ensureExecutableStep(snapshot rolloutSnapshot, step *UpdateStep) {
	for i, role := range snapshot {
		if step.New[i] > role.NewSpecReplicas || min(max(0, role.OldSpecReplicas-step.Past[i]), maxSafeDrain(role)) > 0 {
			return
		}
	}
	for i, role := range snapshot {
		if role.OldSpecReplicas > 0 && maxSafeDrain(role) > 0 {
			step.Past[i] = role.OldSpecReplicas - 1
			return
		}
	}
}

func maxTimestamp(wl disaggregatedsetutils.RevisionRoles) time.Time {
	var maxTS time.Time
	for _, lws := range wl.Roles {
		if lws.CreationTimestamp.Time.After(maxTS) {
			maxTS = lws.CreationTimestamp.Time
		}
	}
	return maxTS
}

func sortByNewestTimestamp(revisions disaggregatedsetutils.RevisionRolesList) disaggregatedsetutils.RevisionRolesList {
	sorted := slices.Clone(revisions)
	slices.SortFunc(sorted, func(a, b disaggregatedsetutils.RevisionRoles) int {
		return maxTimestamp(b).Compare(maxTimestamp(a))
	})
	return sorted
}

// --- Scaling operations ---

func (executor *RollingUpdateExecutor) scaleUpNew(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	newRevision disaggregatedsetutils.RevisionRoles,
	roleNames []string,
	targetNew RoleReplicaState,
) error {
	log := logf.FromContext(ctx)
	for i, name := range roleNames {
		lws := newRevision.Roles[name]
		if lws == nil {
			continue
		}
		currentSpec := int(getLWSReplicas(lws))
		desiredSpec := targetNew[i]
		if currentSpec >= desiredSpec {
			continue
		}
		lwsName := lws.Name
		log.Info("Scaling up", "lws", lwsName, "from_spec", currentSpec, "from_ready", committedReadyReplicas(lws), "to", desiredSpec)
		if err := executor.LWSManager.Scale(ctx, ds, lwsName, desiredSpec); err != nil {
			return fmt.Errorf("failed to scale %s: %w", lwsName, err)
		}
		executor.Record.Eventf(ds, nil, corev1.EventTypeNormal, EventReasonScalingUp,
			"Update", "Scaling up %s LWS %s from %d to %d replicas", name, lwsName, currentSpec, desiredSpec)
	}
	return nil
}

func (executor *RollingUpdateExecutor) scaleDownOld(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	oldRevisions disaggregatedsetutils.RevisionRolesList,
	roleNames []string,
	snapshot rolloutSnapshot,
	targetOld RoleReplicaState,
	allowUncoordinatedDrain bool,
) error {
	budget := make(RoleReplicaState, len(roleNames))
	for i := range roleNames {
		roleState := snapshot[i]
		budget[i] = max(0, min(roleState.OldSpecReplicas-targetOld[i], maxSafeDrain(roleState)))
	}

	log := logf.FromContext(ctx)
	for _, wl := range sortByNewestTimestamp(oldRevisions) {
		plannedDrain := make(RoleReplicaState, len(roleNames))
		for i, name := range roleNames {
			if lws := wl.Roles[name]; lws != nil {
				plannedDrain[i] = min(budget[i], int(getLWSReplicas(lws)))
			}
		}
		if !anyPositive(plannedDrain) {
			continue
		}

		coordinateRevisionDrain(roleNames, wl.Roles, plannedDrain, snapshot, allowUncoordinatedDrain)

		for i, name := range roleNames {
			lws := wl.Roles[name]
			if lws == nil || plannedDrain[i] == 0 {
				continue
			}
			replicas := int(getLWSReplicas(lws))
			drain := plannedDrain[i]
			newReplicas := replicas - drain
			// Address by the LWS's actual name so a legacy slice-0 object drains too.
			lwsName := lws.Name
			log.Info("Scaling down", "lws", lwsName, "from", replicas, "to", newReplicas)
			if err := executor.LWSManager.Scale(ctx, ds, lwsName, newReplicas); err != nil {
				return fmt.Errorf("failed to scale %s: %w", lwsName, err)
			}
			executor.Record.Eventf(ds, nil, corev1.EventTypeNormal, EventReasonScalingDown,
				"Update", "Scaling down %s LWS %s from %d to %d replicas", name, lwsName, replicas, newReplicas)
		}
		// Never move a budget past the newest revision that can consume it.
		return nil
	}

	return nil
}

// coordinateRevisionDrain keeps all roles in an old revision alive together
// when possible. It may retire the whole revision if every role fits within
// its availability budget. If strict coordination would make a legal rollout
// immobile, with no new replicas growing or pending readiness, the
// already-budgeted drain is allowed as a last resort. That fallback remains
// per-role availability-safe, but may leave the revision incomplete and
// therefore unsuitable for independent routing.
func coordinateRevisionDrain(
	roleNames []string,
	roles map[string]*leaderworkersetv1.LeaderWorkerSet,
	drain RoleReplicaState,
	snapshot rolloutSnapshot,
	allowUncoordinated bool,
) {
	anyAliveAfter, anyRetired, canRetire := false, false, true
	for i, name := range roleNames {
		lws := roles[name]
		if lws == nil || getLWSReplicas(lws) == 0 {
			continue
		}
		replicas := int(getLWSReplicas(lws))
		anyAliveAfter = anyAliveAfter || replicas > drain[i]
		anyRetired = anyRetired || drain[i] == replicas
		canRetire = canRetire && replicas <= maxSafeDrain(snapshot[i])
	}
	if !anyAliveAfter || !anyRetired {
		return
	}
	if canRetire {
		for i, name := range roleNames {
			if lws := roles[name]; lws != nil {
				drain[i] = int(getLWSReplicas(lws))
			}
		}
		return
	}

	hasPartialDrain := false
	for i, name := range roleNames {
		if lws := roles[name]; lws != nil {
			replicas := int(getLWSReplicas(lws))
			hasPartialDrain = hasPartialDrain || drain[i] > 0 && drain[i] < replicas
		}
	}
	if !hasPartialDrain && allowUncoordinated {
		return
	}
	for i, name := range roleNames {
		if lws := roles[name]; lws != nil && drain[i] == int(getLWSReplicas(lws)) {
			drain[i] = 0
		}
	}
}

// --- LWS creation ---

func (executor *RollingUpdateExecutor) ensureNewLWSExists(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	slice int,
	revision, role string,
	config *disaggregatedsetv1.DisaggregatedRoleSpec,
	startingReplicas int,
	intendedReplicas int,
) (bool, error) {
	lwsName := disaggregatedsetutils.GenerateName(ds.Name, slice, revision, role)
	existing, err := executor.LWSManager.Get(ctx, ds, lwsName)
	if err != nil {
		return false, fmt.Errorf("failed to get LWS %s: %w", lwsName, err)
	}
	if existing != nil {
		return false, nil
	}

	if err := executor.LWSManager.Create(ctx, disaggregatedsetutils.CreateParams{
		DisaggregatedSet: ds,
		Role:             role,
		Slice:            slice,
		Config:           config,
		Revision:         revision,
		Labels:           disaggregatedsetutils.GenerateLabels(ds.Name, slice, revision, role),
		Replicas:         startingReplicas,
		IntendedReplicas: &intendedReplicas,
	}); err != nil {
		return false, fmt.Errorf("failed to create LWS %s: %w", lwsName, err)
	}
	return true, nil
}

// ensureOldIntendedReplicas migrates legacy objects to intended-replicas. An
// existing value is immutable once the revision is old, even when its Spec has
// already been partially drained.
func (executor *RollingUpdateExecutor) ensureOldIntendedReplicas(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	oldRevisions disaggregatedsetutils.RevisionRolesList,
) error {
	for _, revision := range oldRevisions {
		for _, lws := range revision.Roles {
			if _, ok := disaggregatedsetutils.GetIntendedReplicasAnnotation(lws); ok {
				continue
			}
			intendedReplicas, ok := disaggregatedsetutils.GetIntendedReplicas(lws)
			if !ok {
				intendedReplicas = int32(getLWSReplicas(lws))
			}
			intended := int(intendedReplicas)
			if _, err := executor.LWSManager.SetIntendedReplicas(ctx, ds.Namespace, lws.Name, intended); err != nil {
				return fmt.Errorf("failed to backfill intended replicas on %s: %w", lws.Name, err)
			}
			disaggregatedsetutils.SetIntendedReplicas(lws, int32(intended))
		}
	}
	return nil
}

// syncTargetIntendedReplicas follows replica-only and external-scaler changes
// while a revision is current. The value freezes when that revision becomes
// old, preserving the target it would have reached had its rollout completed.
func (executor *RollingUpdateExecutor) syncTargetIntendedReplicas(
	ctx context.Context,
	ds *disaggregatedsetv1.DisaggregatedSet,
	roleNames []string,
	newRevision disaggregatedsetutils.RevisionRoles,
	scalersByRole map[string]*disaggregatedsetv1.DisaggregatedSetRoleScaler,
) error {
	for _, roleName := range roleNames {
		lws := newRevision.Roles[roleName]
		if lws == nil {
			continue
		}
		intended := getTargetReplicas(ds, roleName, scalersByRole, int(getLWSReplicas(lws)))
		current, ok := disaggregatedsetutils.GetIntendedReplicasAnnotation(lws)
		if ok && int(current) == intended {
			continue
		}
		if _, err := executor.LWSManager.SetIntendedReplicas(ctx, ds.Namespace, lws.Name, intended); err != nil {
			return fmt.Errorf("failed to update intended replicas on %s: %w", lws.Name, err)
		}
		disaggregatedsetutils.SetIntendedReplicas(lws, int32(intended))
	}
	return nil
}

// --- Role change utils ---
func detectRoleChanges(specRoleNames []string, oldRevisions disaggregatedsetutils.RevisionRolesList) ([]string, []string) {
	specRoles, oldRoles := buildRoleSets(specRoleNames, oldRevisions)

	var added, removed []string
	for name := range oldRoles {
		if !specRoles[name] {
			removed = append(removed, name)
		}
	}
	for _, name := range specRoleNames {
		if !oldRoles[name] {
			added = append(added, name)
		}
	}
	return added, removed
}
