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

// Package disaggregatedset provides rolling update planning and execution for DisaggregatedSet.
//
// # Rolling Update Algorithm — Percentage-Block Model
//
// Each revision is treated as a "block" with a progress percentage (0% → 100%).
// Each revision has its own minimalUnit (max(1/N_i) across its roles), and the
// global sync point interval is max(minimalUnit_old, minimalUnit_new).
//
// The planner uses a two-level step structure:
//
//   - Cross-role sync points (big steps): all roles must reach a sync point
//     before any can cross it.
//   - Per-role sub-steps (small steps): each role advances 1 replica at a time
//     between sync points, at its own granularity.
//
// Capacity constraints (per role):
//
//	ceiling: old + new <= max(initialOld, target) + maxSurge
//	floor:   old + new >= min(initialOld, target) - maxUnavailable
package disaggregatedset

// RoleStepState tracks a single role's position in the two-level step structure.
//
// The same struct is reused on both sides of an UpdateStep:
//   - In step.New[role], SyncWindowIndex/RoleStep refer to the NEW revision's
//     sync sequence (position scaling up toward target).
//   - In step.Past[role], SyncWindowIndex/RoleStep refer to the OLD revision's
//     sync sequence (position draining toward 0).
//
// The two sides have independent sync sequences (two-minU model), so the
// indices in step.New and step.Past are not directly comparable.
type RoleStepState struct {
	SyncWindowIndex int // sync point index this role has reached (side-specific)
	RoleStep        int // sub-step index within the current sync window
	Replicas        int // target replica count for this role at this step
}

// UpdateStep is the planner output: the target replica counts per role for
// old and new revisions, along with their position in the step structure.
type UpdateStep struct {
	Past map[string]RoleStepState
	New  map[string]RoleStepState
}

type RollingUpdateConfig struct {
	MaxSurge       int
	MaxUnavailable int
}

func DefaultRollingUpdateConfig(numRoles int) []RollingUpdateConfig {
	configs := make([]RollingUpdateConfig, numRoles)
	for i := range numRoles {
		configs[i].MaxSurge = 1
		configs[i].MaxUnavailable = 0
	}
	return configs
}

// frac represents an exact rational number to avoid float precision issues.
type frac struct {
	num, den int
}

func (f frac) mul(n int) frac {
	return frac{f.num * n, f.den}
}

func floorFrac(f frac) int {
	return f.num / f.den
}

func ceilFrac(f frac) int {
	return (f.num + f.den - 1) / f.den
}

// revisionMinimalUnit returns max(1/N_i) for a single revision's replica counts.
func revisionMinimalUnit(replicas map[string]int) frac {
	minN := 0
	for _, n := range replicas {
		if n > 0 && (minN == 0 || n < minN) {
			minN = n
		}
	}
	if minN == 0 {
		return frac{1, 1}
	}
	return frac{1, minN}
}

func numSyncPoints(unit frac) int {
	return unit.den / unit.num
}

// newSyncTarget returns the target new-replica count for a role at a given
// new-side sync point.
func newSyncTarget(target int, syncPercent frac) int {
	return floorFrac(frac{target * syncPercent.num, syncPercent.den})
}

// oldSyncTarget returns the target old-replica count for a role at a given
// old-side sync point. Old drains down, so this returns the REMAINING old count.
func oldSyncTarget(initialOld int, syncPercent frac) int {
	remaining := frac{initialOld * (syncPercent.den - syncPercent.num), syncPercent.den}
	return ceilFrac(remaining)
}

// sideDirection selects which "side" of a rolling update deriveSideProgress
// reports on. The two sides are mirror images of each other: NEW ramps up
// from 0 to target; OLD drains from initial down to 0.
type sideDirection int

const (
	sideUp   sideDirection = iota // NEW side: 0 → target
	sideDown                      // OLD side: initial → 0
)

// deriveSideProgress reports each role's position in its side's sync sequence.
// "N" is the role's far-end count (target for sideUp, initialOld for sideDown);
// "cur" is its current replica count. A role with N=0 is reported as fully done.
//
// SyncWindowIndex is the highest sync barrier the role has reached/crossed.
// RoleStep counts sub-step replicas advanced beyond that barrier toward the next.
func deriveSideProgress(roleNames []string, N, cur map[string]int, unit frac, dir sideDirection) map[string]RoleStepState {
	nSync := numSyncPoints(unit)
	states := make(map[string]RoleStepState, len(roleNames))

	for _, role := range roleNames {
		n := N[role]
		c := cur[role]
		if n == 0 {
			states[role] = RoleStepState{SyncWindowIndex: nSync, Replicas: c}
			continue
		}

		crossStep, roleStep := 0, 0
		for s := 1; s <= nSync; s++ {
			target := sideTargetAt(n, unit.mul(s), dir)
			if sideReached(c, target, dir) {
				crossStep = s
				roleStep = 0
				continue
			}
			// Not yet at sync s; compute distance from the previous sync barrier.
			prev := sideBaseline(n, dir)
			if s > 1 {
				prev = sideTargetAt(n, unit.mul(s-1), dir)
			}
			if c >= prev {
				roleStep = c - prev
			} else {
				roleStep = prev - c
			}
			break
		}
		states[role] = RoleStepState{SyncWindowIndex: crossStep, RoleStep: roleStep, Replicas: c}
	}
	return states
}

func sideTargetAt(n int, p frac, dir sideDirection) int {
	if dir == sideUp {
		return newSyncTarget(n, p)
	}
	return oldSyncTarget(n, p)
}

func sideReached(cur, target int, dir sideDirection) bool {
	if dir == sideUp {
		return cur >= target
	}
	return cur <= target
}

func sideBaseline(n int, dir sideDirection) int {
	if dir == sideUp {
		return 0
	}
	return n
}

// sidesProgress bundles each side's current sync state (per-role) and its
// minimalUnit, so downstream helpers don't have to thread three values each.
type sidesProgress struct {
	new, old         map[string]RoleStepState
	newUnit, oldUnit frac
}

// syncTargets identifies the NEXT sync barrier each side wants to reach, plus
// the corresponding fractional progress percentage used to compute per-role
// replica targets.
type syncTargets struct {
	nextNewSync    int  // index of the new-side barrier we're advancing toward
	nextOldSync    int  // index of the old-side barrier we're advancing toward
	minNewCross    int  // floor across all roles' new-side progress
	minOldCross    int  // floor across all roles' old-side progress
	newSyncPercent frac // = newUnit * nextNewSync
	oldSyncPercent frac // = oldUnit * nextOldSync
}

// roleDeltas is the per-role decision: how many new replicas to add and how
// many old replicas to drain on this step.
type roleDeltas struct {
	addNew, drainOld int
}

// derivePerSideProgress computes the current sync position for every role on
// both sides (NEW scaling up, OLD draining down), each at its own minimalUnit.
func derivePerSideProgress(roleNames []string, initialOld, currentOld, currentNew, targetNew map[string]int) sidesProgress {
	newUnit := revisionMinimalUnit(targetNew)
	oldUnit := revisionMinimalUnit(initialOld)
	return sidesProgress{
		new:     deriveSideProgress(roleNames, targetNew, currentNew, newUnit, sideUp),
		old:     deriveSideProgress(roleNames, initialOld, currentOld, oldUnit, sideDown),
		newUnit: newUnit,
		oldUnit: oldUnit,
	}
}

// pickNextSyncTargets finds each side's lowest-progressed role (the floor) and
// returns the next sync barrier above it. Within a side, only roles AT this
// floor are allowed to advance — coordinates role ratios within a revision.
func pickNextSyncTargets(roleNames []string, p sidesProgress) syncTargets {
	nNewSync := numSyncPoints(p.newUnit)
	nOldSync := numSyncPoints(p.oldUnit)
	minNewCross := nNewSync
	minOldCross := nOldSync
	for _, role := range roleNames {
		if p.new[role].SyncWindowIndex < minNewCross {
			minNewCross = p.new[role].SyncWindowIndex
		}
		if p.old[role].SyncWindowIndex < minOldCross {
			minOldCross = p.old[role].SyncWindowIndex
		}
	}
	nextNew := min(minNewCross+1, nNewSync)
	nextOld := min(minOldCross+1, nOldSync)
	return syncTargets{
		nextNewSync:    nextNew,
		nextOldSync:    nextOld,
		minNewCross:    minNewCross,
		minOldCross:    minOldCross,
		newSyncPercent: p.newUnit.mul(nextNew),
		oldSyncPercent: p.oldUnit.mul(nextOld),
	}
}

// computeRoleDeltas decides how many replicas to add (new) and drain (old) for
// one role this step. A role parked at a higher sync barrier on either side
// doesn't advance on that side. The capacity envelope (surge ceiling /
// unavailable floor) caps both deltas; a +1 fallback breaks ties when the
// budget is zero but progress is still possible.
func computeRoleDeltas(role string, initial, target, curNew, curOld int, sync syncTargets, p sidesProgress, cfg RollingUpdateConfig) roleDeltas {
	newTarget := newSyncTarget(target, sync.newSyncPercent)
	oldTarget := oldSyncTarget(initial, sync.oldSyncPercent)

	newParked := p.new[role].SyncWindowIndex > sync.minNewCross
	oldParked := p.old[role].SyncWindowIndex > sync.minOldCross

	ceiling := max(initial, target) + cfg.MaxSurge
	floor := max(0, min(initial, target)-cfg.MaxUnavailable)
	if cfg.MaxSurge == 0 && cfg.MaxUnavailable == 0 {
		// Legacy default: allow a +1 ceiling so rollouts can progress at all.
		ceiling = max(initial, target) + 1
	}

	currentTotal := curNew + curOld
	addBudget := max(0, ceiling-currentTotal)
	drainBudget := max(0, currentTotal-floor)

	addNew := 0
	if !newParked {
		addNew = max(0, min(newTarget-curNew, addBudget))
	}
	drainOld := 0
	if !oldParked {
		drainOld = max(0, min(curOld-oldTarget, drainBudget))
	}

	// Guarantee at least one operation when an unparked side has work but its
	// budget is zero at this instant (other side will free room next reconcile).
	if addNew == 0 && drainOld == 0 {
		if !newParked && curNew < newTarget && currentTotal+1 <= ceiling {
			addNew = 1
		} else if !oldParked && curOld > oldTarget && currentTotal-1 >= floor {
			drainOld = 1
		}
	}

	return roleDeltas{addNew: addNew, drainOld: drainOld}
}

// applyRoleDeltas builds the per-role pieces of the UpdateStep output:
// new spec, old spec, and updated sync positions on both sides.
func applyRoleDeltas(role string, initial, target, curNew, curOld int, d roleDeltas, sync syncTargets, p sidesProgress) (newSide, oldSide RoleStepState) {
	newTarget := newSyncTarget(target, sync.newSyncPercent)
	oldTarget := oldSyncTarget(initial, sync.oldSyncPercent)
	nextNew := curNew + d.addNew
	nextOld := curOld - d.drainOld

	newSide = p.new[role]
	if !(p.new[role].SyncWindowIndex > sync.minNewCross) { // not parked
		newSide.RoleStep++
		if nextNew >= newTarget {
			newSide.SyncWindowIndex = sync.nextNewSync
			newSide.RoleStep = 0
		}
	}
	newSide.Replicas = nextNew

	oldSide = p.old[role]
	if !(p.old[role].SyncWindowIndex > sync.minOldCross) { // not parked
		oldSide.RoleStep++
		if nextOld <= oldTarget {
			oldSide.SyncWindowIndex = sync.nextOldSync
			oldSide.RoleStep = 0
		}
	}
	oldSide.Replicas = nextOld
	return
}

// ComputeNextStep computes the next scaling step for a rolling update.
//
// Two-minU model: old and new revisions progress at their own native rhythms
// (one minimalUnit each). Within a single revision, all roles coordinate at
// that revision's sync points. Across revisions, no forced lockstep — the
// maxSurge ceiling and maxUnavailable floor are the only cross-side coupling.
//
// Algorithm in four phases:
//  1. derivePerSideProgress — where is each role on each side's sync sequence?
//  2. pickNextSyncTargets   — which barriers do we want to reach this step?
//  3. computeRoleDeltas     — per role, how many replicas to add / drain?
//  4. applyRoleDeltas       — assemble the UpdateStep with updated positions.
func ComputeNextStep(
	roleNames []string,
	initialOld, currentOld, currentNew, targetNew map[string]int,
	config map[string]RollingUpdateConfig,
) *UpdateStep {
	if isComplete(roleNames, currentOld, currentNew, targetNew) {
		return nil
	}

	progress := derivePerSideProgress(roleNames, initialOld, currentOld, currentNew, targetNew)
	sync := pickNextSyncTargets(roleNames, progress)

	pastStep := make(map[string]RoleStepState, len(roleNames))
	newStep := make(map[string]RoleStepState, len(roleNames))
	for _, role := range roleNames {
		initial := initialOld[role]
		target := targetNew[role]
		curNew := currentNew[role]
		curOld := currentOld[role]
		cfg := config[role]

		deltas := computeRoleDeltas(role, initial, target, curNew, curOld, sync, progress, cfg)
		newSide, oldSide := applyRoleDeltas(role, initial, target, curNew, curOld, deltas, sync, progress)
		newStep[role] = newSide
		pastStep[role] = oldSide
	}

	// Check if any role actually changed.
	changed := false
	for _, role := range roleNames {
		if newStep[role].Replicas != currentNew[role] || pastStep[role].Replicas != currentOld[role] {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}

	return &UpdateStep{Past: pastStep, New: newStep}
}

func isComplete(roleNames []string, currentOld, currentNew, targetNew map[string]int) bool {
	for _, role := range roleNames {
		if currentOld[role] != 0 || currentNew[role] < targetNew[role] {
			return false
		}
	}
	return true
}

// ComputeAllSteps simulates a full rollout by repeatedly calling ComputeNextStep.
// Used in tests to validate the complete rollout sequence.
func ComputeAllSteps(
	roleNames []string,
	initialOld, target map[string]int,
	config map[string]RollingUpdateConfig,
) []UpdateStep {
	currentOld := make(map[string]int, len(roleNames))
	currentNew := make(map[string]int, len(roleNames))
	for _, role := range roleNames {
		currentOld[role] = initialOld[role]
		currentNew[role] = 0
	}

	maxReplicas := 0
	for _, role := range roleNames {
		maxReplicas = max(maxReplicas, initialOld[role], target[role])
	}
	maxSteps := maxReplicas*4 + 10

	initialStep := UpdateStep{
		Past: make(map[string]RoleStepState, len(roleNames)),
		New:  make(map[string]RoleStepState, len(roleNames)),
	}
	for _, role := range roleNames {
		initialStep.Past[role] = RoleStepState{Replicas: initialOld[role]}
		initialStep.New[role] = RoleStepState{Replicas: 0}
	}
	steps := []UpdateStep{initialStep}

	for range maxSteps {
		nextStep := ComputeNextStep(roleNames, initialOld, currentOld, currentNew, target, config)
		if nextStep == nil {
			break
		}
		steps = append(steps, *nextStep)
		for _, role := range roleNames {
			currentOld[role] = nextStep.Past[role].Replicas
			currentNew[role] = nextStep.New[role].Replicas
		}
	}

	return steps
}
