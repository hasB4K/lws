"""Small live scenarios for changing demand, recovery, and virtual roles.

These are real API/controller/kubelet scenarios. Readiness is controlled through
files in the application containers; Pod status is never manufactured.
"""

from collections import Counter, defaultdict

from lab import PREFIX, REVISION_LABEL, SUBROLE_LABEL, VERSION_LABEL, WORKER_INDEX, version_of


def _count(lab, state, version, role):
    return lab.counts(state, version).get(role, {"spec": 0, "ready": 0})


def _total_ready(lab, state, role):
    return sum(_count(lab, state, version, role)["ready"] for version in ("A", "B", "C"))


def _revisions(lab, state, version):
    return {pod["metadata"]["labels"][REVISION_LABEL]
            for pod in lab.pods(state, version=version)}


def _live(pods):
    return [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]


def _uids(pods):
    return {pod["metadata"]["uid"] for pod in _live(pods)}


def _ready(pod):
    return any(condition["type"] == "Ready" and condition["status"] == "True"
               for condition in pod.get("status", {}).get("conditions", []))


def _mixed(lab):
    """Reach unequal Ready progress without races against a startup timer."""
    lab.create(4, 4, surge=1, unavailable=0)
    lab.checkpoint("Start B; its Pods schedule, but their application readiness stays closed.",
                   "Change both role templates from A to B.")
    lab.update(version="B", ready=False)
    lab.wait("B has a scheduled loading group in both roles", lambda state: all(
        _count(lab, state, "B", role).get("scheduled", 0) >= 1
        for role in ("prefill", "decode")))
    lab.checkpoint("Release one B group in each role.", "touch /tmp/ready in those groups")
    for role in ("prefill", "decode"):
        lab.release(version="B", role=role, count=1)
    lab.wait("The next Prefill group is running", lambda state:
             _count(lab, state, "B", "prefill").get("scheduled", 0) >= 2)
    lab.checkpoint("Release a second Prefill, while Decode stays one group behind.",
                   "Open one additional Prefill readiness file.")
    lab.release(version="B", role="prefill", count=2)
    return lab.wait("B is Ready at 2 Prefill / 1 Decode", lambda state:
                    _count(lab, state, "B", "prefill")["ready"] == 2 and
                    _count(lab, state, "B", "decode")["ready"] == 1 and
                    _count(lab, state, "B", "prefill").get("scheduled", 0) >= 3 and
                    _count(lab, state, "B", "decode").get("scheduled", 0) >= 2)


def scale_up(lab):
    before = _mixed(lab)
    old = lab.counts(before, "A")
    target = lab.counts(before, "B")
    started = _uids(lab.pods(before, version="B"))
    revision = _revisions(lab, before, "B")
    lab.checkpoint("Raise desired 4 → 6 per role. Missing capacity is not Ready capacity.",
                   "Patch only each role's spec.replicas to 6.")
    lab.update(counts={"prefill": 6, "decode": 6})
    lab.checkpoint("Let the currently loading groups finish, but hold the next batch.",
                   "Release B up to three Ready Prefill and two Ready Decode groups.")
    lab.release(version="B", role="prefill", count=3)
    lab.release(version="B", role="decode", count=2)

    def protected(state):
        lab.assert_(started <= _uids(lab.pods(state, version="B")),
                    "Scale-up replaced a scheduled target group instead of keeping its startup work")
        for role in ("prefill", "decode"):
            old_spec = _count(lab, state, "A", role)["spec"]
            new_spec = _count(lab, state, "B", role)["spec"]
            lab.assert_(old_spec <= old[role]["spec"],
                        "A demand increase must not recreate old replicas")
            lab.assert_(new_spec >= target[role]["spec"],
                        "Scale-up cancelled already scheduled target replicas")
            old[role]["spec"], target[role]["spec"] = old_spec, new_spec
            lab.assert_(_total_ready(lab, state, role) >= 4,
                        "The controller destroyed already-Ready capacity during scale-up")
        return True

    def growing(state):
        protected(state)
        return all(_total_ready(lab, state, role) >= 5
                   for role in ("prefill", "decode"))

    lab.wait("Preserve five Ready groups while building toward the new floor of six", growing)
    lab.hold("Slow startup does not permit another old drain", 8, lambda state:
             protected(state) and all(_total_ready(lab, state, role) >= 5
                                     for role in ("prefill", "decode")))
    lab.assert_(_revisions(lab, lab.snapshot(), "B") == revision,
                "A replica-only edit created another B revision")
    lab.checkpoint("Release all remaining B groups and finish at exactly 6 + 6.",
                   "Open readiness for current and subsequently created B groups.")
    lab.converge("B", {"prefill": 6, "decode": 6})


def scale_down(lab):
    before = _mixed(lab)
    old, target = lab.counts(before, "A"), lab.counts(before, "B")
    needed_decode = _uids(lab.pods(before, version="B", role="decode"))
    revision = _revisions(lab, before, "B")
    lab.checkpoint("Lower desired 4 → 2 per role. Prefer safe old drains; keep needed loading groups.",
                   "Patch only each role's spec.replicas to 2.")
    lab.update(counts={"prefill": 2, "decode": 2})

    def safe(state):
        lab.assert_(needed_decode == _uids(lab.pods(state, version="B", role="decode")),
                    "Scale-down replaced a needed scheduled Decode group")
        for role in ("prefill", "decode"):
            current = _count(lab, state, "B", role)
            old_spec = _count(lab, state, "A", role)["spec"]
            lab.assert_(old_spec <= old[role]["spec"],
                        "Scale-down recreated old replicas")
            old[role]["spec"] = old_spec
            lab.assert_(current["spec"] <= target[role]["spec"],
                        "Scale-down grew already sufficient target replicas")
            lab.assert_(current["spec"] >= min(target[role]["spec"], 2),
                        "Scale-down cancelled scheduled target work below final desired")
            lab.assert_(_total_ready(lab, state, role) >= 2,
                        "Scale-down crossed the new availability floor")
        return True

    expected_prefill = 2 if lab.policy == "Immediate" else target["prefill"]["spec"]

    def settled(state):
        safe(state)
        return all(_count(lab, state, "A", role)["spec"] <= 1
                   for role in ("prefill", "decode")) and \
            _count(lab, state, "B", "prefill")["spec"] == expected_prefill

    state = lab.wait("Old replicas shrink while B's second Decode is still loading", settled)
    lab.hold("Needed scheduled groups stay; real excess may go", 8, safe)
    state = lab.snapshot()
    lab.assert_(_count(lab, state, "B", "prefill")["spec"] == expected_prefill,
                "Target scale-down did not follow the selected policy")
    lab.assert_(_revisions(lab, state, "B") == revision,
                "A replica-only edit created another B revision")
    lab.checkpoint("Let the remaining Decode become Ready, then finish at exactly 2 + 2.",
                   "Open all B readiness files.")
    lab.converge("B", {"prefill": 2, "decode": 2})


def _handoff_preserved(lab, state, before):
    """UIDs also catch a drain/recreation that completed between samples."""
    lab.assert_(_uids(state["pods"]) == _uids(before["pods"]),
                "Static→External deleted or replaced an existing Pod")
    lab.assert_(_uids(state["lws"]) == _uids(before["lws"]),
                "Changing the replica source created or deleted a workload revision")
    for role, original in lab.counts(before, "A").items():
        current = _count(lab, state, "A", role)
        lab.assert_(current["spec"] == original["spec"] and current["ready"] == original["ready"],
                    f"Static→External changed {role}'s requested or Ready group count")
    return True


def autoscaler_handoff(lab):
    # Two-Pod groups distinguish replica-group counts from individual Pods.
    lab.create(3, 2, size=2, surge=1, unavailable=0)
    before = lab.snapshot()
    lab.assert_(not before["scalers"], "A Static role unexpectedly has a scaler")
    lab.checkpoint("Hand Prefill's three Ready groups to an external scaler. Keep every Pod.",
                   "Set Prefill scaling.mode=External and remove its inline replicas in one patch; keep templates unchanged.")
    lab.update(external_roles=("prefill",))
    scaler_name = lab.ds_name + "-prefill"

    def seeded(state):
        _handoff_preserved(lab, state, before)
        scalers = state["scalers"]
        if not scalers:
            return False
        lab.assert_(len(scalers) == 1 and scalers[0]["metadata"]["name"] == scaler_name,
                    "Only the External Prefill role should get a scaler")
        scaler = scalers[0]
        lab.assert_(scaler["spec"].get("replicas") == 3,
                    "The new scaler must start at three existing groups, not zero, one, or six Pods")
        lab.assert_(any(owner.get("controller") and owner["uid"] == before["ds"]["metadata"]["uid"]
                        for owner in scaler["metadata"].get("ownerReferences", [])),
                    "The scaler is not owned by this DisaggregatedSet")
        return scaler.get("status", {}).get("replicas") == 3 and \
            state["ds"].get("status", {}).get("observedGeneration") == state["ds"]["metadata"]["generation"]

    lab.wait("The generated scaler inherits three replicas, without replacing any Pod", seeded)
    scale_url = f"/apis/disaggregatedset.x-k8s.io/v1/namespaces/{lab.namespace}/disaggregatedsetrolescalers/{scaler_name}/scale"
    scale = lab.k("get", "--raw", scale_url, json_output=True)
    lab.assert_(scale["spec"]["replicas"] == 3 and scale["status"]["replicas"] == 3,
                "The autoscaler's first /scale read did not see the existing three groups")
    lab.hold("No autoscaler write for 15 seconds: all ten original Pods stay Ready", 15, seeded)

    for previous, desired in ((3, 4), (4, 2)):
        prior = lab.snapshot()
        lab.checkpoint(f"The external scaler now requests Prefill {previous} → {desired}; Decode remains at two.",
                       f"kubectl scale dsrs/{scaler_name} --replicas={desired} (writes /scale)")
        lab.k("-n", lab.namespace, "scale", "dsrs/" + scaler_name, f"--replicas={desired}")

        def scaled(state):
            lab.assert_(_uids(state["lws"]) == _uids(before["lws"]), "/scale created another revision")
            lab.assert_(_uids(lab.pods(state, role="decode")) == _uids(lab.pods(before, role="decode")),
                        "Scaling Prefill replaced a Decode Pod")
            decode, prefill = (_count(lab, state, "A", role) for role in ("decode", "prefill"))
            lab.assert_(decode["spec"] == decode["ready"] == 2, "Scaling Prefill disturbed Decode capacity")
            lab.assert_(min(previous, desired) <= prefill["spec"] <= max(previous, desired) and
                        prefill["ready"] >= min(previous, desired), "External scaling drained needed Prefill groups")
            if desired > previous:
                lab.assert_(_uids(prior["pods"]) <= _uids(state["pods"]), "Scale-up replaced existing Pods")
            return prefill["spec"] == prefill["ready"] == desired and \
                len(state["pods"]) == (desired + 2) * 2 and all(_ready(p) for p in _live(state["pods"])) and \
                not any(p["metadata"].get("deletionTimestamp") for p in state["pods"])

        lab.wait(f"/scale reaches {desired}P/2D without changing templates", scaled, stable=3)
    lab.note("This tests the autoscaler handoff and /scale API; it does not deploy a metrics-driven HPA.")


def restart_rollback(lab):
    lab.create(4, 4, surge=1, unavailable=1)
    lab.checkpoint("Start B with readiness held.", "Change A templates to B.")
    lab.update(version="B", ready=False)
    lab.wait("B has scheduled groups", lambda state: all(
        _count(lab, state, "B", role).get("scheduled", 0) >= 1
        for role in ("prefill", "decode")))
    lab.checkpoint("Make one B group Ready per role, leaving the rollout unfinished.",
                   "Open one group readiness file in Prefill and Decode.")
    for role in ("prefill", "decode"):
        lab.release(version="B", role=role, count=1)
    state = lab.wait("B has one Ready group per role", lambda state: all(
        _count(lab, state, "B", role)["ready"] == 1
        for role in ("prefill", "decode")))
    original_b = _revisions(lab, state, "B")
    lab.checkpoint("Interrupt B with C.", "Patch templates to C; keep readiness held.")
    lab.update(version="C", ready=False)
    lab.wait("The interrupted C revision exists", lambda state:
             bool(_revisions(lab, state, "C")))
    lab.checkpoint("Restart the real controller during this interrupted rollout.",
                   "Delete the owned manager Pod; wait for its replacement to start.")
    lab.restart_controller()
    lab.checkpoint("Roll back to B while the new observer is active.",
                   "Restore B's exact template, including its readiness gate.")
    lab.update(version="B", ready=False)
    lab.hold("The restarted controller must not spend the same allowance twice", 8,
             lambda state: all(_total_ready(lab, state, role) >= 3
                               for role in ("prefill", "decode")))
    lab.assert_(_revisions(lab, lab.snapshot(), "B") == original_b,
                "Rollback did not reuse B's revision")
    lab.checkpoint("Release B and complete the rollback.", "Open B readiness files.")
    lab.converge("B", {"prefill": 4, "decode": 4})
    lab.note("This exercises real process/cache recovery, not an artificially delayed API request.")


def _groups(pods):
    groups = defaultdict(list)
    for pod in _live(pods):
        labels = pod["metadata"].get("labels", {})
        key = (labels.get("leaderworkerset.sigs.k8s.io/name"),
               labels.get("leaderworkerset.sigs.k8s.io/group-key"))
        groups[key].append(pod)
    return groups


def _assigned(lab, state, desired, version="A"):
    issued = lab.subrole_counts(state, version)
    if {child: value["spec"] for child, value in issued.items()} != desired:
        return False
    if any(lws["metadata"].get("annotations", {}).get(PREFIX + "subrole-plan")
           for lws in state["lws"] if version_of(lws) == version):
        return False
    if any(pod["metadata"].get("deletionTimestamp") and
           pod["metadata"].get("labels", {}).get(VERSION_LABEL) == version for pod in state["pods"]):
        return False
    observed = Counter()
    for group in _groups(lab.pods(state, version=version, role="decode")).values():
        children = {pod["metadata"].get("labels", {}).get(SUBROLE_LABEL) for pod in group}
        members = {pod["metadata"].get("labels", {}).get(WORKER_INDEX) for pod in group}
        if len(group) != 2 or members != {"0", "1"} or len(children) != 1 or None in children or not all(map(_ready, group)):
            return False
        observed[next(iter(children))] += 1
    return dict(observed) == desired and _count(lab, state, version, "decode")["spec"] == sum(desired.values())


def subroles(lab):
    lab.subroles_create({"a": 2, "b": 1}, size=2, surge=1, unavailable=0)
    initial = lab.wait("One shared LWS has a=2, b=1 whole Ready groups", lambda state:
                       _assigned(lab, state, {"a": 2, "b": 1}))
    original_uids = _uids(lab.pods(initial))
    revision = _revisions(lab, initial, "A")
    lab.checkpoint("Move one routing assignment: a=2/b=1 → a=1/b=2. No Pod restart.",
                   "Patch child replica targets; the physical total stays three.")
    lab.subroles_update(counts={"a": 1, "b": 2})
    state = lab.wait("Assignments changed without replacing Pods", lambda state:
                     _assigned(lab, state, {"a": 1, "b": 2}))
    lab.assert_(_uids(lab.pods(state)) == original_uids, "Relabelling replaced a Pod")
    lab.checkpoint("Shrink b=2 → 1, then restart the manager during assignment recovery.",
                   "Patch child b to one; restart the owned controller.")
    lab.subroles_update(counts={"a": 1, "b": 1})
    lab.restart_controller()
    state = lab.wait("Two physical groups remain with coherent child labels", lambda state:
                     _assigned(lab, state, {"a": 1, "b": 1}))
    survivors = _uids(lab.pods(state))
    lab.assert_(survivors <= original_uids, "Shrinking replaced an unrelated surviving group")
    lab.assert_(_revisions(lab, state, "A") == revision, "Child-count changes created a revision")
    lab.checkpoint("Grow b=1 → 2: a label change cannot substitute for physical capacity.",
                   "Patch child b to two; verify the parent grows from two to three.")
    lab.subroles_update(counts={"a": 1, "b": 2})
    state = lab.wait("Physical growth restores a=1, b=2", lambda state:
                     _assigned(lab, state, {"a": 1, "b": 2}))
    lab.assert_(survivors <= _uids(lab.pods(state)), "Growth replaced an existing group")
    lab.assert_(len([item for item in state["lws"] if not item["metadata"].get("deletionTimestamp")]) == 1,
                "Subroles created multiple physical LeaderWorkerSets")
    lab.checkpoint("Roll the shared template to B; each child independently has MS=1 / MU=0.",
                   "Change the parent template; hold B application readiness.")
    lab.subroles_update(version="B", ready=False)
    lab.wait("B's groups started without spending old child availability", lambda state:
             bool(lab.pods(state, version="B")))
    lab.hold("Unready B cannot authorize removal of A's needed child groups", 8, lambda state:
             _assigned(lab, state, {"a": 1, "b": 2}, version="A"))
    lab.checkpoint("Make B Ready and finish both child rollouts.", "Open all B readiness files.")
    lab.converge("B", {"decode/a": 1, "decode/b": 2})
    lab.wait("One target template with a=1/b=2 coherent Ready groups", lambda state:
             _assigned(lab, state, {"a": 1, "b": 2}, version="B"))
    if lab.identity == "Hash":
        lab.note("Hash deletion costs are preferences. This checks coherent eventual repair, not guaranteed child survivors.")


def native_restart(lab):
    lab.create(2, 1, size=2, surge=1, unavailable=0)
    for worker_index, member in (("1", "worker"), ("0", "leader")):
        state = lab.snapshot()
        prefill = lab.pods(state, version="A", role="prefill")
        victim = next(pod for pod in prefill
                      if pod["metadata"]["labels"].get(WORKER_INDEX) == worker_index)
        group = victim["metadata"]["labels"]["leaderworkerset.sigs.k8s.io/group-key"]
        affected = {pod["metadata"]["uid"] for pod in prefill
                    if pod["metadata"]["labels"]["leaderworkerset.sigs.k8s.io/group-key"] == group}
        unaffected = _uids(lab.pods(state)) - affected
        lab.checkpoint(f"Crash one {member} application. Native policy recreates only its replica group.",
                       f"kubectl exec {victim['metadata']['name']} -- touch /tmp/crash")
        lab.k("-n", lab.namespace, "exec", victim["metadata"]["name"], "--", "touch", "/tmp/crash")

        def recovered(state):
            pods = lab.pods(state)
            current = _uids(pods)
            lab.assert_(unaffected <= current, f"The {member} crash restarted an unrelated group")
            return (not affected & current and len(current) == len(state["pods"]) == 6
                    and all(map(_ready, _live(pods))))

        lab.wait(f"The {member}'s group was replaced; other group UIDs stayed identical", recovered)
        recovered_uids = _uids(lab.pods(lab.snapshot()))
        lab.hold(f"No replacement loop after {member} recovery", 12, lambda state:
                 _uids(lab.pods(state)) == recovered_uids and all(map(_ready, _live(lab.pods(state)))))
    lab.note("This is merged RecreateGroupOnPodRestart: Pod UIDs change in the failed group. The in-place prototype is not in this baseline.")
