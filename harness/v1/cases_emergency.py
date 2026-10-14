"""Bounded emergency recovery using real Pods and real scheduler rejection.

The interrupted history is seeded, not claimed to arise from a timing race:
standalone LWSs first create seven genuinely Ready groups, then are attached to
one paused DisaggregatedSet. Only ownership/revision metadata is seeded; Pod
status, native scale acknowledgements, and Unschedulable timestamps are never
forged. This isolates the fallback without a fragile multi-rollout timing race.
"""

import copy
import json
import subprocess
import time
from contextlib import contextmanager

from lab import (DS_RESOURCE, PREFIX, REVISION_LABEL, ROLE_LABEL, SET_LABEL,
                 SLOT, ready, role_spec)


def _count(lab, state, version, role, field="spec"):
    return lab.counts(state, version).get(role, {}).get(field, 0)


def _old_a_survives(lab, state):
    return all(_count(lab, state, "A", role, field) == count
               for role, count in (("prefill", 1), ("decode", 3))
               for field in ("spec", "ready"))


def _event(state, reason):
    return any(event.get("reason") == reason for event in state["events"])


def _capacity_rejection(lab, state):
    return any(condition.get("type") == "PodScheduled"
               and condition.get("status") == "False"
               and condition.get("reason") == "Unschedulable"
               and ("Insufficient " + SLOT) in condition.get("message", "")
               for pod in lab.pods(state, "C", "prefill")
               for condition in pod.get("status", {}).get("conditions", []))


@contextmanager
def _paused_for_adoption(lab):
    """Exclude only this namespace from the three hooks needed while paused."""
    manager = lab.k("-n", "lws-system", "get", "deployment", "lws-controller-manager",
                    "-o", "json", json_output=True)
    replicas = manager["spec"].get("replicas", 1)
    restores = []
    selected = {"vdisaggregatedset.kb.io", "vleaderworkerset.kb.io", "mleaderworkerset.kb.io"}
    try:
        for resource in ("mutatingwebhookconfigurations", "validatingwebhookconfigurations"):
            configurations = lab.k("get", resource, "-o", "json", json_output=True)
            for configuration in configurations["items"]:
                patch, undo = [], []
                for index, webhook in enumerate(configuration["webhooks"]):
                    if (webhook["name"] not in selected
                            or webhook.get("clientConfig", {}).get("service", {}).get("namespace") != "lws-system"):
                        continue
                    path = f"/webhooks/{index}/namespaceSelector"
                    selector = copy.deepcopy(webhook.get("namespaceSelector", {}))
                    selector.setdefault("matchExpressions", []).append({
                        "key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": [lab.namespace]})
                    patch.append({"op": "add", "path": path, "value": selector})
                    if "namespaceSelector" in webhook:
                        undo.append({"op": "add", "path": path, "value": webhook["namespaceSelector"]})
                    else:
                        undo.append({"op": "remove", "path": path})
                if patch:
                    name = configuration["metadata"]["name"]
                    # Remember the restoration before changing anything, so
                    # failures and Ctrl-C still attempt exact restoration.
                    restores.append((resource, name, undo))
                    lab.k("patch", resource, name, "--type=json", "-p", json.dumps(patch))
        lab.assert_(sum(len(patch) for _, _, patch in restores) == 3,
                    "only the three required DS/LWS admission hooks were scoped to the demo namespace")
        (lab.directory / "emergency-webhook-restoration.json").write_text(json.dumps(restores, indent=2))
        lab.k("-n", "lws-system", "scale", "deployment/lws-controller-manager", "--replicas=0")
        deadline = time.monotonic() + 90
        while lab.k("-n", "lws-system", "get", "pods", "-l", "control-plane=controller-manager",
                    "-o", "json", json_output=True)["items"]:
            if time.monotonic() >= deadline:
                raise TimeoutError("local controller did not stop before history adoption")
            time.sleep(1)
        yield
    finally:
        errors = []
        for resource, name, patch in reversed(restores):
            try:
                lab.k("patch", resource, name, "--type=json", "-p", json.dumps(patch))
            except Exception as error:
                errors.append(str(error))
        try:
            lab.k("-n", "lws-system", "scale", "deployment/lws-controller-manager", f"--replicas={replicas}")
            lab.k("-n", "lws-system", "rollout", "status", "deployment/lws-controller-manager", "--timeout=120s")
        except Exception as error:
            errors.append(str(error))
        if errors:
            raise RuntimeError("Could not fully restore emergency setup: " + "; ".join(errors))


def _seed_history(lab):
    """A=1P/3D, B=1P/1D, C=0P/1D, each with intended baseline 1P/4D."""
    lab.set_slots(7)
    seeds = []
    for version, counts in (("A", (1, 3)), ("B", (1, 1)), ("C", (0, 1))):
        for role, count, initial in zip(("prefill", "decode"), counts, (1, 4)):
            spec = role_spec(role, count, lab.identity, version, surge=1, unavailable=0, auto_ready=False)["spec"]
            # Existing Pods need role/version labels for the dashboard. There
            # is deliberately no DS-name label on the LWS itself until adoption.
            spec["leaderWorkerTemplate"]["workerTemplate"]["metadata"]["labels"].update({
                ROLE_LABEL: role, SET_LABEL: lab.ds_name, PREFIX + "slice": "0"})
            name = f"emergency-{version.lower()}-{role}"
            lab.apply({"apiVersion": "leaderworkerset.x-k8s.io/v1", "kind": "LeaderWorkerSet",
                       "metadata": {"name": name, "labels": {ROLE_LABEL: role},
                                    "annotations": {PREFIX + "initial-replicas": str(initial)}},
                       "spec": spec})
            seeds.append((name, version, role))

    def seeded(state):
        return (len(lab.pods(state)) == 7 and all(ready(p) for p in lab.pods(state))
                and _old_a_survives(lab, state)
                and all(_count(lab, state, "B", role, "ready") == 1 for role in ("prefill", "decode"))
                and _count(lab, state, "C", "decode", "ready") == 1)

    lab.wait("seven real Ready groups occupy seven scheduler slots", seeded,
             timeout=240, tick=lambda state: lab.release(state=state))
    seeded_state = lab.snapshot()
    (lab.directory / "emergency-seeded.json").write_text(json.dumps(seeded_state, indent=2))
    lab.checkpoint("Attach this interrupted history to the controller",
                   "Pause only the local manager, create the parent, attach six LWS objects, restore admission and resume.")
    with _paused_for_adoption(lab):
        roles = [role_spec(role, count, lab.identity, "C", surge=1, unavailable=0, auto_ready=False)
                 for role, count in (("prefill", 1), ("decode", 4))]
        lab.apply({"apiVersion": "disaggregatedset.x-k8s.io/v1", "kind": "DisaggregatedSet",
                   "metadata": {"name": lab.ds_name, "annotations": {PREFIX + "revision-hash-version": "v2"}},
                   "spec": {"roles": roles, "scalingPolicy": {"scaleDownDuringRollout": lab.policy}}})
        ds = lab.k("-n", lab.namespace, "get", DS_RESOURCE, lab.ds_name, "-o", "json", json_output=True)
        # Use the public revision implementation via our read-only observer.
        # The persisted CRD defaults, not a handwritten hash, are authoritative.
        observed = subprocess.run([lab.args.observer, "--kubeconfig", lab.args.kubeconfig,
                                   "--context", lab.args.context, "--namespace", lab.namespace,
                                   "--name", lab.ds_name], text=True, capture_output=True, timeout=60, check=True)
        target = json.loads(observed.stdout)["targetRevision"]
        owner = {"apiVersion": ds["apiVersion"], "kind": ds["kind"], "name": lab.ds_name,
                 "uid": ds["metadata"]["uid"], "controller": True}
        for name, version, role in seeds:
            labels = {SET_LABEL: lab.ds_name, ROLE_LABEL: role, PREFIX + "slice": "0",
                      REVISION_LABEL: target if version == "C" else "seed-" + version.lower()}
            lab.k("-n", lab.namespace, "patch", "lws", name, "--type=merge", "-p",
                  json.dumps({"metadata": {"labels": labels, "ownerReferences": [owner]}}))
    lab.current_version = "C"
    lab.start_observer()


def emergency(lab):
    lab.note("Emergency recovery: desired 1P/4D, maxSurge=1 / maxUnavailable=0 per role.")
    lab.note("Seven equal slots. Seeded A=1P/3D + B=1P/1D + C=0P/1D uses all seven.")
    lab.note("Normal Ready floors: P=1 / D=4. Emergency D floor: 3; positive P floor stays 1.")
    _seed_history(lab)
    state = lab.wait("one bootstrap Prefill is genuinely Unschedulable: no eighth slot",
                     lambda s: _event(s, "BootstrapSurge") and _capacity_rejection(lab, s)
                     and _old_a_survives(lab, s)
                     and all(_count(lab, s, "B", role) == 1 for role in ("prefill", "decode")),
                     timeout=90)
    original_a = {p["metadata"]["uid"] for p in lab.pods(state, "A")}
    (lab.directory / "emergency-unschedulable.json").write_text(json.dumps(state, indent=2))
    lab.note("The real scheduler clock runs for at least 60s; this timeout is not forged or shortened.")
    lab.note("No further event needs injecting: automatic bounded fallback may retire B, keeping A complete.")

    def recovered(s):
        return (_event(s, "AvailabilityFallback") and _old_a_survives(lab, s)
                and all(_count(lab, s, "B", role) == 0 for role in ("prefill", "decode"))
                and _count(lab, s, "C", "prefill", "scheduled") == 1
                and _count(lab, s, "C", "prefill", "ready") == 0)

    state = lab.wait("fallback retires B and schedules C Prefill; A keeps 1P/3D Ready", recovered, timeout=150)
    (lab.directory / "emergency-fallback.json").write_text(json.dumps(state, indent=2))

    def bounded(s):
        return (_old_a_survives(lab, s)
                and original_a == {p["metadata"]["uid"] for p in lab.pods(s, "A")}
                and _count(lab, s, "C", "prefill") == 1
                and _count(lab, s, "C", "prefill", "ready") == 0)

    lab.hold("loading Prefill neither stacks bootstrap replicas nor cascades into more old drains",
             15, bounded)
    lab.checkpoint("The replacement has a slot; let it become Ready",
                   "Open the new C readiness probes. Ordinary rollout can now finish.")
    lab.converge("C", {"prefill": 1, "decode": 4})
    final = lab.snapshot()
    lab.assert_(_event(final, "BootstrapSurge") and _event(final, "AvailabilityFallback"),
                "both emergency mechanisms were observed as real controller events")
