"""Small, explicit Kubernetes operations for the v1 lab; never use ambient context."""

import copy
import json
import subprocess
import threading
import time
from pathlib import Path

PREFIX = "disaggregatedset.x-k8s.io/"
SET_LABEL, ROLE_LABEL, REVISION_LABEL = PREFIX + "name", PREFIX + "role", PREFIX + "revision"
SUBROLE_LABEL = PREFIX + "subrole"
VERSION_LABEL = "harness.lws.x-k8s.io/version"
GROUP_LABEL = "leaderworkerset.sigs.k8s.io/group-key"
WORKER_INDEX = "leaderworkerset.sigs.k8s.io/worker-index"
LWS_LABEL = "leaderworkerset.sigs.k8s.io/name"
SLOT = "harness.lws.x-k8s.io/slot"
DS_RESOURCE = "disaggregatedsets.disaggregatedset.x-k8s.io"
CHILD_COUNTS = PREFIX + "subrole-replicas"


def ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))


def version_of(lws):
    return lws["spec"]["leaderWorkerTemplate"]["workerTemplate"].get("metadata", {}).get("labels", {}).get(VERSION_LABEL, "?")


def role_spec(name, replicas, identity, version, surge=1, unavailable=0, size=1, auto_ready=True):
    template = {
        "metadata": {"labels": {VERSION_LABEL: version}},
        "spec": {"terminationGracePeriodSeconds": 1, "containers": [{
            "name": "demo", "image": "busybox:1.36", "imagePullPolicy": "IfNotPresent",
            "env": [{"name": "AUTO_READY", "value": "1" if auto_ready else "0"}],
            "command": ["sh", "-c", 'trap "exit 0" TERM INT; [ "$AUTO_READY" = 1 ] && touch /tmp/ready; while :; do if [ -f /tmp/crash ]; then echo HARNESS_CRASH; exit 42; fi; sleep 1; done'],
            "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]}, "periodSeconds": 1,
                               "timeoutSeconds": 2, "failureThreshold": 1},
            "resources": {"requests": {"cpu": "5m", "memory": "8Mi", SLOT: "1"}, "limits": {SLOT: "1"}},
        }]},
    }
    return {"name": name, "spec": {"replicas": replicas, "groupIdentity": identity,
        "rolloutStrategy": {"rollingUpdateConfiguration": {"maxSurge": surge, "maxUnavailable": unavailable}},
        "leaderWorkerTemplate": {"size": size, "restartPolicy": "RecreateGroupOnPodRestart", "workerTemplate": template}}}


class Lab:
    namespace, ds_name = "v1-harness", "demo"

    def __init__(self, args, identity, policy, case_name):
        self.args, self.identity, self.policy = args, identity, policy
        self.case_name, self.auto = case_name, args.auto
        self.directory = Path(args.run_dir) / (case_name + "-" + identity.lower() + "-" + policy.lower())
        self.directory.mkdir(parents=True, exist_ok=True)
        self.started, self.last_display = time.monotonic(), 0
        self.observer_process, self.latest_diagnostic = None, None
        self.kube = ["kubectl", "--kubeconfig", args.kubeconfig, "--context", args.context, "--request-timeout=15s"]
        self.current_version = "A"
        self.checks = []

    def k(self, *args, input=None, json_output=False):
        if input is not None and not isinstance(input, str):
            input = json.dumps(input)
        result = subprocess.run(self.kube + list(args), input=input, text=True, capture_output=True, timeout=45)
        if result.returncode:
            summary = ["<JSON>" if i and args[i - 1] == "-p" else value for i, value in enumerate(args)]
            raise RuntimeError(f"kubectl {' '.join(summary)}: {result.stderr.strip() or result.stdout.strip()}")
        return json.loads(result.stdout) if json_output else result.stdout

    def apply(self, value):
        self.k("-n", self.namespace, "apply", "-f", "-", input=value)

    def note(self, message):
        print(f"\033[36;1m{message}\033[0m", flush=True)

    def assert_(self, condition, message):
        if not condition:
            raise AssertionError(message)

    def checkpoint(self, title, command_description=""):
        self.note("\n━━ " + title)
        if command_description:
            print("   Event: " + command_description, flush=True)
        with (self.directory / "events.jsonl").open("a") as output:
            output.write(json.dumps({"time": time.time(), "title": title, "event": command_description}) + "\n")
        if not self.auto:
            answer = input("[Enter] trigger event · [a] auto for the rest of this run · [q] stop: ").strip().lower()
            if answer == "q":
                raise KeyboardInterrupt
            if answer == "a":
                self.auto = True

    def reset(self):
        self.k("-n", self.namespace, "delete", DS_RESOURCE, "--all", "--wait=true", "--timeout=90s")
        self.k("-n", self.namespace, "delete", "lws", "--all", "--wait=true", "--timeout=90s")
        self.wait("previous groups disappear", lambda s: not s["pods"] and not s["lws"], timeout=120)
        self.set_slots(64)

    def set_slots(self, count):
        path = "/status/capacity/" + SLOT.replace("/", "~1")
        self.k("patch", "node", self.args.node, "--subresource=status", "--type=json", "-p",
               json.dumps([{"op": "add", "path": path, "value": str(count)}]))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            node = self.k("get", "node", self.args.node, "-o", "json", json_output=True)
            if node["status"].get("allocatable", {}).get(SLOT) == str(count):
                self.note(f"Scheduler capacity: {count} equal synthetic slots (not GPUs); one slot per Pod")
                return
            time.sleep(1)
        raise TimeoutError("Node did not acknowledge the synthetic slot capacity")

    def start_observer(self):
        if self.observer_process:
            return
        command = [self.args.observer, "--kubeconfig", self.args.kubeconfig, "--context", self.args.context,
                   "--namespace", self.namespace, "--name", self.ds_name, "--watch", "2"]
        error_log = (self.directory / "observer-errors.log").open("w")
        self.observer_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error_log, text=True)

        def consume():
            with (self.directory / "observer.jsonl").open("w") as stream:
                for line in self.observer_process.stdout:
                    stream.write(line)
                    stream.flush()
                    try:
                        self.latest_diagnostic = json.loads(line)
                    except json.JSONDecodeError:
                        pass
        threading.Thread(target=consume, daemon=True).start()

    def close(self):
        if self.observer_process:
            self.observer_process.terminate()
            try:
                self.observer_process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.observer_process.kill()
                self.observer_process.wait()

    def create(self, p, d, version="A", surge=1, unavailable=0, size=1, ready=True,
               names=("prefill", "decode"), policy=None, slots=64):
        self.set_slots(slots)
        budget = lambda value, name: value.get(name, 0) if isinstance(value, dict) else value
        roles = [role_spec(name, count, self.identity, version, budget(surge, name), budget(unavailable, name), size, ready)
                 for name, count in zip(names, (p, d))]
        self.apply({"apiVersion": "disaggregatedset.x-k8s.io/v1", "kind": "DisaggregatedSet",
                    "metadata": {"name": self.ds_name},
                    "spec": {"roles": roles, "scalingPolicy": {"scaleDownDuringRollout": policy or self.policy}}})
        self.current_version = version
        self.start_observer()
        if ready:
            self.converge(version, dict(zip(names, (p, d))))

    def patch_set(self, patch):
        self.k("-n", self.namespace, "patch", DS_RESOURCE, self.ds_name, "--type=merge", "-p", json.dumps(patch))

    def update(self, version=None, counts=None, policy=None, ready=None, names=None, surge=None, unavailable=None):
        ds = self.k("-n", self.namespace, "get", DS_RESOURCE, self.ds_name, "-o", "json", json_output=True)
        roles = copy.deepcopy(ds["spec"]["roles"])
        for i, role in enumerate(roles):
            if names:
                role["name"] = names[i]
            if counts is not None and role["name"] in counts:
                role["spec"]["replicas"] = counts[role["name"]]
            for key, value in (("maxSurge", surge), ("maxUnavailable", unavailable)):
                if value is not None:
                    role["spec"]["rolloutStrategy"]["rollingUpdateConfiguration"][key] = value.get(role["name"], 0) if isinstance(value, dict) else value
            for key in ("workerTemplate", "leaderTemplate"):
                template = role["spec"]["leaderWorkerTemplate"].get(key)
                if not template:
                    continue
                if version is not None:
                    template.setdefault("metadata", {}).setdefault("labels", {})[VERSION_LABEL] = version
                if ready is not None:
                    for container in template["spec"]["containers"]:
                        for env in container.get("env", []):
                            if env["name"] == "AUTO_READY":
                                env["value"] = "1" if ready else "0"
        patch = {"spec": {"roles": roles}}
        if policy:
            patch["spec"]["scalingPolicy"] = {"scaleDownDuringRollout": policy}
        self.patch_set(patch)
        if version:
            self.current_version = version

    def subroles_create(self, counts, version="A", size=2, surge=1, unavailable=0):
        parent = role_spec("decode", 1, self.identity, version, surge, unavailable, size)
        parent["spec"].pop("replicas")
        parent["subRoles"] = [{"name": name, "replicas": n} for name, n in counts.items()]
        self.apply({"apiVersion": "disaggregatedset.x-k8s.io/v1", "kind": "DisaggregatedSet",
                    "metadata": {"name": self.ds_name}, "spec": {"roles": [parent],
                    "scalingPolicy": {"scaleDownDuringRollout": self.policy}}})
        self.current_version = version
        self.start_observer()
        self.converge(version, {"decode/" + name: n for name, n in counts.items()})

    def subroles_update(self, counts=None, version=None, ready=None):
        if counts is not None:
            ds = self.k("-n", self.namespace, "get", DS_RESOURCE, self.ds_name, "-o", "json", json_output=True)
            parent = ds["spec"]["roles"][0]
            parent["subRoles"] = [{"name": name, "replicas": n} for name, n in counts.items()]
            self.patch_set({"spec": {"roles": [parent]}})
        if version is not None or ready is not None:
            self.update(version=version, ready=ready)

    def snapshot(self):
        objects = self.k("-n", self.namespace, "get", DS_RESOURCE + ",lws,pods", "-o", "json", json_output=True)["items"]
        kinds = lambda name: [x for x in objects if x["kind"] == name]
        ds = next(iter(kinds("DisaggregatedSet")), None)
        # Scope controller events by UID: delayed events from a previous demo
        # must not look like emergency recovery in the next scenario.
        events = self.k("-n", self.namespace, "get", "events", "--field-selector",
                        "involvedObject.uid=" + ds["metadata"]["uid"], "-o", "json", json_output=True)["items"] if ds else []
        return {"ds": ds, "lws": kinds("LeaderWorkerSet"), "pods": kinds("Pod"), "events": events}

    def pods(self, state, version=None, role=None):
        return [p for p in state["pods"] if not p["metadata"].get("deletionTimestamp")
                and (version is None or p["metadata"].get("labels", {}).get(VERSION_LABEL) == version)
                and (role is None or p["metadata"].get("labels", {}).get(ROLE_LABEL) == role)]

    def groups(self, state, version=None, role=None):
        groups = {}
        for pod in self.pods(state, version, role):
            labels = pod["metadata"].get("labels", {})
            key = labels.get(GROUP_LABEL) or (labels.get(LWS_LABEL, "") + ":" + labels.get("leaderworkerset.sigs.k8s.io/group-index", ""))
            groups.setdefault(key, []).append(pod)
        return [members for _, members in sorted(groups.items())]

    def counts(self, state, version):
        result = {}
        for lws in state["lws"]:
            if version_of(lws) != version:
                continue
            role = lws["metadata"]["labels"][ROLE_LABEL]
            entry = result.setdefault(role, {"spec": 0, "ready": 0, "scheduled": 0, "initial": 0})
            entry["spec"] += lws["spec"].get("replicas", 1)
            entry["initial"] += int(lws["metadata"].get("annotations", {}).get(PREFIX + "initial-replicas", entry["spec"]))
            size = lws["spec"]["leaderWorkerTemplate"].get("size", 1)
            for members in self.groups(state, version, role):
                if members[0]["metadata"].get("labels", {}).get(LWS_LABEL) != lws["metadata"]["name"]:
                    continue
                complete = len(members) == size
                entry["ready"] += int(complete and all(ready(p) for p in members))
                entry["scheduled"] += int(complete and all(p["spec"].get("nodeName") for p in members))
        return result

    def subrole_counts(self, state, version):
        result = {}
        for lws in state["lws"]:
            if version_of(lws) != version:
                continue
            annotations = lws["metadata"].get("annotations", {})
            for name, count in json.loads(annotations.get(CHILD_COUNTS, "{}") or "{}").items():
                result.setdefault(name, {"spec": 0, "ready": 0, "groups": []})["spec"] += count
            size = lws["spec"]["leaderWorkerTemplate"].get("size", 1)
            for members in self.groups(state, version):
                if members[0]["metadata"].get("labels", {}).get(LWS_LABEL) != lws["metadata"]["name"]:
                    continue
                labels = {p["metadata"].get("labels", {}).get(SUBROLE_LABEL, "") for p in members}
                if len(labels) != 1 or not next(iter(labels)):
                    continue
                child = next(iter(labels))
                entry = result.setdefault(child, {"spec": 0, "ready": 0, "groups": []})
                entry["groups"].append(members)
                entry["ready"] += int(len(members) == size and all(ready(p) for p in members))
        return result

    def display(self, state, label):
        elapsed = int(time.monotonic() - self.started)
        print(f"\033[35;1m[{elapsed:3}s] {self.case_name} / {self.identity} / {self.policy} · {label}\033[0m", flush=True)
        targets = []
        for role in (state["ds"] or {}).get("spec", {}).get("roles", []):
            config = role["spec"].get("rolloutStrategy", {}).get("rollingUpdateConfiguration", {})
            counts = ",".join(f"{s['name']}={s.get('replicas', '?')}" for s in role.get("subRoles", [])) or str(role["spec"].get("replicas", "scaler"))
            targets.append(f"{role['name']}={counts} [MS={config.get('maxSurge', 'default')}, MU={config.get('maxUnavailable', 'default')}]")
        if targets:
            print("  Desired: " + " | ".join(targets), flush=True)
        for version in sorted({version_of(x) for x in state["lws"]}):
            cells = [f"{name} {c['spec']}/{c['ready']}/{c['scheduled']} (initial {c['initial']})"
                     for name, c in sorted(self.counts(state, version).items())]
            print(f"  {version:<5} Spec / whole-group Ready / scheduled: " + " | ".join(cells), flush=True)
        diagnostic = self.latest_diagnostic or {}
        if diagnostic.get("error"):
            print("\033[31m  Diagnostic observation: " + diagnostic["error"] + "\033[0m")
        for row in diagnostic.get("lws", []):
            if row.get("rawReady") != row.get("retainedReady"):
                print(f"  \033[33m{row['name']}: raw Ready {row.get('rawReady')} → retained Ready {row.get('retainedReady')} (acknowledgement/deletion safety)\033[0m")
        for slice_ in diagnostic.get("slices", []):
            rows = slice_.get("candidates", [])
            if not rows and slice_.get("steadyBounds"):
                rows = [{"revision": "steady", "bounds": slice_["steadyBounds"]}]
            for candidate in rows:
                print(f"  Slice {slice_['slice']} · candidate {candidate.get('revision', '?')[:10]} · permits outage={candidate.get('allowOutage', False)}")
                for b in candidate.get("bounds", []):
                    print(f"    {b['role']}: Ready floor {b.get('availabilityFloor')} | surge ceiling {b.get('surgeCeiling')}"
                          f" | pending budget {b.get('projectedPendingBudget')} | new limit {b.get('hardGrowthLimit')}"
                          f" | Ready/startup minima old/new {b.get('oldMinSpec')}/{b.get('targetMinSpec')}")
                    print(f"      serving/individual Ready {b.get('retainedServingReady')}/{b.get('retainedIndividualReady')}"
                          f" | old serving/individual checks {b.get('oldServingCheckEnabled')}/{b.get('oldIndividualCheckEnabled')}"
                          f" | Ready credit {b.get('readinessCredit')} | pending/disjoint growth {b.get('pendingGrowthLimit')}/{b.get('disjointGrowthLimit')}")
                    print(f"      ordinary proposed targets old/new {b.get('oldOrdinaryTarget')}/{b.get('newOrdinaryTarget')}")
                for key, title in (("new", "New built"), ("old", "Old retired"), ("replacement", "Replacement Ready")):
                    window = candidate.get("windows", {}).get(key, {})
                    numerator, denominator = window.get("numerator", 0), window.get("denominator", 0)
                    fraction = f"{numerator}/{denominator} = {100 * numerator / denominator:.0f}%" if denominator else "n/a"
                    width = window.get("widthDenominator", 0)
                    skew = f" · normal skew allowance {100 / width:.0f} percentage points" if width and key != "replacement" else ""
                    print(f"    {title}: {fraction}{skew}")
                if candidate.get("allowOutage"):
                    print("    \033[33mOld revision may pause; individual role budgets still apply.\033[0m")
                preview = candidate.get("preview")
                if preview:
                    print("    Read-only planner preview: " + json.dumps(preview, separators=(",", ":")))
                if candidate.get("schedulerRecoveryPreview"):
                    print("    Scheduler recovery preview: " + json.dumps(candidate["schedulerRecoveryPreview"], separators=(",", ":")))
        exceptions = {e.get("reason") for e in state["events"]} & {"BootstrapSurge", "AvailabilityFallback"}
        if exceptions:
            print("\033[33;1m  Observed emergency events: " + ", ".join(sorted(exceptions)) + "\033[0m")
        for warning in diagnostic.get("warnings", [])[1:]:
            print("\033[33m  " + warning + "\033[0m")
        print("\033[2m  Samples are not atomic/controller decisions; retained Ready and detailed bounds: observer.jsonl\033[0m", flush=True)

    def wait(self, label, predicate, timeout=180, stable=2, tick=None):
        deadline, hits = time.monotonic() + timeout, 0
        while time.monotonic() < deadline:
            state = self.snapshot()
            if tick:
                tick(state)
            with (self.directory / "samples.jsonl").open("a") as log:
                log.write(json.dumps({"time": time.time(), "checkpoint": label, **state}) + "\n")
            if time.monotonic() - self.last_display >= 5:
                self.display(state, label)
                self.last_display = time.monotonic()
            hits = hits + 1 if predicate(state) else 0
            if hits >= stable:
                print("\033[32;1m  PASS: " + label + "\033[0m", flush=True)
                self.checks.append(label)
                return state
            time.sleep(1)
        self.display(state, "TIMEOUT: " + label)
        raise TimeoutError(label)

    def hold(self, label, seconds, predicate):
        deadline = time.monotonic() + seconds
        def check(state):
            self.assert_(predicate(state), label)
            return time.monotonic() >= deadline
        return self.wait(label, check, timeout=seconds + 30, stable=1)

    def release(self, version=None, role=None, count=None, state=None):
        state = state or self.snapshot()
        groups = self.groups(state, version, role)
        groups.sort(key=lambda g: (not all(ready(p) for p in g), g[0]["metadata"]["name"]))
        if count is not None:
            groups = groups[:count]
        for members in groups:
            for pod in members:
                if not ready(pod) and pod.get("status", {}).get("phase") == "Running":
                    try:
                        self.k("-n", self.namespace, "exec", pod["metadata"]["name"], "-c", "demo", "--", "touch", "/tmp/ready")
                    except RuntimeError as error:
                        if "not found" not in str(error) and "not running" not in str(error):
                            raise

    def converge(self, version, counts, timeout=240):
        children = any("/" in name for name in counts)
        def complete(state):
            actual = self.subrole_counts(state, version) if children else self.counts(state, version)
            if any(actual.get(name.split("/")[-1] if children else name, {}).get("spec") != n or
                   actual.get(name.split("/")[-1] if children else name, {}).get("ready") != n for name, n in counts.items()):
                return False
            if not all(version_of(lws) == version or lws["spec"].get("replicas", 1) == 0 for lws in state["lws"]):
                return False
            # Convergence means physical completion too, not merely a lower
            # spec while old Pods remain stuck in Terminating.
            target = [lws for lws in state["lws"] if version_of(lws) == version]
            expected_pods = sum(lws["spec"].get("replicas", 1) * lws["spec"]["leaderWorkerTemplate"].get("size", 1) for lws in target)
            if len(state["pods"]) != expected_pods or any(p["metadata"].get("deletionTimestamp") or
                    p["metadata"].get("labels", {}).get(VERSION_LABEL) != version for p in state["pods"]):
                return False
            ds = state["ds"] or {}
            return ds.get("status", {}).get("observedGeneration") == ds.get("metadata", {}).get("generation")
        return self.wait(f"{version} converges to {counts}", complete, timeout=timeout, stable=3,
                         tick=lambda state: self.release(version=version, state=state))

    def restart_controller(self):
        pods = self.k("-n", "lws-system", "get", "pods", "-l", "control-plane=controller-manager", "-o", "json", json_output=True)
        self.assert_(len(pods["items"]) == 1, "exactly one owned controller to restart")
        name = pods["items"][0]["metadata"]["name"]
        uid = pods["items"][0]["metadata"]["uid"]
        self.k("-n", "lws-system", "delete", "pod", name, "--wait=false")
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            replacement = self.k("-n", "lws-system", "get", "pods", "-l", "control-plane=controller-manager", "-o", "json", json_output=True)["items"]
            if replacement and all(p["metadata"]["uid"] != uid for p in replacement) and any(ready(p) for p in replacement):
                break
            time.sleep(1)
        else:
            raise TimeoutError("Replacement manager Pod did not become Ready")
        self.k("-n", "lws-system", "rollout", "status", "deployment/lws-controller-manager", "--timeout=120s")
        # Manager /readyz can succeed before the Service points to the new
        # webhook listener. A server dry-run proves admission works without
        # editing the set or accepting a failed rollback as a controller bug.
        deadline = time.monotonic() + 60
        while True:
            try:
                self.k("-n", self.namespace, "patch", DS_RESOURCE, self.ds_name, "--type=merge", "--dry-run=server",
                       "-p", json.dumps({"metadata": {"annotations": {"harness.lws.x-k8s.io/admission-probe": "ready"}}}))
                break
            except RuntimeError as error:
                if "failed calling webhook" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def result(self, passed, error=None):
        value = {"case": self.case_name, "identity": self.identity, "policy": self.policy,
                 "passed": passed, "error": error, "seconds": round(time.monotonic() - self.started, 1),
                 "controllerCommit": self.args.controller_commit, "checks": self.checks}
        (self.directory / "result.json").write_text(json.dumps(value, indent=2) + "\n")
        return value
