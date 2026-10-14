"""Checkpoint predicates must catch incomplete assignment and hidden fleet replacement."""

import copy
import json
import unittest
from unittest.mock import DEFAULT, patch

from cases_rollout import RENAMED, ROLES, _rename_capacity, rename_outage
from cases_scaling import _assigned, _handoff_preserved
from lab import (CHILD_COUNTS, GROUP_LABEL, LWS_LABEL, PREFIX, ROLE_LABEL,
                 SUBROLE_LABEL, VERSION_LABEL, WORKER_INDEX, Lab, role_spec)


class AssignmentCheckpointTests(unittest.TestCase):
    def setUp(self):
        # These Lab helpers are pure snapshot readers: no Kubernetes calls,
        # subprocesses, or initialization of a real run are needed.
        self.lab = object.__new__(Lab)
        self.state = {
            "lws": [{
                "metadata": {"name": "shared", "labels": {ROLE_LABEL: "decode"},
                             "annotations": {CHILD_COUNTS: json.dumps({"a": 1})}},
                "spec": {"replicas": 1, "leaderWorkerTemplate": {
                    "size": 2, "workerTemplate": {"metadata": {"labels": {VERSION_LABEL: "A"}}}}},
            }],
            "pods": [self.pod(index) for index in ("0", "1")],
        }

    @staticmethod
    def pod(index):
        return {
            "metadata": {"name": "pod-" + index, "uid": "uid-" + index, "labels": {
                ROLE_LABEL: "decode", SUBROLE_LABEL: "a", WORKER_INDEX: index,
                LWS_LABEL: "shared", GROUP_LABEL: "group", VERSION_LABEL: "A"}},
            "spec": {"nodeName": "local"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }

    def assigned(self):
        return _assigned(self.lab, self.state, {"a": 1})

    def test_complete_published_assignment(self):
        self.assertTrue(self.assigned())

    def test_labels_alone_do_not_replace_published_counts(self):
        self.state["lws"][0]["metadata"]["annotations"][CHILD_COUNTS] = '{"a":2}'
        self.assertFalse(self.assigned())

    def test_pending_plan_is_not_settled_for_either_identity(self):
        for plan in ({"a": 1}, {"counts": {"a": 1}, "readyFloor": {"a": 1}}):
            with self.subTest(plan=plan):
                self.state["lws"][0]["metadata"]["annotations"][PREFIX + "subrole-plan"] = json.dumps(plan)
                self.assertFalse(self.assigned())

    def test_missing_worker(self):
        self.state["pods"].pop()
        self.assertFalse(self.assigned())

    def test_two_leaders_are_not_a_whole_group(self):
        self.state["pods"][1]["metadata"]["labels"][WORKER_INDEX] = "0"
        self.assertFalse(self.assigned())

    def test_members_must_have_the_same_child(self):
        self.state["pods"][1]["metadata"]["labels"][SUBROLE_LABEL] = "b"
        self.assertFalse(self.assigned())

    def test_terminating_member_is_not_a_survivor(self):
        self.state["pods"][1]["metadata"]["deletionTimestamp"] = "2026-10-13T12:00:00Z"
        self.assertFalse(self.assigned())

    def test_every_member_must_be_ready(self):
        self.state["pods"][1]["status"]["conditions"][0]["status"] = "False"
        self.assertFalse(self.assigned())

    def test_physical_parent_must_match_the_child_sum(self):
        self.state["lws"][0]["spec"]["replicas"] = 2
        self.assertFalse(self.assigned())


class AutoscalerHandoffTests(unittest.TestCase):
    def setUp(self):
        self.lab = object.__new__(Lab)
        self.before = {"lws": [], "pods": []}
        for role, count in (("prefill", 3), ("decode", 2)):
            self.before["lws"].append({
                "metadata": {"name": role, "uid": "lws-" + role, "labels": {ROLE_LABEL: role}},
                "spec": role_spec(role, count, "Hash", "A", size=2)["spec"],
            })
            for group in range(count):
                for worker in ("0", "1"):
                    pod = AssignmentCheckpointTests.pod(worker)
                    name = f"{role}-{group}-{worker}"
                    pod["metadata"].update(name=name, uid="uid-" + name)
                    pod["metadata"]["labels"].update({ROLE_LABEL: role, LWS_LABEL: role,
                                                       GROUP_LABEL: f"{role}-{group}"})
                    self.before["pods"].append(pod)
        self.state = copy.deepcopy(self.before)

    def preserved(self):
        return _handoff_preserved(self.lab, self.state, self.before)

    def test_same_fleet_is_preserved(self):
        self.assertTrue(self.preserved())

    def test_requested_replicas_drop_to_zero_before_pods_disappear(self):
        self.state["lws"][0]["spec"]["replicas"] = 0
        with self.assertRaises(AssertionError):
            self.preserved()

    def test_complete_recreation_is_caught_even_after_ready_counts_recover(self):
        for pod in self.state["pods"]:
            pod["metadata"]["uid"] += "-replacement"
        self.assertEqual(self.lab.counts(self.state, "A"), self.lab.counts(self.before, "A"))
        with self.assertRaises(AssertionError):
            self.preserved()

    def test_one_replaced_or_terminating_pod_is_rejected(self):
        for field, value in (("uid", "replacement"), ("deletionTimestamp", "2026-10-14T12:00:00Z")):
            with self.subTest(field=field):
                self.state = copy.deepcopy(self.before)
                self.state["pods"][0]["metadata"][field] = value
                with self.assertRaises(AssertionError):
                    self.preserved()

    def test_lost_worker_readiness_is_rejected(self):
        self.state["pods"][1]["status"]["conditions"][0]["status"] = "False"
        with self.assertRaises(AssertionError):
            self.preserved()

    def test_replaced_lws_revision_is_rejected(self):
        self.state["lws"][0]["metadata"]["uid"] = "new-lws-revision"
        with self.assertRaises(AssertionError):
            self.preserved()

    def test_mode_patch_changes_only_the_requested_replica_source(self):
        roles = [role_spec(role, count, "Hash", "A", size=2)
                 for role, count in (("prefill", 3), ("decode", 2))]
        original = copy.deepcopy(roles)
        with patch.object(self.lab, "k", return_value={"spec": {"roles": roles}}), \
                patch.object(self.lab, "patch_set") as write:
            self.lab.update(external_roles=("prefill",))
        expected = copy.deepcopy(original)
        expected[0]["scaling"] = {"mode": "External"}
        del expected[0]["spec"]["replicas"]
        write.assert_called_once_with({"spec": {"roles": expected}})
        self.assertEqual(roles, original, "Updating the role must not mutate the read snapshot")


class RenameOutageTests(unittest.TestCase):
    def setUp(self):
        self.lab = object.__new__(Lab)

    @staticmethod
    def fleet(old=(0, 0), new=(2, 3)):
        state = {"lws": [], "pods": [], "events": []}
        for version, roles, counts in (("A", ROLES, old), ("B", RENAMED, new)):
            for role, count in zip(roles, counts):
                state["lws"].append({
                    "metadata": {"name": role, "labels": {ROLE_LABEL: role}},
                    "spec": role_spec(role, count, "Hash", version, surge=0, unavailable="100%")["spec"],
                })
                for group in range(count):
                    pod = AssignmentCheckpointTests.pod("0")
                    pod["metadata"].update(name=f"{role}-{group}", uid=f"uid-{role}-{group}")
                    pod["metadata"]["labels"].update({ROLE_LABEL: role, LWS_LABEL: role,
                                                       GROUP_LABEL: f"{role}-{group}", VERSION_LABEL: version})
                    pod["status"]["conditions"][0]["status"] = "True" if version == "A" else "False"
                    state["pods"].append(pod)
        return state

    def test_capacity_accepts_full_outage_but_not_per_role_surge(self):
        self.assertTrue(_rename_capacity(self.lab, self.fleet(), (2, 3)))
        for old, new in (((1, 0), (2, 2)), ((0, 1), (1, 3))):
            with self.subTest(old=old, new=new):
                self.assertEqual(sum(old) + sum(new), 5, "Total alone would miss this violation")
                with self.assertRaises(AssertionError):
                    _rename_capacity(self.lab, self.fleet(old, new), (2, 3))

    def test_emergency_allowances_are_not_needed(self):
        for reason in ("BootstrapSurge", "AvailabilityFallback"):
            with self.subTest(reason=reason):
                state = self.fleet()
                state["events"] = [{"reason": reason}]
                with self.assertRaises(AssertionError):
                    _rename_capacity(self.lab, state, (2, 3))

    def test_loading_requires_full_drain_and_complete_unready_replacement(self):
        with patch.object(self.lab, "create") as create, \
                patch.multiple(self.lab, update=DEFAULT, wait=DEFAULT, hold=DEFAULT,
                               checkpoint=DEFAULT, release=DEFAULT, converge=DEFAULT) as calls:
            calls["converge"].return_value = self.fleet()
            rename_outage(self.lab)
        create.assert_called_once_with(2, 3, surge=0, unavailable="100%", slots=5)
        calls["update"].assert_called_once_with(version="B", names=RENAMED, ready=False)
        loading = calls["wait"].call_args_list[0].args[1]
        self.assertIs(calls["hold"].call_args.args[2], loading)
        self.assertTrue(loading(self.fleet()))
        self.assertFalse(loading(self.fleet(old=(1, 2), new=(1, 1))))
        self.assertFalse(loading(self.fleet(new=(2, 2))))
        state = self.fleet()
        state["pods"][-1]["spec"].pop("nodeName")
        self.assertFalse(loading(state), "Requested Pods are not necessarily scheduled")
        state = self.fleet()
        state["pods"][0]["status"]["conditions"][0]["status"] = "True"
        with self.assertRaises(AssertionError):
            loading(state)

    def test_rename_preserves_both_percentage_budgets(self):
        roles = [role_spec(role, count, "Hash", "A", surge=0, unavailable="100%")
                 for role, count in zip(ROLES, (2, 3))]
        with patch.object(self.lab, "k", return_value={"spec": {"roles": roles}}), \
                patch.object(self.lab, "patch_set") as write:
            self.lab.update(version="B", names=RENAMED, ready=False)
        updated = write.call_args.args[0]["spec"]["roles"]
        self.assertEqual([role["name"] for role in updated], list(RENAMED))
        for role in updated:
            self.assertEqual(role["spec"]["rolloutStrategy"]["rollingUpdateConfiguration"],
                             {"maxSurge": 0, "maxUnavailable": "100%"})


if __name__ == "__main__":
    unittest.main()
