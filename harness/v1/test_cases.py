"""Assignment checkpoints must not mistake a partial transition for completion."""

import json
import unittest

from cases_scaling import _assigned
from lab import (CHILD_COUNTS, GROUP_LABEL, LWS_LABEL, PREFIX, ROLE_LABEL,
                 SUBROLE_LABEL, VERSION_LABEL, WORKER_INDEX, Lab)


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


if __name__ == "__main__":
    unittest.main()
