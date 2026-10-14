"""Cheap harness checks before spending time on a kind cluster."""

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lab import Lab, SLOT, VERSION_LABEL, role_spec
from suite import matrix, parser


class SuiteTests(unittest.TestCase):
    def test_default_matrix_is_bounded_and_balanced(self):
        p = parser()
        runs = matrix(p.parse_args([]), p)
        self.assertEqual(len(runs), 24)
        self.assertEqual(sum(r[1] == "Hash" for r in runs), 12)
        self.assertEqual(len(set(runs)), len(runs))
        self.assertEqual(len(matrix(p.parse_args(["--include-large"]), p)), 26)
        self.assertEqual({r[1] for r in runs if r[0] == "autoscaler-handoff"}, {"Ordinal", "Hash"})

    def test_single_case_selection(self):
        p = parser()
        args = p.parse_args(["outage", "--identity", "hash", "--variant", "percentage"])
        self.assertEqual(matrix(args, p), [("outage", "Hash", "Deferred", "percentage")])

    def test_readiness_and_budget_manifest(self):
        role = role_spec("prefill", 1, "Hash", "B", surge=0, unavailable="100%", size=2, auto_ready=False)
        spec = role["spec"]
        self.assertEqual(spec["groupIdentity"], "Hash")
        self.assertEqual(spec["rolloutStrategy"]["rollingUpdateConfiguration"], {"maxSurge": 0, "maxUnavailable": "100%"})
        template = spec["leaderWorkerTemplate"]["workerTemplate"]
        self.assertEqual(template["metadata"]["labels"][VERSION_LABEL], "B")
        container = template["spec"]["containers"][0]
        self.assertEqual(container["env"][0]["value"], "0")
        self.assertEqual(container["resources"]["requests"][SLOT], "1")
        self.assertEqual(container["readinessProbe"]["exec"]["command"], ["test", "-f", "/tmp/ready"])

    def test_readiness_release_counts_existing_ready_groups_first(self):
        def pod(name, is_ready):
            return {"metadata": {"name": name}, "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True" if is_ready else "False"}]}}
        with tempfile.TemporaryDirectory(prefix="lws-v1-unit-") as directory:
            args = argparse.Namespace(run_dir=directory, auto=True, kubeconfig="/private/test", context="kind-owned")
            lab = Lab(args, "Hash", "Deferred", "fixture")
            groups = [[pod("z-already-ready", True)], [pod("a-loading", False)], [pod("b-loading", False)]]
            with patch.object(lab, "groups", return_value=groups), patch.object(lab, "k") as command:
                lab.release(state={"fixture": True}, count=2)
                self.assertEqual(command.call_count, 1)
                self.assertIn("a-loading", command.call_args.args)
            self.assertEqual(lab.kube[1:5], ["--kubeconfig", "/private/test", "--context", "kind-owned"])

    def test_manual_event_switch_to_auto(self):
        with tempfile.TemporaryDirectory(prefix="lws-v1-unit-") as directory:
            args = argparse.Namespace(run_dir=directory, auto=False, kubeconfig="/private/test", context="kind-owned")
            lab = Lab(args, "Ordinal", "Deferred", "fixture")
            with patch("builtins.input", return_value="a") as prompt:
                lab.checkpoint("first", "change template")
                lab.checkpoint("second", "open readiness")
                prompt.assert_called_once()
            self.assertTrue(lab.auto)
            self.assertEqual(len((Path(lab.directory) / "events.jsonl").read_text().splitlines()), 2)

    def test_convergence_waits_for_physical_cleanup_and_status(self):
        with tempfile.TemporaryDirectory(prefix="lws-v1-unit-") as directory:
            args = argparse.Namespace(run_dir=directory, auto=True, kubeconfig="/private/test", context="kind-owned")
            lab = Lab(args, "Hash", "Deferred", "fixture")
            state = {"ds": {"metadata": {"generation": 2}, "status": {"observedGeneration": 2}},
                     "lws": [{"spec": role_spec("prefill", 1, "Hash", "B")["spec"]}],
                     "pods": [{"metadata": {"labels": {VERSION_LABEL: "B"}}}]}
            with patch.object(lab, "wait") as wait, patch.object(lab, "counts", return_value={"prefill": {"spec": 1, "ready": 1}}):
                lab.converge("B", {"prefill": 1})
                complete = wait.call_args.args[1]
                self.assertTrue(complete(state))
                state["pods"].append({"metadata": {"deletionTimestamp": "now", "labels": {VERSION_LABEL: "A"}}})
                self.assertFalse(complete(state))
                state["pods"].pop()
                state["ds"]["status"]["observedGeneration"] = 1
                self.assertFalse(complete(state))

    def test_restart_waits_for_actual_admission_without_persisting_a_probe(self):
        with tempfile.TemporaryDirectory(prefix="lws-v1-unit-") as directory:
            args = argparse.Namespace(run_dir=directory, auto=True, kubeconfig="/private/test", context="kind-owned")
            lab = Lab(args, "Ordinal", "Deferred", "fixture")
            def manager(uid):
                return {"items": [{"metadata": {"name": "manager-" + uid, "uid": uid},
                                   "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
            replies = [manager("old"), "deleted", manager("new"), "available",
                       RuntimeError("failed calling webhook: connection refused"), "dry-run passed"]
            with patch.object(lab, "k", side_effect=replies) as command, patch("lab.time.sleep"):
                lab.restart_controller()
            for call in command.call_args_list[-2:]:
                self.assertIn("--dry-run=server", call.args)


if __name__ == "__main__":
    unittest.main()
