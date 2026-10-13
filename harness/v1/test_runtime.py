"""Fast viewer/launcher safety checks. No Docker daemon or Kubernetes API is used."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


HERE = Path(__file__).resolve().parent


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="lws-v1-runtime-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.commands = self.root / "bin"
        self.commands.mkdir()
        self.run = self.root / "run with spaces"
        self.run.mkdir()
        (self.run / "kubeconfig").write_text("isolated fixture\n")
        self.log = self.root / "viewer.json"
        self.environment = dict(os.environ, PATH=f"{self.commands}:{os.environ['PATH']}",
                                TEST_VIEWER_LOG=str(self.log))
        self.command("kubectl", """#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
assert args[0] == '--kubeconfig' and args[2] == '--context'
assert args[3] == 'kind-lws-v1-test'
if 'get-contexts' in args:
    print(os.environ.get('TEST_CONTEXTS', 'kind-lws-v1-test'))
elif 'current-context' in args:
    print('kind-lws-v1-test')
elif 'view' in args:
    print(os.environ.get('TEST_SERVER', 'https://127.0.0.1:12345'))
else:
    raise AssertionError(args)
""")
        self.viewer = self.command("custom viewer", """#!/usr/bin/env python3
import json, os, sys
with open(os.environ['TEST_VIEWER_LOG'], 'w') as output:
    json.dump({'args': sys.argv[1:], 'kubeconfig': os.environ['KUBECONFIG'],
               'config': os.environ['K9S_CONFIG_DIR']}, output)
""")

    def command(self, name, source):
        path = self.commands / name
        path.write_text(source)
        path.chmod(0o755)
        return path

    def view(self, **environment):
        return subprocess.run(["bash", str(HERE / "view.sh"), str(self.run),
                               "kind-lws-v1-test", str(self.viewer)],
                              env=dict(self.environment, **environment),
                              capture_output=True, text=True, timeout=5)

    def test_explicit_private_context_and_read_only_viewer(self):
        result = self.view()
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads(self.log.read_text())
        args = invocation["args"]
        self.assertEqual(args[args.index("--kubeconfig") + 1], str(self.run / "kubeconfig"))
        self.assertEqual(args[args.index("--context") + 1], "kind-lws-v1-test")
        self.assertEqual(args[args.index("--namespace") + 1], "v1-harness")
        self.assertIn("--readonly", args)
        self.assertEqual(invocation["config"], str(self.run / "k9s-config"))

    def test_other_context_refused(self):
        result = self.view(TEST_CONTEXTS="kind-lws-v1-test\nstaging")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.log.exists())

    def test_remote_api_refused(self):
        result = self.view(TEST_SERVER="https://production.example:6443")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.log.exists())

    def test_help_and_list_do_not_need_baseline_or_docker(self):
        harness = self.root / "checkout/harness/v1"
        harness.mkdir(parents=True)
        shutil.copyfile(HERE / "run.sh", harness / "run.sh")
        (harness / "suite.py").write_text("import sys; print(' '.join(sys.argv[1:]))\n")
        self.command("docker", "#!/bin/sh\necho 'Docker must not run' >&2\nexit 91\n")
        for args, expected in [([], "--list"), (["--no-k9s"], "--list"), (["--help"], "--help"),
                               (["--list", "--no-k9s"], "--list")]:
            with self.subTest(args=args):
                result = subprocess.run(["bash", str(harness / "run.sh"), *args],
                                        env=self.environment, capture_output=True,
                                        text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines()[0], expected)
                if args == ["--help"]:
                    self.assertIn("--no-k9s", result.stdout)

    def test_invalid_suite_arguments_fail_before_any_setup(self):
        harness = self.root / "checkout/harness/v1"
        harness.mkdir(parents=True)
        shutil.copyfile(HERE / "run.sh", harness / "run.sh")
        (harness / "suite.py").write_text(
            "import sys; assert '--validate' in sys.argv; sys.exit(2)\n")
        result = subprocess.run(["bash", str(harness / "run.sh"), "unknown-scenario"],
                                env=self.environment, capture_output=True,
                                text=True, timeout=5)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse((harness / ".state").exists())

    def test_cleanup_ownership_handles_only_exact_docker_desktop_translation(self):
        self.command("uname", "#!/bin/sh\nprintf '%s\\n' \"$TEST_OS\"\n")
        # Exercise the actual shell predicate without invoking cluster setup.
        body = (HERE / "run.sh").read_text().split("owns_node() {\n", 1)[1].split("\n}\n", 1)[0]
        script = "owns_node() {\n" + body + '\n}\nowns_node "$1" "$2" "$3"\n'
        expected = str(self.run / "owner")
        for os_name, node_id, label, mount, accepted in [
            ("Darwin", "owned-id", "owned-cluster", expected, True),
            ("Darwin", "owned-id", "owned-cluster", "/host_mnt" + expected, True),
            ("Linux", "owned-id", "owned-cluster", "/host_mnt" + expected, False),
            ("Darwin", "owned-id", "owned-cluster", "/host_mnt" + expected + "-other", False),
            ("Darwin", "replacement-id", "owned-cluster", "/host_mnt" + expected, False),
            ("Darwin", "owned-id", "other-cluster", "/host_mnt" + expected, False),
        ]:
            with self.subTest(os=os_name, node=node_id, label=label, mount=mount):
                result = subprocess.run(["bash", "-c", script, "ownership-test", node_id, label, mount],
                                        env=dict(self.environment, TEST_OS=os_name, NODE_ID="owned-id",
                                                 CLUSTER="owned-cluster", RUN_DIR=str(self.run)),
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode == 0, accepted, result.stderr)


if __name__ == "__main__":
    unittest.main()
