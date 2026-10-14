#!/usr/bin/env python3
"""Scenario selection and reporting. run.sh owns the cluster and its lifecycle."""

import argparse
import importlib
import json
import signal
import sys
import traceback
from pathlib import Path

CASES = {
    "ordinary": ("cases_rollout", "ordinary", "asymmetric rollout + steady scaling; 2P/4D, MS1/MU0"),
    "scale-up": ("cases_scaling", "scale_up", "raise demand during unequal rollout progress; 4→6, MS1/MU0"),
    "scale-down": ("cases_scaling", "scale_down", "lower demand, preserve needed loading groups; 4→2, both policies"),
    "autoscaler-handoff": ("cases_scaling", "autoscaler_handoff", "Static→External without draining; 3P/2D, then /scale P 3→4→2"),
    "rename": ("cases_rollout", "rename", "renamed roles share released slots fairly; 2P/2D, MS0/MU1"),
    "restart-rollback": ("cases_scaling", "restart_rollback", "interrupt A→B with C, restart manager, roll back B; MS1/MU1"),
    "subroles": ("cases_scaling", "subroles", "relabel, shrink, restart, grow, roll one physical LWS; MS1/MU0 per child"),
    "native-restart": ("cases_scaling", "native_restart", "crash a worker then a leader; only their group is recreated"),
    "outage": ("cases_rollout", "outage", "1P/2D in three slots; MS0, P MU1 or100%, D MU1"),
    "emergency": ("cases_emergency", "emergency", "real bootstrap + ≥60s Unschedulable fallback; 1P/4D, MS1/MU0"),
    "large-rename": ("cases_rollout", "large_rename", "optional 9P/4D→8P/3D rename in thirteen slots; old MU1/new MU4"),
}


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("cases", nargs="*", help="case IDs, or all (the ten default families)")
    p.add_argument("--identity", choices=("ordinal", "hash", "both"), default="both")
    p.add_argument("--policy", choices=("Deferred", "Immediate"), help="select one scale-down policy instead of both")
    p.add_argument("--variant", choices=("integer", "percentage"), help="select one outage budget instead of both")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--interactive", dest="auto", action="store_false", help="Enter to trigger events; a to auto; q to stop")
    mode.add_argument("--auto", dest="auto", action="store_true", help="no prompts (default)")
    p.set_defaults(auto=True)
    p.add_argument("--include-large", action="store_true")
    p.add_argument("--fail-fast", action="store_true", help="stop after the first failed scenario")
    p.add_argument("--list", action="store_true", help="show the selected matrix without building anything")
    p.add_argument("--validate", action="store_true", help=argparse.SUPPRESS)
    for name in ("kubeconfig", "context", "run-dir", "node", "controller-commit", "observer"):
        p.add_argument("--" + name, help=argparse.SUPPRESS)
    return p


def matrix(args, p):
    names = args.cases or ["all"]
    unknown = set(names) - set(CASES) - {"all"}
    if unknown:
        p.error("unknown cases: " + ", ".join(sorted(unknown)))
    if "all" in names:
        names = [name for name in CASES if name != "large-rename"] + [name for name in names if name != "all"]
    if args.include_large:
        names.append("large-rename")
    identities = ("Ordinal", "Hash") if args.identity == "both" else (args.identity.title(),)
    result = []
    for name in dict.fromkeys(names):
        policies = ((args.policy,) if args.policy else ("Deferred", "Immediate")) if name == "scale-down" else ("Deferred",)
        variants = (args.variant,) if args.variant and name == "outage" else (("integer", "percentage") if name == "outage" else ("default",))
        for identity in identities:
            for policy in policies:
                for variant in variants:
                    result.append((name, identity, policy, variant))
    return result


def main():
    p = parser()
    args = p.parse_args()
    selected = matrix(args, p)
    if args.list or args.validate:
        print(f"{len(selected)} runs; slots are equal synthetic scheduler resources, not real GPUs.")
        for name in dict.fromkeys(item[0] for item in selected):
            print(f"  {name:18} {CASES[name][2]}")
            for _, identity, policy, variant in (item for item in selected if item[0] == name):
                print(f"    {identity:7} / {policy:9} / {variant}")
        return 0
    for required in ("kubeconfig", "context", "run_dir", "node", "controller_commit", "observer"):
        if not getattr(args, required):
            p.error("use run.sh to create the isolated cluster before running scenarios")
    if not args.context.startswith("kind-lws-v1-run-") or args.node != args.context[5:] + "-control-plane":
        p.error("refusing a context/node outside this harness's private kind naming scheme")
    from lab import Lab

    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    results, cancelled = [], False
    for index, (name, identity, policy, variant) in enumerate(selected, 1):
        case_name = name if variant == "default" else name + "-" + variant
        lab = Lab(args, identity, policy, case_name)
        lab.variant = variant
        lab.note(f"\n{'━' * 65}\n{index}/{len(selected)}  {case_name} · {identity} · {policy}\n{'━' * 65}")
        passed, error = False, None
        try:
            lab.reset()
            module, function, _ = CASES[name]
            getattr(importlib.import_module(module), function)(lab)
            lab.wait("read-only planner bounds captured", lambda state:
                     bool((lab.latest_diagnostic or {}).get("slices")), timeout=20)
            passed = True
        except KeyboardInterrupt:
            error, cancelled = "Interrupted by user or runtime", True
        except Exception as failure:
            error = str(failure)
            traceback.print_exc()
            try:
                (lab.directory / "failure-snapshot.json").write_text(json.dumps(lab.snapshot(), indent=2))
                (lab.directory / "controller-failure.log").write_text(lab.k("-n", "lws-system", "logs", "deployment/lws-controller-manager", "--tail=1500"))
            except Exception as capture_error:
                print(f"Could not capture additional failure state: {capture_error}", file=sys.stderr)
        finally:
            lab.close()
            args.auto = lab.auto
            results.append(lab.result(passed, error))
            (Path(args.run_dir) / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"\033[{'32' if passed else '31'};1m{'PASS' if passed else 'FAIL'}: {case_name} / {identity} / {policy}\033[0m", flush=True)
        if cancelled or (not passed and args.fail_fast):
            break
    print(f"\n{sum(r['passed'] for r in results)}/{len(results)} attempted runs passed; {len(selected)} selected.", flush=True)
    return 130 if cancelled else (0 if len(results) == len(selected) and all(r["passed"] for r in results) else 1)


if __name__ == "__main__":
    sys.exit(main())
