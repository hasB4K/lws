# Validation — 14 October 2026

Controller: `cf833e2421f105e16d4366a2cc17057a1b5bec17`, the pinned integration of upstream main, #1181 and #1172 described in `baseline.json`. Environment: macOS/arm64, Docker Desktop, kind Kubernetes v1.36.1, k9s v0.51.0. No existing cluster was used.

## Live results

All **28 distinct scenarios** have a passing kind execution across the runs below: 26 default runs plus the two optional larger rename runs. The expanded 28-case matrix has not been rerun as a single invocation.

| Family | Ordinal | Hash |
|---|---|---|
| Ordinary rollout and steady scaling | Pass | Pass |
| Scale up during rollout | Pass | Pass |
| Scale down during rollout | Pass — Deferred and Immediate | Pass — Deferred and Immediate |
| Static → External autoscaler handoff, then `/scale` up/down | Pass | Pass |
| Rename and released capacity | Pass | Pass |
| Rename both roles with MS0/MU100%, then recover from full outage | Pass | Pass |
| Interrupted rollout, controller restart, rollback | Pass after harness startup fix | Pass, including rerun |
| Subrole assignment, shrink/grow, restart and rollout | Pass | Pass |
| Worker then leader group recreation | Pass | Pass |
| 1P/2D explicit outage | Pass — integer and percentage MU | Pass — integer and percentage MU |
| Emergency bootstrap and bounded unavailable fallback | Pass | Pass |
| Optional 9P/4D → 8P/3D rename | Pass | Pass |

The original full matrix used `all --include-large --auto` and took about 19 minutes of scenario execution, excluding setup. It returned **23/24**, not a false green: the Ordinal rollback request hit the controller's webhook before its Service was reachable, despite the manager Pod being Ready. The harness now waits for a successful **server-side dry-run** after restart. Both identities were rerun with that fix and passed. No controller behavior was changed to make a test pass.

The added `autoscaler-handoff --auto` run passed **2/2**, using the installed kind v0.33.0 from PATH without a `KIND` override. Ordinal took 40.4 seconds and Hash 37.5 seconds; the invocation took 2 minutes 6 seconds including setup and cleanup. Both started with 3P/2D in two-Pod groups, inherited the three-group Prefill target on switching to External, kept all ten original Pods Ready during a 15-second pause before the first scaler write, and then completed Prefill 3→4→2 through `/scale` without disturbing Decode. No metrics-driven HPA was deployed.

The added `rename-outage --auto` run passed **2/2**: Ordinal took 43.0 seconds and Hash 40.6 seconds. Both used 2P/3D, five slots, and MS0/MU100% on both old and new roles. All old groups drained and all renamed groups scheduled before any replacement was Ready. After an eight-second full-outage hold, Prefill became Ready first, then Decode restored the complete service. Sampled requests stayed within the zero-surge ceilings and no bootstrap/fallback event was recorded. The owned cluster was removed successfully.

The emergency runs used genuine scheduler rejection. In both identities, the first availability-fallback event occurred 60 seconds after the Pod's Unschedulable transition (whole-second comparison). The tests then held the replacement unready, checked that the surviving old revision kept its original Pod UIDs, and finally completed at 1P/4D. The fixture seeds interrupted ownership/history metadata, not Pod status or timestamps.

Local evidence is retained under `.state/` and deliberately excluded from Git:

- `run.MCi3yM`: full 24-run matrix, raw samples, observer output, emergency snapshots and events.
- `run.GZYcJK`: both restart/rollback reruns; Enter triggered an event and `a` enabled automatic mode for the remaining cases; successful exit and cluster cleanup.
- `run.Phiwqt`: both new autoscaler-handoff cases passed; successful exit and owned-cluster cleanup.
- `run.vEE7Hx`: both rename-outage cases passed; full downtime, recovery and zero-surge checks; successful exit and owned-cluster cleanup.
- `run.gyhK6Z`: deliberate Ctrl-C at an interactive checkpoint; exit 130, observer stopped and owned kind node removed.
- Earlier bring-up logs remain available too, including the intentional SIGTERM cleanup test.

## Runtime and code checks

- 33 Python tests passed: scenario selection, assignment predicates, autoscaler handoff and mode patching, full-outage rename checkpoints and budgets, physical completion, admission readiness, private-context/viewer arguments and cleanup ownership checks. The handoff checks reject zero replicas, a recreated or terminating Pod, lost readiness, and replacement of an LWS.
- Bash syntax and tagged observer build/vet/tests passed.
- `go test ./pkg/controllers/disaggregatedset ./pkg/replicagroups ./pkg/utils/disaggregatedset -count=1` passed on the merged baseline.
- Actual k9s started against the private kind context with `[R]` read-only mode and the correct namespace. It displayed live Pods. Runtime cleanup stopped this viewer and removed its owned node.
- Both SIGTERM during a real run and Ctrl-C at an interactive checkpoint removed only the run's cluster and stopped its observer. Successful runs also removed their clusters. Artifacts and cached images are intentionally retained.

**Ghostty window launch/close was not live-verified.** Its scripting endpoint timed out, and computer-use access was restricted. The launcher compiles against the installed Ghostty dictionary, requests a new window rather than a tab, and has bounded launch/cleanup timeouts. No existing Ghostty windows were changed. Interactive checkpoints were verified separately with `--no-k9s`; real k9s was verified in its own terminal.

This is focused regression evidence, not exhaustive release qualification. See the README for the scope limits, especially real GPU scheduling, delayed-write fault injection and the unmerged in-place restart prototype.
