"""Ordinary rollout, full role rename, and explicitly permitted outage cases."""

from lab import REVISION_LABEL


ROLES = ("prefill", "decode")
RENAMED = ("prefill-v2", "decode-v2")


def _count(lab, state, version, role, field="spec"):
    return lab.counts(state, version).get(role, {}).get(field, 0)


def _revisions(state):
    return {lws["metadata"]["labels"][REVISION_LABEL] for lws in state["lws"]}


def _reason(state, reason):
    return any(event.get("reason") == reason for event in state["events"])


def ordinary(lab):
    lab.create(2, 4, surge=1, unavailable=0)
    initial = lab.snapshot()
    revisions = _revisions(initial)
    workloads = {lws["metadata"]["uid"] for lws in initial["lws"]}
    for desired in ((4, 6), (2, 4)):
        lab.checkpoint(f"Steady-state resize to {desired[0]} Prefill / {desired[1]} Decode.",
                       "Change replica counts only; keep template A.")
        lab.update(counts=dict(zip(ROLES, desired)))
        state = lab.converge("A", dict(zip(ROLES, desired)))
        lab.assert_(_revisions(state) == revisions, "A count-only edit created a new revision")
        lab.assert_({lws["metadata"]["uid"] for lws in state["lws"]} == workloads,
                    "Steady scaling replaced a physical LeaderWorkerSet")

    lab.checkpoint("Roll 2 Prefill / 4 Decode to B, with MS=1 / MU=0 per role.",
                   "Change templates to B; hold their application readiness.")
    lab.update(version="B", ready=False)
    lab.wait("Both target roles have a scheduled loading group", lambda state: all(
        _count(lab, state, "B", role, "scheduled") >= 1 for role in ROLES))
    lab.checkpoint("Make Prefill Ready first; Decode is deliberately slow.",
                   "Open only B's first Prefill readiness file.")
    lab.release(version="B", role="prefill", count=1)
    lab.wait("B Prefill is Ready, but B Decode is not", lambda state:
             _count(lab, state, "B", "prefill", "ready") == 1 and
             _count(lab, state, "B", "decode", "ready") == 0)

    def protected(state):
        for role, floor in zip(ROLES, (2, 4)):
            total = sum(_count(lab, state, version, role, "ready") for version in ("A", "B"))
            usable = sum(_count(lab, state, version, role, "ready") for version in ("A", "B")
                         if all(_count(lab, state, version, peer, "ready") > 0 for peer in ROLES))
            lab.assert_(total >= floor, f"Independent {role} Ready floor was crossed")
            lab.assert_(usable >= floor, f"Same-revision serving capacity for {role} was lost")
        return True

    lab.hold("Healthy Prefill cannot make an incomplete B revision serve Decode traffic", 8, protected)
    lab.note("The ordinary surge ceilings are P=3, D=5. Any bootstrap is separately marked in the observer/events.")
    lab.checkpoint("Release Decode and all subsequent B groups.", "Open B readiness files until rollout completes.")
    lab.converge("B", {"prefill": 2, "decode": 4})


def _renamed_rollout(lab, old, desired, new_unavailable):
    lab.create(*old, surge=0, unavailable=1, slots=sum(old))
    lab.checkpoint(f"Rename both roles; old MU=1 survives, new MU={new_unavailable} is separate.",
                   f"Replace names with {RENAMED}, request {desired}, and hold B readiness.")
    lab.update(version="B", names=RENAMED, counts=dict(zip(RENAMED, desired)),
               surge=0, unavailable=new_unavailable, ready=False)

    def first_batch(state):
        return all(_count(lab, state, "A", role) == initial - 1
                   for role, initial in zip(ROLES, old)) and all(
            _count(lab, state, "B", role) == 1 and
            _count(lab, state, "B", role, "scheduled") == 1 for role in RENAMED)

    lab.wait("Observe old capacity released before the first renamed pair schedules", first_batch)
    lab.checkpoint("Only new Prefill becomes Ready; new Decode must get its turn.",
                   "Open the first new Prefill readiness file; keep Decode closed.")
    lab.release(version="B", role=RENAMED[0], count=1)
    lab.wait("New Prefill is Ready and Decode is still loading", lambda state:
             _count(lab, state, "B", RENAMED[0], "ready") == 1 and
             _count(lab, state, "B", RENAMED[1], "ready") == 0)

    def fair_and_safe(state):
        for role, initial in zip(ROLES, old):
            lab.assert_(_count(lab, state, "A", role, "ready") >= initial - 1,
                        f"Renaming spent more than the saved old {role} allowance")
        for role in RENAMED:
            lab.assert_(_count(lab, state, "B", role) <= 1,
                        "A faster renamed role grew again before every target role became Ready")
        lab.assert_(not _reason(state, "BootstrapSurge"), "Bootstrap bypassed renamed-role released-capacity limits")
        lab.assert_(not _reason(state, "AvailabilityFallback"), "Renamed roles used same-role scheduler recovery")
        return True

    lab.hold("No repeated fast-role growth, no extra old-budget spending", 8, fair_and_safe)
    lab.checkpoint("Release Decode and complete replacement using the same fixed slots.",
                   "Open all present and future B readiness files.")
    lab.converge("B", dict(zip(RENAMED, desired)), timeout=360)


def rename(lab):
    _renamed_rollout(lab, old=(2, 2), desired=(2, 2), new_unavailable=1)


def large_rename(lab):
    _renamed_rollout(lab, old=(9, 4), desired=(8, 3), new_unavailable=4)


def outage(lab):
    percentage = getattr(lab, "variant", "integer") == "percentage"
    p_unavailable = "100%" if percentage else 1
    lab.create(1, 2, surge=0, unavailable={"prefill": p_unavailable, "decode": 1}, slots=3)
    lab.checkpoint(f"Roll 1P/2D with no spare slot; Prefill MU={p_unavailable} explicitly allows outage.",
                   "Change A to B; hold all B application readiness.")
    lab.update(version="B", ready=False)

    def ordinary_capacity(state):
        for role, ceiling in zip(ROLES, (1, 2)):
            requested = sum(_count(lab, state, version, role) for version in ("A", "B"))
            lab.assert_(requested <= ceiling, f"The outage case exceeded {role}'s zero-surge ceiling")

    def drained_to_fit(state):
        ordinary_capacity(state)
        lab.assert_(not _reason(state, "BootstrapSurge"),
                    "The permitted-outage rollout unnecessarily requested bootstrap capacity")
        return (_count(lab, state, "A", "prefill") == 0 and
                _count(lab, state, "A", "decode") == 1 and
                all(_count(lab, state, "B", role) == 1 and
                    _count(lab, state, "B", role, "scheduled") == 1 for role in ROLES))

    lab.wait("A=0P/1D and B=1P/1D fit into three real scheduler slots", drained_to_fit)

    def keep_decode(state):
        ordinary_capacity(state)
        ready = sum(_count(lab, state, version, "decode", "ready") for version in ("A", "B"))
        lab.assert_(ready >= 1, "Permitted Prefill outage erased Decode's independent Ready floor")
        lab.assert_(not _reason(state, "BootstrapSurge"), "A fourth group slot was required")
        return True

    lab.hold("Prefill outage is allowed; at least one Ready Decode remains protected", 8, keep_decode)
    lab.checkpoint("Make the first replacement pair Ready and finish on the same three slots.",
                   "Open B readiness files; its second Decode is created after old Decode retires.")
    state = lab.converge("B", {"prefill": 1, "decode": 2})
    lab.assert_(not _reason(state, "BootstrapSurge"), "The rollout used extra capacity despite its outage budget")
