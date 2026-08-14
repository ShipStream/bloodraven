"""The four graded checks for brstatus.

Run them all locally with:

    python tests/test_brstatus.py

Each one is also invoked on its own by the project's test cases.
"""

from __future__ import annotations

import _harness
from _harness import calls_within, function_def, run, source, strings_within


def test_healthy_group_summary():
    """playground is healthy: one writable site, both followers caught up."""
    result = run("playground-healthy.json")

    assert result.code == 0, result.explain("expected exit 0 on a healthy group")
    assert result.verdict == "OK", result.explain("expected VERDICT: OK")

    assert result.header.get("active") == "iad", result.explain(
        "header must report active=iad"
    )
    assert result.header.get("ready") == "True", result.explain(
        "header must report ready=True from the Ready condition"
    )
    assert result.header.get("degraded") == "False(Healthy)", result.explain(
        "header must report degraded=False(Healthy) from the Degraded condition"
    )

    iad = result.site("iad")
    assert iad["ROLE"] == "primary-candidate", result.explain("iad is a candidate")
    assert iad["STATE"] == "writable", result.explain("iad is the writable site")
    assert iad["LAG"] == "unknown", result.explain(
        "the active primary reports no secondsBehindSource at all; that is "
        "unknown, not 0s"
    )
    assert iad["SERVING"] == "no", result.explain(
        "the primary carries role=primary and is never behind "
        "mysql-playground-replicas"
    )

    pdx = result.site("pdx")
    assert pdx["LAG"] == "0s", result.explain("pdx reports 0 seconds behind")
    assert pdx["SERVING"] == "yes", result.explain("pdx is a caught-up replica")

    reader = result.site("reader")
    assert reader["ROLE"] == "read-only", result.explain("reader is a read-only site")
    assert reader["SERVING"] == "yes", result.explain(
        "a converged, caught-up reader serves reads"
    )


def test_lagging_reader_is_not_an_unhealthy_group():
    """The inversion. Lag means opposite things depending on the role."""
    soaking = run("playground-reader-soaking.json")

    assert soaking.code == 0, soaking.explain(
        "a read-only reader 300s behind a 30s threshold does not degrade the "
        "group: the operator's replication conditions skip read-only sites "
        "entirely, and the fixture's Degraded condition says False(Healthy)"
    )
    assert soaking.verdict == "OK", soaking.explain("expected VERDICT: OK")
    assert soaking.site("reader")["LAG"] == "300s", soaking.explain(
        "the reader is 300s behind"
    )
    assert soaking.site("reader")["SERVING"] == "no", soaking.explain(
        "300s is past readOnlyMaxLagSeconds (inherited 30s), so the reader "
        "drops out of mysql-playground-replicas"
    )
    assert soaking.site("pdx")["SERVING"] == "yes", soaking.explain(
        "pdx is caught up and still serving"
    )
    assert soaking.site("iad")["ROLE"] == "primary-candidate", soaking.explain(
        "iad omits spec.sites[].role, which defaults to primary-candidate"
    )

    lagging = run("playground-candidate-lagging.json")

    assert lagging.code == 1, lagging.explain(
        "a primary-candidate replica 300s behind DOES degrade the group; the "
        "Degraded condition reads True(ReplicationLagging) and an active site "
        "remains, so the verdict is DEGRADED"
    )
    assert lagging.verdict == "DEGRADED", lagging.explain("expected VERDICT: DEGRADED")
    assert lagging.header.get("degraded") == "True(ReplicationLagging)", (
        lagging.explain("header must carry the Degraded reason")
    )
    assert lagging.site("pdx")["SERVING"] == "yes", lagging.explain(
        "the lag gate applies only to read-only sites; a lagging "
        "primary-candidate replica keeps healthy=yes and stays behind "
        "mysql-playground-replicas"
    )
    assert lagging.site("reader")["SERVING"] == "yes", lagging.explain(
        "the reader is caught up here"
    )


def test_awkward_status_null_lag_and_lost_authority():
    """Absent is not zero, an explicit zero threshold is meaningful, and a
    group with no authority sheds every endpoint."""
    detached = run("playground-reader-detached.json")

    assert detached.code == 0, detached.explain(
        "a detached reader still does not degrade the group"
    )
    reader = detached.site("reader")
    assert reader["REPL"] == "no", detached.explain("replication is stopped")
    assert reader["LAG"] == "unknown", detached.explain(
        "secondsBehindSource is absent; rendering it as 0s makes a detached "
        "replica look caught up"
    )
    assert reader["SERVING"] == "no", detached.explain(
        "not replicating, not converged, and following the wrong source"
    )

    zero = run("playground-zero-readonly-lag.json")

    assert zero.code == 0, zero.explain("this group is not degraded")
    assert zero.site("reader")["SERVING"] == "no", zero.explain(
        "readOnlyMaxLagSeconds is explicitly 0, so 30s of lag is too much; an "
        "explicit zero is meaningful and must not fall back to maxLagSeconds"
    )
    assert zero.site("pdx")["SERVING"] == "yes", zero.explain(
        "pdx is a primary-candidate: no lag gate applies to it at all"
    )

    none = run("playground-no-primary.json")

    assert none.code == 2, none.explain(
        "Degraded is True and there is no active site: that is CRITICAL"
    )
    assert none.verdict == "CRITICAL", none.explain("expected VERDICT: CRITICAL")
    assert none.header.get("active") == "none", none.explain(
        "status.activeSite is empty"
    )
    for name in ("iad", "pdx", "reader"):
        assert none.site(name)["SERVING"] == "no", none.explain(
            "invalid authority sheds every endpoint, including %s" % name
        )

    split = run("playground-split-brain.json")

    assert split.code == 2, split.explain(
        "two writable sites leave activeSite empty, so the verdict is CRITICAL"
    )
    assert split.header.get("degraded") == "True(SplitBrain)", split.explain(
        "header must carry the SplitBrain reason"
    )


def test_verdict_reads_conditions_and_reader_threshold():
    """Structural: the two rules must be implemented where they belong."""
    text, tree = source()

    verdict = function_def(tree, "verdict")
    verdict_strings = strings_within(verdict)
    verdict_calls = calls_within(verdict)
    assert "Degraded" in verdict_strings, (
        "verdict() must read the Degraded condition the operator writes. No "
        "'Degraded' string appears in it."
    )
    assert "condition" in verdict_calls or "conditions" in verdict_strings, (
        "verdict() must reach status.conditions - call the condition() helper "
        "rather than re-deriving group health from the site rows."
    )

    serving = function_def(tree, "is_serving")
    serving_calls = calls_within(serving)
    assert "effective_readonly_max_lag" in serving_calls, (
        "is_serving() must gate a read-only reader on "
        "effective_readonly_max_lag(spec). The group threshold is the wrong "
        "one: readOnlyMaxLagSeconds has no default and an explicit 0 is "
        "meaningful."
    )
    assert "read-only" in strings_within(serving), (
        "is_serving() must branch on the site's role; the reader rule and the "
        "candidate rule are not the same rule."
    )
    assert "secondsBehindSource" not in verdict_strings, (
        "verdict() must not look at per-site lag. Group health comes from the "
        "conditions."
    )
    assert "unknown" in strings_within(function_def(tree, "format_lag")), (
        "format_lag() must be able to return 'unknown' for an absent "
        "secondsBehindSource."
    )
    assert len(text.splitlines()) > 0


CHECKS = [
    test_healthy_group_summary,
    test_lagging_reader_is_not_an_unhealthy_group,
    test_awkward_status_null_lag_and_lost_authority,
    test_verdict_reads_conditions_and_reader_threshold,
]


if __name__ == "__main__":
    import sys

    failed = 0
    print("brstatus.py -> %s" % _harness.find_tool())
    for check in CHECKS:
        try:
            check()
        except AssertionError as err:
            failed += 1
            print("FAIL %s\n  %s" % (check.__name__, err))
        else:
            print("PASS %s" % check.__name__)
    sys.exit(1 if failed else 0)
