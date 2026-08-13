#!/usr/bin/env python3
"""Grading harness for brdecide.

Every check runs against the JSON fixtures in tests/fixtures — no cluster, no
API server. Each entry point raises AssertionError with a readable diff on
failure and returns quietly on success.

    python -c "import sys; sys.path.insert(0,'tests'); import harness; harness.canonical(); print('PASS')"

Set BRDECIDE=/path/to/brdecide.py to point the harness at your file; otherwise it
looks in ./brdecide.py, ./starter/brdecide.py, ./solution/brdecide.py, ./work/brdecide.py.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import json
import os
import subprocess
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "fixtures")
ROOT = os.path.dirname(HERE)
NOW = "2026-08-12T12:04:16Z"

_CANDIDATES = [
    os.environ.get("BRDECIDE"),
    os.path.join(ROOT, "brdecide.py"),
    os.path.join(ROOT, "starter", "brdecide.py"),
    os.path.join(ROOT, "solution", "brdecide.py"),
    os.path.join(ROOT, "work", "brdecide.py"),
    os.path.join(os.getcwd(), "brdecide.py"),
    os.path.join(os.getcwd(), "starter", "brdecide.py"),
]


def locate() -> str:
    for candidate in _CANDIDATES:
        if candidate and os.path.isfile(candidate):
            return candidate
    raise AssertionError(
        "cannot find brdecide.py — set BRDECIDE=/path/to/brdecide.py"
    )


def load_module():
    path = locate()
    spec = importlib.util.spec_from_file_location("brdecide_under_test", path)
    module = importlib.util.module_from_spec(spec)
    # Register before exec: dataclasses resolve their module from sys.modules.
    sys.modules["brdecide_under_test"] = module
    spec.loader.exec_module(module)
    return module


def run(fixture: str, now: str = NOW) -> dict:
    """Run the tool over one fixture and return the parsed JSON decision."""
    path = os.path.join(FIXTURES, fixture)
    proc = subprocess.run(
        [sys.executable, locate(), "--status", path, "--now", now, "--json"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"{fixture}: exited {proc.returncode}\nstderr: {proc.stderr.strip()}"
        )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as err:
        raise AssertionError(f"{fixture}: --json did not print JSON ({err})\n{proc.stdout[:400]}")


def _close(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) < 0.001
    except (TypeError, ValueError):
        return False


def check(fixture: str, expected: dict, now: str = NOW) -> None:
    got = run(fixture, now)
    problems = []
    for key, want in expected.items():
        if key not in got:
            problems.append(f"  {key}: missing from the decision")
            continue
        have = got[key]
        if isinstance(want, float) and _close(want, have):
            continue
        if have != want:
            problems.append(f"  {key}: expected {want!r}, got {have!r}")
    if problems:
        raise AssertionError(f"{fixture}:\n" + "\n".join(problems))


# --------------------------------------------------------------------------
# 1. The canonical decisions for `playground`
# --------------------------------------------------------------------------
CANONICAL = {
    "playground-healthy.json": {
        "coreCount": 2, "writable": ["iad"], "readOnly": ["pdx"], "unreachable": [],
        "fenceSites": [], "reason": "Healthy", "alert": None, "splitBrain": False,
        "promotionCandidates": [], "willRun": [],
    },
    "playground-iad-down.json": {
        "coreCount": 2, "writable": [], "readOnly": ["pdx"], "unreachable": ["iad"],
        "reason": "Degraded", "alert": None, "promotionCandidates": ["pdx"],
        "promotionBlockedBy": None, "willRun": ["promote"],
    },
    "playground-reader-writable.json": {
        "coreCount": 2, "writable": ["iad"], "fenceSites": ["reader"],
        "reason": "Degraded", "splitBrain": False,
        "alert": "writable non-promotable site requires fencing (reader)",
        "promotionCandidates": [], "willRun": ["fence:reader"],
    },
    # Order is the behaviour: two writable candidates AND a writable reader.
    # Fence-first returns before the split-brain row is ever reached.
    "playground-reader-writable-split-brain.json": {
        "coreCount": 2, "writable": ["iad", "pdx"], "fenceSites": ["reader"],
        "reason": "Degraded", "splitBrain": False,
        "alert": "writable non-promotable site requires fencing (reader)",
        "promotionCandidates": [], "willRun": ["fence:reader"],
    },
    "playground-split-brain.json": {
        "coreCount": 2, "writable": ["iad", "pdx"], "fenceSites": [],
        "reason": "SplitBrain", "splitBrain": True,
        "alert": "SPLIT BRAIN: 2 sites are writable (iad, pdx)",
        "promotionCandidates": [],
    },
    "playground-all-read-only.json": {
        "coreCount": 2, "readOnly": ["iad", "pdx"], "unreachable": [],
        "reason": "NoPrimary", "alert": "NO PRIMARY: both sites are read-only",
        "promotionCandidates": [], "willRun": [],
    },
    "playground-total-loss.json": {
        "coreCount": 2, "unreachable": ["iad", "pdx"], "reason": "TotalLoss",
        "alert": "TOTAL LOSS: all sites are unreachable", "promotionCandidates": [],
    },
    "playground-peer-down.json": {
        "coreCount": 2, "writable": ["iad"], "unreachable": ["pdx"],
        "reason": "Degraded", "alert": "pdx unreachable while iad is primary",
        "promotionCandidates": [], "willRun": [],
    },
}


def canonical() -> None:
    for fixture, expected in CANONICAL.items():
        check(fixture, expected)


# --------------------------------------------------------------------------
# 2. Awkward topologies and the two durable history copies
# --------------------------------------------------------------------------
AWKWARD = {
    # A dr-only site is non-promotable but still counts toward coreCount and
    # still lands in a tally. Excluding it like a reader would read 2 == 2 and
    # report TotalLoss; promoting it would name lhr as a candidate.
    "playground-dr-only.json": {
        "coreCount": 3, "readOnly": ["lhr"], "unreachable": ["iad", "pdx"],
        "reason": "NoPrimary", "alert": "NO PRIMARY: no writable site available",
        "promotionCandidates": [], "willRun": [],
    },
    # Fence-first also preempts TotalLoss: both candidates are unreachable and
    # the reader is writable, so len(unreachable) == coreCount never gets asked.
    "playground-reader-writable-total-loss.json": {
        "coreCount": 2, "unreachable": ["iad", "pdx"], "fenceSites": ["reader"],
        "reason": "Degraded",
        "alert": "writable non-promotable site requires fencing (reader)",
        "willRun": ["fence:reader"],
    },
    # Every site unknown at startup: no tally holds anything, and the message is
    # the general one, not the two-site one.
    "playground-unknown-startup.json": {
        "coreCount": 2, "writable": [], "readOnly": [], "unreachable": [],
        "reason": "NoPrimary", "alert": "NO PRIMARY: no writable site available",
    },
    # sitePriorities reorders the tiebreak list; the site that is not listed goes last.
    "playground-priority-order.json": {
        "coreCount": 3, "reason": "Degraded", "promotionCandidates": ["sfo", "pdx"],
        "willRun": ["promote"],
    },
    # A site absent from status.sites is unknown, not missing.
    "playground-reader-unpolled.json": {
        "coreCount": 2, "writable": ["iad"], "readOnly": ["pdx"], "reason": "Healthy",
    },
    # The annotation is an hour ahead — outside the 5m grace, so it is discarded.
    "playground-history-skewed.json": {
        "lastFailover": "2026-08-12T12:03:16Z", "lastFailoverSource": "status",
        "lastFailoverTarget": "pdx", "cooldownRemaining": 240.0,
        "promotionBlockedBy": "cooldown", "willRun": [],
    },
    # Equal timestamps describe the same promotion: the tie goes to status.
    "playground-history-tie.json": {
        "lastFailoverSource": "status", "cooldownRemaining": 44.0,
    },
    # Stamped 2m ahead but inside the grace: kept, and negative elapsed time
    # still counts as inside the cooldown.
    "playground-history-near-future.json": {
        "lastFailoverSource": "status", "cooldownRemaining": 420.0,
        "promotionBlockedBy": "cooldown", "willRun": [],
    },
}


def awkward() -> None:
    for fixture, expected in AWKWARD.items():
        check(fixture, expected)


# --------------------------------------------------------------------------
# 3. ADVERSARIAL — the cooldown gates promotion and nothing else
# --------------------------------------------------------------------------
def cooldown_gate() -> None:
    # A writable reader 10s into a 30s cooldown. Fencing a writable
    # non-promotable site is not gated: it runs every poll. A tool that wraps
    # the whole decision in the cooldown emits an empty willRun here.
    check("playground-reader-writable-cooldown.json", {
        "reason": "Degraded",
        "alert": "writable non-promotable site requires fencing (reader)",
        "fenceSites": ["reader"],
        "promotionCandidates": [],
        "promotionBlockedBy": None,
        "cooldown": 30.0,
        "cooldownRemaining": 20.0,
        "willRun": ["fence:reader"],
    })

    # 90s into the shipped 5m cooldown with a promotion selected. The table
    # still ran: the reason and the candidate list are unchanged, and only the
    # promotion is withheld. A tool that returns early on cooldown loses them.
    check("playground-iad-down-cooldown.json", {
        "reason": "Degraded",
        "alert": None,
        "promotionCandidates": ["pdx"],
        "promotionBlockedBy": "cooldown",
        "cooldownRemaining": 210.0,
        "willRun": [],
    })

    # The annotation copy is an hour newer than the status copy. Reading status
    # alone puts the promotion outside the default 5m cooldown and lets it run.
    check("playground-history-conflict.json", {
        "lastFailover": "2026-08-12T12:00:00Z",
        "lastFailoverSource": "annotation",
        "lastFailoverTarget": "pdx",
        "cooldown": 300.0,
        "cooldownRemaining": 44.0,
        "promotionBlockedBy": "cooldown",
        "willRun": [],
    })

    # Same topology, no history at all: the promotion is not blocked.
    check("playground-iad-down.json", {
        "promotionBlockedBy": None,
        "cooldownRemaining": 0.0,
        "willRun": ["promote"],
    })


# --------------------------------------------------------------------------
# 4. Structural — the table stays pure
# --------------------------------------------------------------------------
FORBIDDEN_IN_TABLE = ("cooldown", "last_failover", "lastfailover", "datetime.now",
                      "time.time", "clock")


def _code_body(func) -> str:
    """The executable body of a function, with its docstring and comments stripped."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    node = tree.body[0]
    if (node.body and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)):
        node.body = node.body[1:] or [ast.Pass()]
    return ast.unparse(tree).lower()


def purity() -> None:
    module = load_module()

    for name in ("evaluate_cross_site", "tally", "apply_gate", "Observation"):
        if not hasattr(module, name):
            raise AssertionError(f"brdecide.py no longer defines {name}")

    signature = inspect.signature(module.evaluate_cross_site)
    if list(signature.parameters) != ["observations", "site_priorities"]:
        raise AssertionError(
            "evaluate_cross_site must stay pure: it takes (observations, "
            f"site_priorities), not {list(signature.parameters)}"
        )

    body = _code_body(module.evaluate_cross_site) + _code_body(module.tally)
    for needle in FORBIDDEN_IN_TABLE:
        if needle in body:
            raise AssertionError(
                f"the cross-site table references {needle!r}. EvalCrossSite is pure — "
                "it never consults history, policy or a clock. Keep the gate in apply_gate()."
            )

    gate = _code_body(module.apply_gate)
    if "cooldown" not in gate:
        raise AssertionError("apply_gate() does not mention the cooldown at all")

    # The table must return the same action whatever the clock says.
    Observation = module.Observation
    observations = [
        Observation("iad", "primary-candidate", "unreachable"),
        Observation("pdx", "primary-candidate", "read-only"),
        Observation("reader", "read-only", "read-only"),
    ]
    action = module.evaluate_cross_site(observations, ["iad", "pdx"])
    if action["reason"] != "Degraded" or action["promotionCandidates"] != ["pdx"]:
        raise AssertionError(
            "evaluate_cross_site called directly returned "
            f"reason={action.get('reason')!r} candidates={action.get('promotionCandidates')!r}; "
            "expected 'Degraded' and ['pdx']"
        )
    if action["alert"] is not None:
        raise AssertionError(
            "the failover row sets no alert — it is the only acting row without one; "
            f"got {action['alert']!r}"
        )


ALL = {"canonical": canonical, "awkward": awkward,
       "cooldown_gate": cooldown_gate, "purity": purity}


if __name__ == "__main__":
    selected = sys.argv[1:] or list(ALL)
    for label in selected:
        ALL[label]()
        print(f"ok  {label}")
    print("PASS")
