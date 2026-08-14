#!/usr/bin/env python3
"""Local copy of the four graded test cases in project.json.

Run it from the project directory once your brfence.py is finished:

    python3 tests/test_brfence.py

Each check prints PASS on success. The graded harness runs the same code.
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path.cwd()
_cands = [p for p in ROOT.rglob("brfence.py") if ".git" not in p.parts]
assert _cands, "brfence.py not found under the working directory"
_cands.sort(key=lambda p: ("starter" in p.parts, len(p.parts), str(p)))
TOOL = _cands[0]
FIX = next(p for p in ROOT.rglob("fixtures") if p.is_dir())

ROW = re.compile(r"^\s*(\d{4}-\d{2}-\d{2}T\S+)\s+(\S+)\s+(\S+)\s+(\S+)")


def run(bundle):
    r = subprocess.run([sys.executable, str(TOOL), str(FIX / bundle)],
                       capture_output=True, text=True)
    return r.returncode, r.stdout


def rows(out):
    return [(m.group(2), m.group(3), m.group(4))
            for m in (ROW.match(line) for line in out.splitlines()) if m]


def flat_source():
    src = TOOL.read_text(encoding="utf-8")
    src = re.sub(r'"\s*(?:\\\n\s*)?"', "", src)
    src = re.sub(r"'\s*(?:\\\n\s*)?'", "", src)
    return re.sub(r"\s+", " ", src)


def canonical_timeline():
    code, out = run("partition-a")
    assert code == 0, f"expected exit 0 on a clean bundle, got {code}"
    assert "3 fence events" in out, "partition-a holds exactly 3 fence events"
    assert rows(out) == [("iad", "rule-2", "correct"),
                         ("reader", "safety-net", "correct"),
                         ("iad", "rule-1", "correct")], rows(out)
    assert "0 premature" in out
    assert "(none)" in out, "no site in partition-a is left unfenced"
    print("PASS")


def awkward_bundle():
    code, out = run("split-brain-tier3")
    assert code == 1, f"expected exit 1 when there are findings, got {code}"
    assert "3 fence events" in out
    assert rows(out) == [("reader", "safety-net", "correct"),
                         ("reader", "non-promotable", "correct"),
                         ("iad", "rule-2", "premature")], rows(out)
    assert "1 premature" in out
    assert "1 unfenced writable site" in out
    section = out.split("UNFENCED WRITABLE SITES", 1)[1].split("VERDICT", 1)[0]
    assert section.strip().startswith("pdx"), section
    print("PASS")


def decoy_lines_are_not_fences():
    code, out = run("decoys")
    assert code == 0, f"expected exit 0, got {code}"
    assert rows(out) == [("iad", "rule-1", "correct")], rows(out)
    assert "1 fence event" in out and "1 fence events" not in out
    assert "0 premature" in out
    print("PASS")


def stable_msg_vocabulary():
    flat = flat_source()

    def has(needle):
        return re.sub(r"\s+", " ", needle) in flat

    for needle in (
        "SELF-FENCING: topology mismatch — operator-authoritative active site "
        "disagrees with our site, setting super_read_only=ON",
        "SELF-FENCING: Bloodraven and every peer unreachable beyond lease timeout, "
        "setting super_read_only=ON",
        "safety net: could not query active site, staying fenced",
        "safety net: no active site reported by operator, staying fenced",
        "safety net: confirmed standby site, staying fenced",
    ):
        assert has(needle), f"missing verbatim msg string: {needle[:60]}..."
    for field in ("bloodravenLastOk", "latestPeerOk", "leaseTimeout"):
        assert field in flat, f"rule-2 verdict never reads {field}"
    print("PASS")


if __name__ == "__main__":
    for check in (canonical_timeline, awkward_bundle,
                  decoy_lines_are_not_fences, stable_msg_vocabulary):
        print(f"--- {check.__name__}")
        check()
