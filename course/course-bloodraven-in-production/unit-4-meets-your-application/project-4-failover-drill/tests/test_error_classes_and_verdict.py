import io
import json
import pathlib
import sys


def _root():
    here = pathlib.Path.cwd().resolve()
    for base in [here, *here.parents]:
        if (base / "tests" / "fixtures").is_dir():
            return base
    raise SystemExit("FAIL: cannot locate tests/fixtures from " + str(here))


ROOT = _root()
FIX = ROOT / "tests" / "fixtures"
for _cand in (ROOT, ROOT / "starter"):
    if (_cand / "brdrill.py").is_file():
        sys.path.insert(0, str(_cand))
        break
else:
    raise SystemExit("FAIL: brdrill.py not found in project root or starter/")
import brdrill  # noqa: E402


def check(condition, message):
    if not condition:
        print("FAIL: " + message)
        raise SystemExit(1)


check(tuple(sorted(brdrill.READ_ONLY_REFUSAL_CODES)) == (1290, 1792),
      "READ_ONLY_REFUSAL_CODES should stay (1290, 1792)")

# Planned drill, fixed writer: two read-only refusals (one 1290 from the
# fence, one 1792 from a transaction that was already open), six transport
# failures while mysql-orders-primary had no endpoint, and two lock-wait
# timeouts that a retry policy must NOT treat as a failover symptom.
planned = brdrill.error_classes(brdrill.load_probe(FIX / "planned-probe.jsonl"))
check(planned is not None, "error_classes returned None — TODO B is unimplemented")
check(set(planned) == {"readOnlyRefusal", "connection", "other"},
      "error_classes must always return exactly the three keys, got %r" % (sorted(planned),))
check(planned["readOnlyRefusal"] == 2,
      "readOnlyRefusal should be 2 — 1290 and 1792 are both refusals, got %r"
      % (planned["readOnlyRefusal"],))
check(planned["connection"] == 6, "connection should be 6, got %r" % (planned["connection"],))
check(planned["other"] == 2,
      "other should be 2 — a lock-wait timeout is not a read-only refusal, got %r"
      % (planned["other"],))

# Clean kill: nothing was refused, because nothing answered.
emergency = brdrill.error_classes(brdrill.load_probe(FIX / "emergency-probe.jsonl"))
check(emergency["readOnlyRefusal"] == 0,
      "a killed primary refuses nothing; readOnlyRefusal should be 0, got %r"
      % (emergency["readOnlyRefusal"],))
check(emergency["connection"] == 28, "connection should be 28, got %r" % (emergency["connection"],))

# Unfixed writer against a fenced but living primary: every write refused.
unbounded = brdrill.error_classes(brdrill.load_probe(FIX / "planned-probe-unbounded.jsonl"))
check(unbounded["readOnlyRefusal"] == 122,
      "readOnlyRefusal should be 122, got %r" % (unbounded["readOnlyRefusal"],))
check(unbounded["connection"] == 0,
      "the demoted primary was up the whole time; connection should be 0, got %r"
      % (unbounded["connection"],))

# The verdict is read from the drill, never from the gap.
planned_verdict = brdrill.verdict(brdrill.load_drill(FIX / "planned-drill.json"))
check(planned_verdict is not None, "verdict returned None — TODO D is unimplemented")
check(planned_verdict == "RPO 0 by construction (target GTID_EXECUTED contained "
                         "sourceGtidAtFence before promotion)",
      "unexpected planned verdict: %r" % (planned_verdict,))

emergency_verdict = brdrill.verdict(brdrill.load_drill(FIX / "emergency-drill.json"))
check(emergency_verdict == "RPO not established by this drill — audit divergentGtid "
                           "on the old primary",
      "unexpected emergency verdict: %r" % (emergency_verdict,))

# A planned failover that stopped short of Succeeded claims nothing.
stalled = json.loads((FIX / "planned-drill.json").read_text())
stalled["status"]["plannedFailover"]["phase"] = "WaitingForLag"
stalled_verdict = brdrill.verdict(stalled)
check("not established" in stalled_verdict and "WaitingForLag" in stalled_verdict,
      "a planned failover stuck in WaitingForLag must not claim RPO 0, got %r"
      % (stalled_verdict,))

print("PASS")
