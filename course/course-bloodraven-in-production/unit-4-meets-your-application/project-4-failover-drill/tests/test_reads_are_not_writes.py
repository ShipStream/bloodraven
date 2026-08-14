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


# The baseline capture: a planned move of the primary from pdx to iad, run
# against the unfixed writer — one pool for reads and writes, no bounded
# connection lifetime, no read/write split. The surviving connection keeps
# serving successful READS from the demoted pdx for the whole outage while
# every WRITE is refused with 1290. Treat a successful read as recovery and
# the gap collapses from 63.5 s to 2.25 s.
samples = brdrill.load_probe(FIX / "planned-probe-unbounded.jsonl")
drill = brdrill.load_drill(FIX / "planned-drill-unbounded.json")

gap = brdrill.write_gap(samples, drill)
check(gap is not None, "write_gap returned None — TODO A is unimplemented")
check(gap["gapSeconds"] != 2.25,
      "gapSeconds is 2.25: successful reads on the demoted site were counted as "
      "recovery. Only op == 'write' with ok true ends a write-gap.")
check(gap["gapSeconds"] == 63.5,
      "gapSeconds should be 63.5 (10:00:04.500Z -> 10:01:08.000Z), got %r" % (gap["gapSeconds"],))

# The primary moved pdx -> iad in this drill. A tool that hardcodes the site
# names from the emergency capture reports the wrong pair, or no gap at all.
check(gap["oldSite"] == "pdx", "oldSite should be pdx in this drill, got %r" % (gap["oldSite"],))
check(gap["newSite"] == "iad", "newSite should be iad in this drill, got %r" % (gap["newSite"],))

# The stale-read window: successful reads served by the demoted site at or
# after status.lastFailover (2026-08-12T10:00:38Z), ending when the NoExecute
# taint's tolerationSeconds expired and the pod was evicted.
stale = brdrill.stale_read_window(samples, drill)
check(stale is not None, "stale_read_window returned None — TODO C is unimplemented")
check(stale["count"] == 56, "staleReads should be 56, got %r" % (stale["count"],))
check(stale["seconds"] == 27.5, "staleReadSeconds should be 27.5, got %r" % (stale["seconds"],))
check(brdrill.iso(stale["first"]) == "2026-08-12T10:00:38.250Z",
      "first stale read should be 2026-08-12T10:00:38.250Z, got %r"
      % (brdrill.iso(stale["first"]),))

# And the fixed writer on the same procedure: 4.5 s, no stale reads at all.
fixed = brdrill.write_gap(brdrill.load_probe(FIX / "planned-probe.jsonl"),
                          brdrill.load_drill(FIX / "planned-drill.json"))
check(fixed["gapSeconds"] == 4.5, "the fixed-pool planned drill is 4.5 s, got %r"
      % (fixed["gapSeconds"],))
check(round(fixed["gapSeconds"] - gap["gapSeconds"], 3) == -59.0,
      "the pool fix should show as a 59.0 s improvement")

print("PASS")
