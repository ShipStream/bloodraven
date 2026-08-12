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


# Canonical drill: iad scaled to 0, operator promotes pdx, writer with a
# bounded-lifetime pool and a read/write split. The gap runs from the last
# write completed on iad to the first completed on pdx.
samples = brdrill.load_probe(FIX / "emergency-probe.jsonl")
drill = brdrill.load_drill(FIX / "emergency-drill.json")

gap = brdrill.write_gap(samples, drill)
check(gap is not None, "write_gap returned None — TODO A is unimplemented")
check(gap["oldSite"] == "iad", "oldSite should be iad, got %r" % (gap["oldSite"],))
check(gap["newSite"] == "pdx", "newSite should be pdx, got %r" % (gap["newSite"],))
check(gap["closed"] is True, "the gap closed in this capture; closed should be True")
check(gap["gapSeconds"] == 14.0,
      "gapSeconds should be 14.0 (09:14:05.500Z -> 09:14:19.500Z), got %r" % (gap["gapSeconds"],))
check(brdrill.iso(gap["lastWriteOldSite"]) == "2026-08-11T09:14:05.500Z",
      "lastWriteOldSite should be 2026-08-11T09:14:05.500Z, got %r"
      % (brdrill.iso(gap["lastWriteOldSite"]),))
check(brdrill.iso(gap["firstWriteNewSite"]) == "2026-08-11T09:14:19.500Z",
      "firstWriteNewSite should be 2026-08-11T09:14:19.500Z, got %r"
      % (brdrill.iso(gap["firstWriteNewSite"]),))

# A clean kill leaves no stale reads: the host is gone, so the reads fail too.
# Reads that succeed on `reader` report read_only=1 by design and are not stale.
stale = brdrill.stale_read_window(samples, drill)
check(stale is not None, "stale_read_window returned None — TODO C is unimplemented")
check(stale["count"] == 0,
      "a clean kill produces 0 stale reads; reads on `reader` are not stale, got %r"
      % (stale["count"],))
check(stale["seconds"] == 0.0, "staleReadSeconds should be 0.0, got %r" % (stale["seconds"],))

# The record the CLI prints must carry the same numbers.
record = brdrill.build_record(samples, drill)
check(record["writeGapSeconds"] == 14.0, "record writeGapSeconds should be 14.0")
check(record["gapClosed"] is True, "record gapClosed should be True")
check(record["verdict"] is not None, "verdict returned None — TODO D is unimplemented")
check(record["verdict"].startswith("RPO not established by this drill"),
      "an emergency drill cannot claim an RPO; got %r" % (record["verdict"],))

print("PASS")
