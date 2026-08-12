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


# The probe process was killed 2 s before the promotion landed, so this
# capture never records a write completing on the new primary. An unmeasured
# gap must be reported as unmeasured — a 0 here would be a claim the capture
# does not support.
samples = brdrill.load_probe(FIX / "unclosed-probe.jsonl")
drill = brdrill.load_drill(FIX / "emergency-drill.json")

gap = brdrill.write_gap(samples, drill)
check(gap is not None, "write_gap returned None — TODO A is unimplemented")
check(gap["closed"] is False, "closed should be False when no write completed on the new site")
check(gap["gapSeconds"] is None,
      "gapSeconds should be None, not %r — an open gap is not a zero gap" % (gap["gapSeconds"],))
check(gap["firstWriteNewSite"] is None, "firstWriteNewSite should be None in this capture")
check(brdrill.iso(gap["lastWriteOldSite"]) == "2026-08-11T09:14:05.500Z",
      "lastWriteOldSite is still known: 2026-08-11T09:14:05.500Z")

# The CLI must say so out loud and exit 2.
buf = io.StringIO()
saved, sys.stdout = sys.stdout, buf
try:
    code = brdrill.main(["--probe", str(FIX / "unclosed-probe.jsonl"),
                         "--drill", str(FIX / "emergency-drill.json")])
finally:
    sys.stdout = saved
out = buf.getvalue()
check(code == 2, "exit code should be 2 for an unclosed gap, got %r" % (code,))
check("UNCLOSED" in out, "the record should print UNCLOSED for writeGapSeconds, got:\n" + out)

print("PASS")
