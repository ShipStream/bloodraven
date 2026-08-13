#!/usr/bin/env python3
"""brdrill — turn a failover drill capture into a drill record.

You give it two artefacts from one drill against the `playground` failover group:

  --probe   a JSONL probe log, one line per write or read attempt your writer made
  --drill   a JSON capture of the drill: what you triggered, and the group status
            afterwards (activeSite, lastFailover, lastFailoverTarget, and for a
            planned move, status.plannedFailover)

It prints a drill record: the measured write-gap, the stale-read window, the
error classes your writer actually saw, and an RPO verdict.

Everything peripheral is wired already — argument parsing, JSONL and JSON
loading, timestamp parsing, record assembly, both output formats, and the exit
codes. Four functions are left for you. Each is marked TODO A .. TODO D and is
referenced by letter from the project instructions.

Usage:
  python3 brdrill.py --probe P.jsonl --drill D.json
  python3 brdrill.py --probe P.jsonl --drill D.json --json
  python3 brdrill.py --probe P.jsonl --drill D.json \
      --baseline B.jsonl --baseline-drill BD.json

Exit codes: 0 record complete and the gap closed; 1 something is still
`not computed`; 2 the gap never closed in this capture.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

# The two MySQL error codes a write gets from a demoted primary. A pooled
# connection that survived a promotion fails here first — never on a health
# check, which passes, because the server is alive and merely read-only.
READ_ONLY_REFUSAL_CODES = (1290, 1792)


# --------------------------------------------------------------------------
# Wired: loading and parsing.
# --------------------------------------------------------------------------

def parse_ts(value: str) -> datetime:
    """Parse an RFC3339 timestamp from a probe log or a status field."""
    if not isinstance(value, str) or not value:
        raise ValueError("not a timestamp: %r" % (value,))
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt):
    """Render a datetime back as RFC3339 with millisecond precision."""
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def load_probe(path):
    """Load a JSONL probe log. Blank lines are skipped; `ts` is parsed into
    `at`. Samples are returned in timestamp order, whatever order they were
    written in."""
    samples = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit("%s:%d: not JSON: %s" % (path, lineno, exc))
            try:
                row["at"] = parse_ts(row.get("ts"))
            except ValueError as exc:
                raise SystemExit("%s:%d: %s" % (path, lineno, exc))
            samples.append(row)
    if not samples:
        raise SystemExit("%s: no probe samples" % path)
    samples.sort(key=lambda r: r["at"])
    return samples


def load_drill(path):
    """Load a drill capture and check the fields every record needs."""
    with open(path, "r", encoding="utf-8") as fh:
        drill = json.load(fh)
    for section in ("drill", "status"):
        if section not in drill:
            raise SystemExit("%s: missing top-level %r" % (path, section))
    for field in ("mode", "group", "demotedSite"):
        if not drill["drill"].get(field):
            raise SystemExit("%s: missing drill.%s" % (path, field))
    for field in ("lastFailover", "lastFailoverTarget"):
        if not drill["status"].get(field):
            raise SystemExit("%s: missing status.%s" % (path, field))
    return drill


def demoted_site(drill):
    """The site the primary moved off. You record it when you run the drill."""
    return drill["drill"]["demotedSite"]


def promoted_site(drill):
    """The site the primary moved to, straight out of the group status."""
    return drill["status"]["lastFailoverTarget"]


def promotion_instant(drill):
    """When the operator stamped the promotion. `status.lastFailover` is
    RFC3339 at second precision, so it is coarser than your probe log."""
    return parse_ts(drill["status"]["lastFailover"])


# --------------------------------------------------------------------------
# TODO A — write_gap
#
# Return a dict describing the write-gap: the interval between the last write
# your application completed against the demoted site and the first it
# completed against the promoted one.
#
#   {"oldSite": str, "newSite": str,
#    "lastWriteOldSite": datetime|None, "firstWriteNewSite": datetime|None,
#    "gapSeconds": float|None, "closed": bool}
#
# `lastWriteOldSite` is the last sample with op == "write", ok true, and
# site == demoted_site(drill). `firstWriteNewSite` is the earliest sample with
# op == "write", ok true, site == promoted_site(drill), and a timestamp after
# `lastWriteOldSite`. `gapSeconds` is the difference in seconds, rounded to
# three decimals. `closed` is True only when both ends were found; when it is
# False, `gapSeconds` is None — do not fall back to 0.
#
# Reads are not writes. A read that succeeds during the outage tells you the
# site is alive, not that your application recovered.
# --------------------------------------------------------------------------

def write_gap(samples, drill):
    return None  # TODO A


# --------------------------------------------------------------------------
# TODO B — error_classes
#
# Return counts of the failed samples by error class, with all three keys
# always present:
#
#   {"readOnlyRefusal": int, "connection": int, "other": int}
#
# Classify each sample with ok false by its error code:
#   * code in READ_ONLY_REFUSAL_CODES  -> "readOnlyRefusal"
#   * code is null/missing             -> "connection" (the client never got a
#                                          server error; the transport failed)
#   * anything else                    -> "other"
#
# This is the split that decides what your retry policy may retry. Blanket
# retry-everything replays statements that failed for reasons a retry cannot fix.
# --------------------------------------------------------------------------

def error_classes(samples):
    return None  # TODO B


# --------------------------------------------------------------------------
# TODO C — stale_read_window
#
# Return the window during which reads kept succeeding against the demoted
# site after the promotion had already been stamped:
#
#   {"count": int, "first": datetime|None, "last": datetime|None,
#    "seconds": float}
#
# Count samples with op == "read", ok true, site == demoted_site(drill), and a
# timestamp at or after promotion_instant(drill). `seconds` is last - first
# rounded to three decimals, or 0.0 when there are fewer than two.
#
# Two traps. `readOnly` is true on the `reader` site by design, so a reader
# that reports read_only=1 is not stale — the site is. And a clean kill
# produces no stale reads at all: the host is gone, so the reads fail. Zero
# here is a real answer, not a missing one.
# --------------------------------------------------------------------------

def stale_read_window(samples, drill):
    return None  # TODO C


# --------------------------------------------------------------------------
# TODO D — verdict
#
# Return the RPO verdict for this drill as a string. Exactly these three:
#
#   mode "planned", status.plannedFailover.phase == "Succeeded" and
#   transactionsLost == 0:
#     "RPO 0 by construction (target GTID_EXECUTED contained sourceGtidAtFence
#      before promotion)"            <- one line, single spaces
#
#   mode "planned", any other phase:
#     "planned failover did not reach Succeeded (phase=<phase>) — RPO not established"
#
#   mode "emergency":
#     "RPO not established by this drill — audit divergentGtid on the old primary"
#
# A write-gap is not an RPO. The gap says how long your application could not
# write; the RPO says how much committed work the promotion threw away. Only
# the planned path can claim zero, and it claims it from the GTID superset
# gate, not from this capture.
# --------------------------------------------------------------------------

def verdict(drill):
    return None  # TODO D


# --------------------------------------------------------------------------
# Wired: record assembly and output.
# --------------------------------------------------------------------------

def build_record(samples, drill):
    gap = write_gap(samples, drill)
    errors = error_classes(samples)
    stale = stale_read_window(samples, drill)
    writes = sum(1 for s in samples if s.get("op") == "write")
    reads = sum(1 for s in samples if s.get("op") == "read")
    record = {
        "group": drill["drill"]["group"],
        "namespace": drill["drill"].get("namespace"),
        "mode": drill["drill"]["mode"],
        "oldSite": demoted_site(drill),
        "newSite": promoted_site(drill),
        "promotedAt": iso(promotion_instant(drill)),
        "trigger": drill["drill"].get("trigger") or drill["drill"].get("injection"),
        "samples": {"total": len(samples), "writes": writes, "reads": reads},
        "lastWriteOldSite": iso(gap["lastWriteOldSite"]) if gap else None,
        "firstWriteNewSite": iso(gap["firstWriteNewSite"]) if gap else None,
        "writeGapSeconds": gap["gapSeconds"] if gap else None,
        "gapClosed": gap["closed"] if gap else None,
        "staleReads": stale["count"] if stale else None,
        "staleReadSeconds": stale["seconds"] if stale else None,
        "staleReadFirst": iso(stale["first"]) if stale else None,
        "staleReadLast": iso(stale["last"]) if stale else None,
        "errors": errors,
        "verdict": verdict(drill),
        "_complete": all(x is not None for x in (gap, errors, stale)) and verdict(drill) is not None,
    }
    return record


def _cell(value):
    if value is None:
        return "not computed"
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


def format_record(record):
    lines = []
    lines.append("DRILL RECORD — %s / %s / %s -> %s" % (
        record["group"], record["mode"], record["oldSite"], record["newSite"]))
    rows = [
        ("namespace", record["namespace"]),
        ("trigger", record["trigger"]),
        ("promotedAt", record["promotedAt"]),
        ("lastWriteOldSite", record["lastWriteOldSite"]),
        ("firstWriteNewSite", record["firstWriteNewSite"]),
        ("writeGapSeconds", "UNCLOSED" if record["gapClosed"] is False else record["writeGapSeconds"]),
        ("gapClosed", record["gapClosed"]),
        ("staleReads", record["staleReads"]),
        ("staleReadSeconds", record["staleReadSeconds"]),
    ]
    errors = record["errors"]
    if errors is None:
        rows.append(("errors", None))
    else:
        rows.append(("errors", "readOnlyRefusal=%d connection=%d other=%d" % (
            errors.get("readOnlyRefusal", 0), errors.get("connection", 0), errors.get("other", 0))))
    rows.append(("verdict", record["verdict"]))
    rows.append(("samples", "%d (writes %d, reads %d)" % (
        record["samples"]["total"], record["samples"]["writes"], record["samples"]["reads"])))
    if "baselineGapSeconds" in record:
        rows.append(("baselineGapSeconds", record["baselineGapSeconds"]))
        rows.append(("gapDeltaSeconds", record["gapDeltaSeconds"]))
    for key, value in rows:
        lines.append("  %-20s %s" % (key, _cell(value)))
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description="turn a failover drill capture into a drill record")
    parser.add_argument("--probe", required=True, help="JSONL probe log from the writer")
    parser.add_argument("--drill", required=True, help="JSON drill capture (trigger + group status)")
    parser.add_argument("--baseline", help="probe log from an earlier run to compare against")
    parser.add_argument("--baseline-drill", help="drill capture belonging to --baseline")
    parser.add_argument("--json", action="store_true", help="emit the record as JSON")
    args = parser.parse_args(argv)

    record = build_record(load_probe(args.probe), load_drill(args.drill))

    if args.baseline or args.baseline_drill:
        if not (args.baseline and args.baseline_drill):
            raise SystemExit("--baseline and --baseline-drill must be given together")
        base_gap = write_gap(load_probe(args.baseline), load_drill(args.baseline_drill))
        base_seconds = base_gap["gapSeconds"] if base_gap else None
        record["baselineGapSeconds"] = base_seconds
        if base_seconds is None or record["writeGapSeconds"] is None:
            record["gapDeltaSeconds"] = None
        else:
            record["gapDeltaSeconds"] = round(record["writeGapSeconds"] - base_seconds, 3)

    if args.json:
        print(json.dumps({k: v for k, v in record.items() if k != "_complete"}, indent=2))
    else:
        print(format_record(record))

    if not record["_complete"]:
        return 1
    if record["gapClosed"] is False:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
