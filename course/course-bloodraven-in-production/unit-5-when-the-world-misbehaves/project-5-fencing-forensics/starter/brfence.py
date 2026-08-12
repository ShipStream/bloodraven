#!/usr/bin/env python3
"""brfence — fencing forensics for one MysqlFailoverGroup log bundle.

Reads a directory of JSON-lines logs collected from a single injected fault on
`orders` and reports every fence, what caused it, and whether the record's own
evidence supports it.

    python3 brfence.py tests/fixtures/partition-a

Bundle layout:

    operator.jsonl          the operator's operational (slog) stream
    sidecar-<site>.jsonl    one file per site's bloodraven-sidecar container

Everything except the four TODOs is already wired: bundle loading, timestamp
and duration parsing, ordering, the report format, and the exit code.

Do not change the report format. The tests read it.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Fields that appear on nearly every record and carry no forensic weight.
# The report prints everything else as evidence.
BORING_FIELDS = ("time", "level", "msg", "fg", "pod")


# --------------------------------------------------------------------------
# TODO A — the fence vocabulary.
#
# Return the cause id for a record that is a fence *decision*, or None for
# everything else. The six cause ids and the exact `msg` strings that map to
# them are in the brief. Key on the whole `msg` string: the `SELF-FENCING:`
# prefix is stable but it is not a synonym for "a fence happened".
#
#   rec     the decoded JSON record
#   stream  "operator" or "sidecar" (which file it came from)
# --------------------------------------------------------------------------
def classify(rec: dict, stream: str) -> str | None:
    return None  # TODO A


# --------------------------------------------------------------------------
# TODO B — which site got fenced.
#
# Not every fence record carries a `site` key, and the operator's split-brain
# line calls it something else. Fall back to `file_site` (the site name taken
# from the sidecar filename) when the record does not name a site itself.
# Return a site name, never None.
#
#   file_site  "iad" for sidecar-iad.jsonl, None for operator.jsonl
# --------------------------------------------------------------------------
def fenced_site(rec: dict, cause: str, file_site: str | None) -> str:
    return "?"  # TODO B


# --------------------------------------------------------------------------
# TODO C — was the fence supported by its own evidence?
#
# Return "correct" or "premature". The verdict table is in the brief. The
# interesting one is rule-2: it may only fire when the operator *and* every
# peer have been silent for the whole `leaseTimeout`, so compare the record's
# `bloodravenLastOk` and `latestPeerOk` against its `time`.
#
# parse_time() and parse_duration() below are already written for you.
# --------------------------------------------------------------------------
def judge(rec: dict, cause: str, site: str) -> str:
    return "correct"  # TODO C


# --------------------------------------------------------------------------
# TODO D — writable sites that nobody fenced.
#
# The operator emits `msg="ALERT"` with a `message` field. A split-brain alert
# reads exactly:  SPLIT BRAIN: 2 sites are writable (iad, pdx)
#
# Take the site names from inside the parentheses of every such alert, then
# drop:
#   * any site that has a fence event anywhere in this bundle, and
#   * the site named by the most recent `failover complete` `promotedSite` at
#     or before the alert — that site holds primary authority; fencing it
#     would be the bug.
#
# Return a list of (site, alert_message) pairs, each site at most once, in the
# order the sites first appear.
#
#   records  list of (rec, stream, file_site), already sorted by time
#   fences   list of Fence, already built from TODOs A-C
# --------------------------------------------------------------------------
def unfenced_writable_sites(records: list, fences: list) -> list:
    return []  # TODO D


# ==========================================================================
# Everything below is wired. You should not need to change it.
# ==========================================================================


@dataclass
class Fence:
    when: datetime
    raw_time: str
    site: str
    cause: str
    verdict: str
    rec: dict = field(repr=False)


def parse_time(value: str) -> datetime:
    """RFC3339Nano as the binaries emit it, e.g. 2026-08-12T09:14:08.113Z."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, _, tail = text.partition(".")
        digits = "".join(c for c in tail if c.isdigit())
        offset = tail[len(digits):]
        text = f"{head}.{digits[:6]:<06}{offset}"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def parse_duration(value: str) -> timedelta:
    """slog's default time.Duration rendering: "20s", "500ms", "5m"."""
    text = str(value).strip()
    for suffix, factor in (("ms", 0.001), ("s", 1.0), ("m", 60.0), ("h", 3600.0)):
        if text.endswith(suffix):
            return timedelta(seconds=float(text[: -len(suffix)]) * factor)
    return timedelta(seconds=float(text))


def load_bundle(bundle: Path) -> list:
    """Read every *.jsonl in the bundle, sorted by event time.

    Returns a list of (record, stream, file_site) tuples. `stream` is
    "operator" or "sidecar"; `file_site` is the site name taken from a
    sidecar-<site>.jsonl filename, or None for the operator stream.
    """
    rows = []
    for path in sorted(bundle.glob("*.jsonl")):
        name = path.stem
        if name == "operator":
            stream, file_site = "operator", None
        elif name.startswith("sidecar-"):
            stream, file_site = "sidecar", name[len("sidecar-"):]
        else:
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"{path.name}:{lineno + 1}: skipping unparseable line: {exc}",
                      file=sys.stderr)
                continue
            if "time" not in rec or "msg" not in rec:
                continue  # controller-runtime (zap) noise, not the operational stream
            rows.append((rec, stream, file_site, path.name, lineno))
    rows.sort(key=lambda r: (parse_time(r[0]["time"]), r[3], r[4]))
    return [(rec, stream, file_site) for rec, stream, file_site, _, _ in rows]


def group_name(records: list) -> str:
    for rec, _, _ in records:
        fg = rec.get("fg")
        if fg:
            return fg.split("/")[-1]
    return "orders"


def evidence(rec: dict) -> str:
    parts = []
    for key, value in rec.items():
        if key in BORING_FIELDS:
            continue
        parts.append(f"{key}={value if isinstance(value, str) else json.dumps(value)}")
    return " ".join(parts)


def plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def report(group: str, bundle: Path, records: list, fences: list, unfenced: list) -> int:
    print(f"FENCE TIMELINE — {group} (bundle: {bundle.name})")
    print(f"  {plural(len(records), 'record')} scanned, {plural(len(fences), 'fence event')}")
    print()
    if not fences:
        print("  (no fence events in this bundle)")
    for f in fences:
        print(f"  {f.raw_time:<26}{f.site:<9}{f.cause:<15}{f.verdict:<11}{evidence(f.rec)}")
    print()
    print("UNFENCED WRITABLE SITES")
    if not unfenced:
        print("  (none)")
    for site, alert in unfenced:
        print(f"  {site}  — writable per ALERT \"{alert}\", no fence event in this bundle")
    print()
    premature = [f for f in fences if f.verdict != "correct"]
    print(f"VERDICT: {plural(len(fences), 'fence')}, {len(premature)} premature, "
          f"{plural(len(unfenced), 'unfenced writable site')}")
    return 1 if (premature or unfenced) else 0


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description="Fencing forensics for a Bloodraven log bundle.")
    ap.add_argument("bundle", type=Path, help="directory holding operator.jsonl and sidecar-*.jsonl")
    args = ap.parse_args(argv)

    if not args.bundle.is_dir():
        print(f"brfence: {args.bundle} is not a directory", file=sys.stderr)
        return 2

    records = load_bundle(args.bundle)
    if not records:
        print(f"brfence: no operational log records found in {args.bundle}", file=sys.stderr)
        return 2

    fences = []
    for rec, stream, file_site in records:
        cause = classify(rec, stream)
        if cause is None:
            continue
        site = fenced_site(rec, cause, file_site)
        fences.append(Fence(
            when=parse_time(rec["time"]),
            raw_time=rec["time"],
            site=site,
            cause=cause,
            verdict=judge(rec, cause, site),
            rec=rec,
        ))

    unfenced = unfenced_writable_sites(records, fences)
    return report(group_name(records), args.bundle, records, fences, unfenced)


if __name__ == "__main__":
    sys.exit(main())
