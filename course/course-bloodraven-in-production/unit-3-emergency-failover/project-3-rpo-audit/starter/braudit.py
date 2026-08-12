#!/usr/bin/env python3
"""braudit — turn a captured MysqlFailoverGroup into a post-failover incident record.

Usage:
    python3 braudit.py <capture.json> [--now 2026-04-30T21:00:00Z]

The capture is whatever `kubectl get mysqlfailovergroup orders -o json` printed
after the promotion. Everything peripheral is already wired: argument parsing,
capture loading, GTID rendering, report rendering, exit-code plumbing.

Five gaps are marked TODO A .. TODO E. Fill them in the order the brief lists.
The file runs as given and prints a report that is confidently wrong.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

# --- Wired: the two durable locations of the failover record -----------------
# The operator writes the same fact twice, deliberately: once to the status
# subresource, once to these annotations. Second precision, RFC 3339.
LAST_FAILOVER_ANNOTATION = "bloodraven.shipstream.io/last-failover"
LAST_FAILOVER_TARGET_ANNOTATION = "bloodraven.shipstream.io/last-failover-target"

# The operator refuses a durable record stamped further than this ahead of its
# own clock, rather than trusting a future timestamp it cannot have written.
CLOCK_SKEW_GRACE = timedelta(minutes=5)


# --- TODO A ------------------------------------------------------------------
def parse_gtid_set(text):
    """Parse a MySQL GTID set into {uuid: [(start, end), ...]}.

    Accepts: 'uuid:1-19', 'uuid:1-19:25-30' (several intervals for one UUID),
    'uuid:7' (a single transaction), several UUIDs separated by commas, and
    newlines anywhere (a captured set is often folded across lines).

    Returns {} for an empty or whitespace-only string.

    Raise ValueError for anything else — including a MySQL 9.x tagged set such
    as 'uuid:Domain_1:1-3'. A tag is part of the UUID's identity, so ignoring it
    would understate the count, and an audit that understates is worse than one
    that refuses.
    """
    return {}  # TODO A


# --- TODO B ------------------------------------------------------------------
def transaction_count(gtid_set):
    """Number of transactions in a parsed GTID set."""
    return 0  # TODO B


# --- TODO C ------------------------------------------------------------------
def gtid_subtract(minuend, subtrahend):
    """GTID_SUBTRACT: the transactions in `minuend` that are not in `subtrahend`.

    Both arguments are parsed sets. Returns a parsed set. Subtracting an
    interval from the middle of another splits it in two: {u: [(1, 32)]} minus
    {u: [(1, 19), (25, 30)]} is {u: [(20, 24), (31, 32)]}.
    """
    return {}  # TODO C


# --- Wired: containment, expressed through subtraction ------------------------
def gtid_contains(superset, subset):
    """True when every transaction in `subset` is also in `superset`.

    This is GTID_SUBSET(subset, superset) with the arguments the way round the
    operator asks the question: does the new primary contain the old one's set?
    """
    return not gtid_subtract(subset, superset)


# --- TODO D ------------------------------------------------------------------
def select_failover_record(obj, now):
    """Pick the authoritative failover record, the way the operator does on restart.

    Read both durable copies — the status subresource and the annotation pair.
    Discard a copy whose timestamp is more than CLOCK_SKEW_GRACE ahead of `now`,
    and discard a copy whose target is not the name of a site in spec.sites.
    Of what survives, the later timestamp wins; a tie goes to status, because
    equal instants describe the same promotion.

    Returns {"at": <datetime or None>, "target": <str>, "source": <str>} where
    source is "status", "annotations", or "none".
    """
    status = obj.get("status") or {}
    # The naive read: trust status and stop. It is right until the status write
    # is the one that failed. Replace it.
    at = parse_time(status.get("lastFailover"))
    return {"at": at, "target": status.get("lastFailoverTarget", ""), "source": "status" if at else "none"}  # TODO D


# --- TODO E ------------------------------------------------------------------
def audit(obj, now):
    """Build the incident record. See the brief for the verdict rules in order."""
    status = obj.get("status") or {}
    meta = obj.get("metadata") or {}
    record = select_failover_record(obj, now)

    return {
        "group": "%s/%s" % (meta.get("namespace", ""), meta.get("name", "")),
        "failoverAt": fmt_time(record["at"]),
        "recordSource": record["source"],
        "from": "-",
        "to": record["target"] or "-",
        "promotionGtidExecuted": "-",  # TODO E: render status.promotionGtidExecuted
        "sites": [],                   # TODO E: one entry per non-active site
        "lost": 0,                     # TODO E: measured total, or None
        "verdict": "RPO 0 — no transaction was lost",  # TODO E
        "exitCode": 0,                 # TODO E: 0 / 1 / 2
    }


# --- Wired: rendering and plumbing -------------------------------------------
def render_gtid_set(gtid_set):
    """Render a parsed set back to MySQL's notation. '-' when the set is empty."""
    if not gtid_set:
        return "-"
    parts = []
    for uuid in sorted(gtid_set):
        intervals = sorted(tuple(iv) for iv in gtid_set[uuid])
        if not intervals:
            continue
        rendered = [str(s) if s == e else "%d-%d" % (s, e) for s, e in intervals]
        parts.append(uuid + ":" + ":".join(rendered))
    return ",".join(parts) if parts else "-"


def parse_time(value):
    """Parse an RFC 3339 timestamp into an aware datetime, or None."""
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def fmt_time(value):
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def render(record):
    lines = [
        "group: %s" % record["group"],
        "failoverAt: %s" % record["failoverAt"],
        "recordSource: %s" % record["recordSource"],
        "from: %s" % record["from"],
        "to: %s" % record["to"],
        "promotionGtidExecuted: %s" % record["promotionGtidExecuted"],
    ]
    for site in record["sites"]:
        lines.append("site: %s divergent=%s lost=%s"
                     % (site["name"], site["divergent"],
                        "-" if site["lost"] is None else site["lost"]))
    lines.append("lost: %s" % ("-" if record["lost"] is None else record["lost"]))
    lines.append("verdict: %s" % record["verdict"])
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Post-failover audit for a MysqlFailoverGroup capture.")
    ap.add_argument("capture", help="path to `kubectl get mysqlfailovergroup <name> -o json` output")
    ap.add_argument("--now", default=None,
                    help="RFC 3339 instant to audit against (default: now, UTC)")
    args = ap.parse_args(argv)

    with open(args.capture, "r", encoding="utf-8") as fh:
        obj = json.load(fh)

    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)

    record = audit(obj, now)
    print(render(record))
    return record["exitCode"]


if __name__ == "__main__":
    sys.exit(main())
