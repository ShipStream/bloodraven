#!/usr/bin/env python3
"""brdecide — predict the cross-site decision the Bloodraven operator would take.

Feed it a MysqlFailoverGroup object (``kubectl get mysqlfailovergroup playground -o json``)
and a clock. It prints the action, the alert, the ``Reason`` string that reaches
``status.conditions``, and whether ``spec.failoverCooldown`` will let the promotion run.

    python starter/brdecide.py --status tests/fixtures/playground-healthy.json
    python starter/brdecide.py --status tests/fixtures/playground-iad-down-cooldown.json \
        --now 2026-08-12T12:04:16Z --json

Everything peripheral is already wired: argument parsing, loading the object, joining
spec roles to status states, Go duration parsing, candidate ranking, and both output
formats. Four gaps are yours — TODO A, TODO B, TODO C, TODO D.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# --- Grounded constants -----------------------------------------------------
# Site roles: api/v1alpha1/types.go — enum primary-candidate;dr-only;read-only.
ROLE_PRIMARY_CANDIDATE = "primary-candidate"
ROLE_DR_ONLY = "dr-only"
ROLE_READ_ONLY = "read-only"

# The four per-site states: internal/state/machine.go.
STATE_UNKNOWN = "unknown"
STATE_WRITABLE = "writable"
STATE_READ_ONLY = "read-only"
STATE_UNREACHABLE = "unreachable"

# spec.failoverCooldown default "5m"; the operator falls back to the same value
# when the pointer is nil.
DEFAULT_FAILOVER_COOLDOWN = 300.0

# FailoverClockSkewGrace = 5 * time.Minute.
FAILOVER_CLOCK_SKEW_GRACE = 300.0

# The two durable annotation keys, written as a pair by JSON merge patch.
LAST_FAILOVER_ANNOTATION = "bloodraven.shipstream.io/last-failover"
LAST_FAILOVER_TARGET_ANNOTATION = "bloodraven.shipstream.io/last-failover-target"


@dataclass(frozen=True)
class Observation:
    """One site at one poll cycle: its name, its configured role, its state."""

    name: str
    role: str
    state: str


def new_action() -> dict:
    """The empty CrossSiteAction. Fill these keys in evaluate_cross_site()."""
    return {
        "coreCount": 0,
        "writable": [],
        "readOnly": [],
        "unreachable": [],
        "fenceSites": [],
        "promotionCandidates": [],
        "splitBrain": False,
        "alert": None,
        "reason": "",
    }


# ===========================================================================
# TODO A — the pre-pass: coreCount, fence routing, the three tallies
# ===========================================================================
def tally(observations: list[Observation]) -> dict:
    """Walk the observations once and return the partial action.

    Set ``coreCount``, ``fenceSites``, ``writable``, ``readOnly`` and
    ``unreachable`` (name lists, in observation order). Leave the rest alone.

    Three rules, in this order, for every observation:

      1. ``coreCount`` increments for every site whose role is **not**
         ``read-only``. A ``dr-only`` site counts. An ``unknown`` state still
         counts — the site is part of the topology.
      2. A site that is ``writable`` while its role is **not**
         ``primary-candidate`` goes to ``fenceSites`` and is skipped: it never
         reaches a tally.
      3. A site whose role is ``read-only`` is skipped entirely.

    Everything that survives lands in the tally for its state. ``unknown``
    sites land in no tally at all.
    """
    action = new_action()
    # TODO A: implement the three rules above.
    return action


# ===========================================================================
# TODO B — the rows, in evaluation order
# ===========================================================================
def evaluate_cross_site(observations: list[Observation], site_priorities: list[str]) -> dict:
    """Return the CrossSiteAction for one poll cycle.

    This function is **pure**: no clock, no failover history, no cooldown. It
    mirrors EvalCrossSite in internal/state/matrix.go, which never considers
    history or policy beyond the supplied priorities.

    Evaluate the rows in this order and return at the first one that fires:

      1. fence-first — ``fenceSites`` non-empty:
         alert ``writable non-promotable site requires fencing (<sites>)``
         (comma-space joined), reason ``Degraded``.
      2. TotalLoss — ``len(unreachable) == coreCount``:
         alert ``TOTAL LOSS: all sites are unreachable``, reason ``TotalLoss``.
      3. SplitBrain — ``len(writable) > 1``: ``splitBrain`` True,
         alert ``SPLIT BRAIN: <n> sites are writable (<sites>)``,
         reason ``SplitBrain``.
      4. Failover — ``len(writable) == 0`` and ``len(unreachable) > 0`` and
         ``len(readOnly) > 0`` and ranking yields at least one candidate:
         set ``promotionCandidates``, reason ``Degraded``, **no alert**.
      5. NoPrimary — still no writable site: reason ``NoPrimary``, alert
         ``NO PRIMARY: both sites are read-only`` when exactly two read-only
         sites and zero unreachable, otherwise
         ``NO PRIMARY: no writable site available``.
      6. Degraded — exactly one writable and at least one unreachable:
         alert ``<unreachable sites> unreachable while <writable site> is primary``,
         reason ``Degraded``.
      7. Healthy — reason ``Healthy``, no alert.

    Use rank_promotion_candidates() for row 4; it is already written.
    """
    action = tally(observations)
    # TODO B: evaluate the rows above, in order, and return at the first hit.
    return action


# ===========================================================================
# TODO C — rehydrate the failover history from its two durable copies
# ===========================================================================
def rehydrate_last_failover(status_record, annotation_record, now):
    """Pick which durable copy of the failover history the operator believes.

    ``status_record`` and ``annotation_record`` are each ``(timestamp, target)``
    with ``timestamp`` a timezone-aware datetime or None.

    Return ``(timestamp, target, source)`` where ``source`` is ``"status"``,
    ``"annotation"`` or None.

      * Discard either copy stamped more than FAILOVER_CLOCK_SKEW_GRACE ahead of
        ``now``. A future-dated record would wedge promotion indefinitely.
      * Of what survives, install the **later** one.
      * Ties go to status: equal timestamps describe the same promotion.
      * Nothing left → ``(None, None, None)``.
    """
    # TODO C: implement the skew guard, the later-copy rule and the tie rule.
    return (None, None, None)


# ===========================================================================
# TODO D — the execution gate
# ===========================================================================
def apply_gate(action: dict, last_failover, now: datetime, cooldown: float) -> dict:
    """Decide what actually runs this poll, and report the cooldown timer.

    Return a dict with exactly these three keys:

      ``willRun``            list of strings, in order: one ``fence:<site>``
                             per entry in ``action["fenceSites"]``, then
                             ``promote`` when a promotion is selected and not
                             blocked.
      ``promotionBlockedBy`` ``"cooldown"`` or None.
      ``cooldownRemaining``  seconds left on the timer, as a float.

    The cooldown is enforced in exactly one place: immediately before the
    promotion call. So it can only ever block ``promote``. Fencing a writable
    non-promotable site is not gated by it — that runs every poll.

    Blocked when a promotion is selected, ``last_failover`` is set, and
    ``now - last_failover < cooldown``. Negative elapsed time (a record stamped
    in the future but inside the skew grace) counts as still active.

    ``cooldownRemaining`` is ``max(0.0, cooldown - elapsed)`` whenever
    ``last_failover`` is set — report the timer even when nothing is waiting on
    it — and ``0.0`` when it is not.
    """
    # TODO D: build willRun, set promotionBlockedBy, compute cooldownRemaining.
    return {"willRun": [], "promotionBlockedBy": None, "cooldownRemaining": 0.0}


# ===========================================================================
# Scaffolding below this line. You should not need to change any of it.
# ===========================================================================

_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|us|ns|h|m|s)")
_DURATION_UNITS = {
    "ns": 1e-9,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def parse_go_duration(text: str | None, default: float) -> float:
    """Parse a Go duration string such as "5m", "30s" or "1m30s" into seconds."""
    if not text:
        return default
    parts = _DURATION_RE.findall(text)
    if not parts:
        raise ValueError(f"cannot parse duration {text!r}")
    return sum(float(value) * _DURATION_UNITS[unit] for value, unit in parts)


def parse_rfc3339(text: str | None):
    """Parse an RFC3339 timestamp into a timezone-aware UTC datetime, or None."""
    if not text:
        return None
    cleaned = text.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_rfc3339(moment) -> str | None:
    if moment is None:
        return None
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_group(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        group = json.load(handle)
    if group.get("kind") != "MysqlFailoverGroup":
        raise ValueError(f"{path}: not a MysqlFailoverGroup object")
    return group


def observations_from(group: dict) -> list[Observation]:
    """Join spec.sites (role) to status.sites (state), in declared site order.

    A site the operator has not reported on yet is `unknown`, not missing.
    """
    states = {
        site.get("name"): site.get("state") or STATE_UNKNOWN
        for site in group.get("status", {}).get("sites", [])
    }
    observations = []
    for site in group.get("spec", {}).get("sites", []):
        name = site.get("name")
        if not name:
            raise ValueError("spec.sites[] entry with no name")
        observations.append(
            Observation(
                name=name,
                role=site.get("role") or ROLE_PRIMARY_CANDIDATE,
                state=states.get(name, STATE_UNKNOWN),
            )
        )
    if len(observations) < 2:
        raise ValueError("spec.sites has fewer than 2 entries")
    return observations


def read_history(group: dict):
    """Return the two durable copies as ((ts, target), (ts, target))."""
    status = group.get("status", {})
    annotations = group.get("metadata", {}).get("annotations", {}) or {}
    status_record = (
        parse_rfc3339(status.get("lastFailover")),
        status.get("lastFailoverTarget") or None,
    )
    annotation_record = (
        parse_rfc3339(annotations.get(LAST_FAILOVER_ANNOTATION)),
        annotations.get(LAST_FAILOVER_TARGET_ANNOTATION) or None,
    )
    return status_record, annotation_record


def rank_promotion_candidates(read_only: list[str], observations: list[Observation],
                              site_priorities: list[str]) -> list[str]:
    """Order the read-only primary-candidates: sitePriorities first, then declared order.

    Mirrors RankPromotionCandidates. Non-primary-candidate sites are dropped, so a
    `dr-only` replica is never returned. This list is the **tiebreaker** only:
    the operator ranks it by GTID freshness before promoting anything.
    """
    roles = {obs.name: obs.role for obs in observations}
    eligible = [name for name in read_only if roles.get(name) == ROLE_PRIMARY_CANDIDATE]
    out: list[str] = []
    for name in site_priorities or []:
        if name in eligible and name not in out:
            out.append(name)
    for name in eligible:
        if name not in out:
            out.append(name)
    return out


def decide(group: dict, now: datetime) -> dict:
    """Run the table, rehydrate the history, apply the gate. The whole tool."""
    observations = observations_from(group)
    spec = group.get("spec", {})
    site_priorities = (spec.get("splitBrainPolicy") or {}).get("sitePriorities") or []
    cooldown = parse_go_duration(spec.get("failoverCooldown"), DEFAULT_FAILOVER_COOLDOWN)

    action = evaluate_cross_site(observations, site_priorities)

    status_record, annotation_record = read_history(group)
    last_failover, last_target, source = rehydrate_last_failover(
        status_record, annotation_record, now
    )
    gate = apply_gate(action, last_failover, now, cooldown)

    meta = group.get("metadata", {})
    report = {
        "group": f"{meta.get('namespace', 'default')}/{meta.get('name', '?')}",
        "now": format_rfc3339(now),
        "sites": [{"name": o.name, "role": o.role, "state": o.state} for o in observations],
    }
    report.update(action)
    report.update(
        {
            "lastFailover": format_rfc3339(last_failover),
            "lastFailoverTarget": last_target,
            "lastFailoverSource": source,
            "cooldown": cooldown,
        }
    )
    report.update(gate)
    return report


def _human_seconds(seconds: float) -> str:
    seconds = float(seconds)
    if seconds >= 60:
        minutes, rest = divmod(seconds, 60)
        return f"{int(minutes)}m{rest:g}s"
    return f"{seconds:g}s"


def render_text(report: dict) -> str:
    lines = [f"brdecide — {report['group']} at {report['now']}", ""]
    sites = "  ".join(f"{s['name']}={s['state']}({s['role']})" for s in report["sites"])
    lines.append(f"  sites         {sites}")
    lines.append(f"  coreCount     {report['coreCount']}")
    lines.append(
        "  tallies       writable={} readOnly={} unreachable={}".format(
            report["writable"] or "[]", report["readOnly"] or "[]",
            report["unreachable"] or "[]",
        )
    )
    lines.append(f"  fenceSites    {report['fenceSites'] or '-'}")
    lines.append("")
    lines.append(f"  Reason        {report['reason'] or '(unset)'}")
    lines.append(f"  Alert         {report['alert'] or '(none)'}")
    lines.append(f"  SplitBrain    {'yes' if report['splitBrain'] else 'no'}")
    candidates = report["promotionCandidates"]
    if candidates:
        lines.append(
            "  Candidates    {}  (tiebreak order — GTID freshness picks the winner)".format(
                ", ".join(candidates)
            )
        )
    else:
        lines.append("  Candidates    -")
    lines.append("")
    if report["lastFailover"]:
        lines.append(
            "  lastFailover  {} → {}  (from {})".format(
                report["lastFailover"], report["lastFailoverTarget"] or "?",
                report["lastFailoverSource"],
            )
        )
        lines.append(
            "  cooldown      {} configured, {} remaining".format(
                _human_seconds(report["cooldown"]),
                _human_seconds(report["cooldownRemaining"]),
            )
        )
    else:
        lines.append("  lastFailover  (none recorded)")
        lines.append(f"  cooldown      {_human_seconds(report['cooldown'])} configured")
    if report["promotionBlockedBy"]:
        lines.append(f"  BLOCKED       promotion blocked by {report['promotionBlockedBy']}")
    lines.append(f"  willRun       {', '.join(report['willRun']) or '(nothing)'}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--status", required=True,
                        help="path to a MysqlFailoverGroup JSON object")
    parser.add_argument("--now", default=None,
                        help="RFC3339 clock reading (default: the real clock)")
    parser.add_argument("--json", action="store_true",
                        help="emit the decision as JSON instead of a report")
    args = parser.parse_args(argv)

    try:
        group = load_group(args.status)
        now = parse_rfc3339(args.now) or datetime.now(timezone.utc)
        report = decide(group, now)
    except (OSError, ValueError, KeyError) as err:
        print(f"brdecide: {err}", file=sys.stderr)
        return 2

    print(json.dumps(report, indent=2) if args.json else render_text(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
