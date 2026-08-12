#!/usr/bin/env python3
"""go-live pack checker for the failover group `orders`.

Reads three artefacts out of ``pack/`` and reports whether the pack is
fit to hand to an on-call rotation:

  pack/alerts.yml    Prometheus alerting rules
  pack/runbooks.yml  alert -> runbook anchor -> first command
  pack/drill.json    the DR drill record

Then it replays every fixture under ``tests/fixtures/`` through your
rules and compares what fired against what should have fired.

Run it:

    python3 starter/golive.py

Everything below is wired except the two functions marked TODO F and
TODO G. Argument parsing, YAML/JSON loading, evaluation and report
formatting are done.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import promeval  # noqa: E402

# ---------------------------------------------------------------------------
# Grounded reference data. Do not add to these lists from memory.
# ---------------------------------------------------------------------------

# Every metric name the shipped operator (v0.9.1) actually exports and
# that this course teaches you to alert on. A rule referencing anything
# else is a rule that can never fire.
SHIPPED_METRICS = {
    "bloodraven_site_state",
    "bloodraven_replication_lag_seconds",
    "bloodraven_replication_running",
    "bloodraven_failovers_total",
    "bloodraven_divergent_transactions",
    "bloodraven_split_brain_auto_resolve_total",
    "bloodraven_primary_reassert_total",
    "bloodraven_poll_latency_seconds",
    "bloodraven_archiver_backlog_files",
    "bloodraven_backup_last_success_timestamp_seconds",
    "bloodraven_keyring_phase",
    "bloodraven_dns_flips_total",
    "bloodraven_state_transitions_total",
}

# `up` is Prometheus' own scrape-health series, not a Bloodraven metric.
# It is the only non-Bloodraven name this pack is allowed to use.
ALLOWED_FOREIGN_METRICS = {"up"}

# The minimum alert set for `orders`.
REQUIRED_ALERTS = [
    "BloodravenOperatorDown",
    "BloodravenNoWritableSite",
    "BloodravenSplitBrainResolved",
    "BloodravenReplicationLagging",
    "BloodravenReplicationDown",
    "BloodravenDivergentTransactions",
    "BloodravenBackupStale",
    "BloodravenPITRArchiveLagging",
    "BloodravenKeyringNotSealed",
    "BloodravenFailoverOccurred",
]

# Alerts that must not page. `bloodraven_failovers_total` tells you the
# operator finished, not that traffic recovered.
INFO_ONLY_ALERTS = {"BloodravenFailoverOccurred"}

PAGING_SEVERITIES = {"critical", "warning"}

# Vocabulary for the drill record. Anything outside it is rejected, so
# two people writing a drill record produce comparable claims.
PROVED_VOCABULARY = {
    "artifact-loads",
    "sanity-check-passed",
    "restore-in-place-completed",
    "reader-endpoint-returned",
    "dns-record-updated",
}
ASSUMED_VOCABULARY = {
    "logical-equivalence-with-live-primary",
    "application-traffic-cutover",
    "cross-cluster-split-brain-detection",
    "dns-propagation-time",
}
# A verification proves the artifact loads and your scalar assertion
# held. These two are the things it never proves, so they belong in
# `assumed` on every honest drill record.
ASSUMED_REQUIRED = {
    "logical-equivalence-with-live-primary",
    "application-traffic-cutover",
}

BACKUP_SOURCE_REASONS = {"override", "replica-preferred", "primary-fallback"}

# `orders` runs three sites. `reader` carries role: read-only, so it can
# neither be promoted nor source a backup.
READ_ONLY_SITES = {"reader"}

ANCHOR_RE = re.compile(r"^runbook\.md#[a-z0-9-]+$")


# ---------------------------------------------------------------------------
# Loading (wired)
# ---------------------------------------------------------------------------


def load_rules(path):
    """Flatten a Prometheus rules file into a list of rule dicts."""
    doc = yaml.safe_load(Path(path).read_text()) or {}
    rules = []
    for group in doc.get("groups", []) or []:
        for rule in group.get("rules", []) or []:
            if "alert" not in rule:
                continue
            rules.append(
                {
                    "alert": rule["alert"],
                    "expr": rule.get("expr", ""),
                    "for": rule.get("for", ""),
                    "labels": rule.get("labels") or {},
                    "annotations": rule.get("annotations") or {},
                    "group": group.get("name", ""),
                }
            )
    return rules


def load_runbooks(path):
    doc = yaml.safe_load(Path(path).read_text()) or {}
    return doc.get("runbooks") or {}


def load_drill(path):
    return json.loads(Path(path).read_text())


# ---------------------------------------------------------------------------
# TODO F and TODO G — the two checks you implement
# ---------------------------------------------------------------------------


def check_metric_allowlist(rules):
    """Report every rule that references a metric the operator does not export.

    Return a sorted list of ``(alert_name, metric_name)`` tuples, one per
    offending metric, and an empty list when every rule is clean.

    Use ``promeval.metric_names_in(expr)`` to pull the metric names out of
    an expression. A name is acceptable when it is in ``SHIPPED_METRICS``
    or in ``ALLOWED_FOREIGN_METRICS``; anything else is a finding.

    TODO F
    """
    return None


def check_runbook_coverage(rules, runbooks):
    """Report every alert whose runbook entry is missing or unusable.

    Return a sorted list of ``(alert_name, problem)`` tuples and an empty
    list when the map is complete. An entry is usable when all of these
    hold:

      * the alert has an entry in ``runbooks`` at all
      * ``anchor`` is a non-empty string matching ``ANCHOR_RE``
      * ``firstCommand`` is a non-empty string starting with ``kubectl``

    The ``problem`` string is yours to word; nothing grades it. What is
    graded is which alerts you flag.

    TODO G
    """
    return None


# ---------------------------------------------------------------------------
# Checks that are already wired
# ---------------------------------------------------------------------------


def check_coverage(rules):
    """Required alerts present, paging rules debounced, info alerts not paging."""
    problems = []
    by_name = {r["alert"]: r for r in rules}
    for name in REQUIRED_ALERTS:
        if name not in by_name:
            problems.append(f"missing required alert: {name}")
    for rule in sorted(rules, key=lambda r: r["alert"]):
        name = rule["alert"]
        severity = str(rule["labels"].get("severity", "")).lower()
        if name in INFO_ONLY_ALERTS:
            if severity != "info":
                problems.append(
                    f"{name} must carry severity: info — it reports that the operator "
                    f"finished, not that traffic recovered (found {severity or 'none'})"
                )
            continue
        if severity not in PAGING_SEVERITIES:
            problems.append(
                f"{name} has severity {severity or 'none'}; expected one of "
                + ", ".join(sorted(PAGING_SEVERITIES))
            )
        if not str(rule["for"]).strip() or str(rule["for"]).strip() in {"0", "0s", "0m"}:
            problems.append(f"{name} has no for: duration — it pages on one bad scrape")
    return problems


def check_drill(drill):
    """The drill record must separate what was proved from what was assumed."""
    problems = []

    proved = drill.get("proved") or []
    assumed = drill.get("assumed") or []
    if not proved:
        problems.append("proved[] is empty — a drill that proved nothing is not a drill")
    if not assumed:
        problems.append("assumed[] is empty — every drill leaves something unproved")

    for item in proved:
        if item not in PROVED_VOCABULARY:
            problems.append(f"proved[] entry {item!r} is outside the vocabulary")
    for item in assumed:
        if item not in ASSUMED_VOCABULARY:
            problems.append(f"assumed[] entry {item!r} is outside the vocabulary")

    for item in sorted(ASSUMED_REQUIRED):
        if item not in assumed:
            problems.append(
                f"assumed[] must contain {item!r} — a Succeeded verification never proves it"
            )
        if item in proved:
            problems.append(f"proved[] claims {item!r}, which no verification can prove")

    reason = drill.get("backupSourceReason", "")
    if reason not in BACKUP_SOURCE_REASONS:
        problems.append(
            f"backupSourceReason {reason!r} is not one of "
            + ", ".join(sorted(BACKUP_SOURCE_REASONS))
        )
    site = drill.get("backupSourceSite", "")
    if site in READ_ONLY_SITES:
        problems.append(
            f"backupSourceSite {site!r} is a read-only site, which cannot be a backup source"
        )
    elif not site:
        problems.append("backupSourceSite is empty")

    confirm = str((drill.get("restore") or {}).get("confirm", ""))
    try:
        datetime.fromisoformat(confirm.replace("Z", "+00:00"))
    except ValueError:
        problems.append(
            f"restore.confirm {confirm!r} is not an RFC 3339 timestamp — the in-place "
            "restore token is rejected unless it parses and is strictly greater than "
            "status.restoreInPlace.confirmTokenUsed"
        )

    if not str(drill.get("earliestReachablePoint", "")).strip():
        problems.append("earliestReachablePoint is empty — say how far back you can reach")
    if not str(drill.get("applicationSideAlertOwner", "")).strip():
        problems.append(
            "applicationSideAlertOwner is empty — no shipped alert fires for "
            "'the application is still broken after a successful failover'"
        )
    if not str(drill.get("handoverNote", "")).strip():
        problems.append(
            "handoverNote is empty — say in one line what this pack will not tell "
            "the rotation"
        )
    return problems


def alert_key(entry):
    """Identify a firing series as ``Alert`` or ``Alert@site``.

    Fixtures list what should fire using these keys, so an alert that
    fires for the wrong site is a mismatch rather than a pass.
    """
    labels = entry.get("labels") or {}
    scope = (
        labels.get("site")
        or labels.get("target_site")
        or labels.get("prefer_site")
        or ""
    )
    return f"{entry['alert']}@{scope}" if scope else entry["alert"]


def firing_keys(rules, fixture):
    """The sorted set of ``Alert``/``Alert@site`` keys a fixture produces."""
    return sorted({alert_key(entry) for entry in evaluate(rules, fixture)})


def evaluate(rules, fixture):
    """Return the firing series for every rule, as a list of dicts."""
    firing = []
    for rule in rules:
        try:
            result = promeval.eval_expr(rule["expr"], fixture)
        except promeval.ExprError as exc:
            firing.append(
                {"alert": rule["alert"], "labels": {}, "value": 0.0, "error": str(exc)}
            )
            continue
        for series in result:
            firing.append(
                {
                    "alert": rule["alert"],
                    "labels": series["labels"],
                    "value": series["value"],
                }
            )
    return firing


# ---------------------------------------------------------------------------
# Report (wired)
# ---------------------------------------------------------------------------


def _fmt_labels(labels):
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return "{" + inner + "}"


def report(pack_dir, fixtures_dir, out=sys.stdout):
    rules = load_rules(pack_dir / "alerts.yml")
    runbooks = load_runbooks(pack_dir / "runbooks.yml")
    drill = load_drill(pack_dir / "drill.json")

    problems = 0
    print("go-live pack for `orders`", file=out)
    print("=" * 46, file=out)
    print(f"{len(rules)} alert rules, {len(runbooks)} runbook entries", file=out)
    print("", file=out)

    metric_findings = check_metric_allowlist(rules)
    if metric_findings is None:
        print("[metrics]  not implemented (TODO F)", file=out)
        problems += 1
    elif metric_findings:
        print(
            f"[metrics]  {len(metric_findings)} reference(s) to a metric the operator "
            "does not export:",
            file=out,
        )
        for alert, metric in metric_findings:
            print(f"             {alert} -> {metric}", file=out)
        problems += len(metric_findings)
    else:
        print("[metrics]  clean", file=out)

    runbook_findings = check_runbook_coverage(rules, runbooks)
    if runbook_findings is None:
        print("[runbooks] not implemented (TODO G)", file=out)
        problems += 1
    elif runbook_findings:
        print(f"[runbooks] {len(runbook_findings)} alert(s) with no usable runbook entry:", file=out)
        for alert, problem in runbook_findings:
            print(f"             {alert}: {problem}", file=out)
        problems += len(runbook_findings)
    else:
        print("[runbooks] clean", file=out)

    coverage = check_coverage(rules)
    if coverage:
        for problem in coverage:
            print(f"[coverage] {problem}", file=out)
        problems += len(coverage)
    else:
        print("[coverage] clean", file=out)

    drill_problems = check_drill(drill)
    if drill_problems:
        for problem in drill_problems:
            print(f"[drill]    {problem}", file=out)
        problems += len(drill_problems)
    else:
        print("[drill]    clean", file=out)

    owner = str(drill.get("applicationSideAlertOwner", "")).strip()
    print(f"[owner]    application-side alerting owned by: {owner or '(nobody)'}", file=out)

    print("", file=out)
    print("fixture replay", file=out)
    print("-" * 46, file=out)
    for path in sorted(fixtures_dir.glob("*.json")):
        fixture = promeval.load_fixture(path)
        firing = evaluate(rules, fixture)
        keys = firing_keys(rules, fixture)
        expected = sorted(set(fixture["expectedAlerts"]))
        verdict = "OK" if keys == expected else "MISMATCH"
        if verdict == "MISMATCH":
            problems += 1
        print(
            f"  fixture {fixture['id']}: {len(keys)} firing, "
            f"expected {len(expected)}  {verdict}",
            file=out,
        )
        for entry in sorted(firing, key=lambda f: (f["alert"], str(f["labels"]))):
            marker = "FIRING  " if alert_key(entry) in expected else "SPURIOUS"
            print(
                f"    {marker} {alert_key(entry)}{_fmt_labels(entry['labels'])} "
                f"= {entry['value']:g}",
                file=out,
            )
        for missing in [k for k in expected if k not in keys]:
            print(f"    MISSING  {missing}", file=out)

    print("", file=out)
    if problems:
        print(f"RESULT: NOT READY ({problems} problem(s))", file=out)
    else:
        print("RESULT: READY", file=out)
    return 1 if problems else 0


def main(argv=None):
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="check the go-live pack for `orders`")
    parser.add_argument("--pack", type=Path, default=here / "pack")
    parser.add_argument(
        "--fixtures", type=Path, default=here.parent / "tests" / "fixtures"
    )
    args = parser.parse_args(argv)
    return report(args.pack, args.fixtures)


if __name__ == "__main__":
    raise SystemExit(main())
