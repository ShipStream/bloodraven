"""The four graded checks. Each returns "PASS" or "FAIL: <reason>".

Do not edit anything under tests/. The fixtures are the grading input.
"""

from __future__ import annotations

import copy

import harness

golive = harness.golive


def _verdict(errors):
    return "PASS" if not errors else "FAIL: " + "; ".join(errors)


# ---------------------------------------------------------------------------


def reader_soak_stays_silent():
    """Adversarial: the reader soak must be silent, and pdx must still page."""
    errors = []

    silent = harness.firing("reader-soak-3x")
    if silent:
        errors.append(
            "reader soaked past 3x maxLagSeconds fired "
            + ", ".join(silent)
            + " — scenario 42 shows the group stays Ready, no failover fires and only "
              "the reader endpoint sheds, so nothing here should page"
        )

    loud = harness.firing("candidate-lagging")
    if loud != ["BloodravenReplicationLagging@pdx"]:
        errors.append(
            "a genuinely lagging primary-candidate must still page: expected exactly "
            "['BloodravenReplicationLagging@pdx'], got " + str(loud)
        )

    lagging = [r for r in harness.rules() if r["alert"] == "BloodravenReplicationLagging"]
    if not lagging:
        errors.append("BloodravenReplicationLagging is missing from the rules file")
    elif "bloodraven_replication_lag_seconds" not in str(lagging[0]["expr"]):
        errors.append(
            "BloodravenReplicationLagging no longer reads "
            "bloodraven_replication_lag_seconds"
        )

    return _verdict(errors)


# ---------------------------------------------------------------------------


def real_loss_still_pages():
    """Canonical: four incident fixtures must produce exactly their alert sets."""
    expected = {
        "primary-lost": [
            "BloodravenNoWritableSite",
            "BloodravenReplicationDown@pdx",
        ],
        "post-failover-divergence": [
            "BloodravenBackupStale",
            "BloodravenDivergentTransactions@iad",
            "BloodravenFailoverOccurred@pdx",
            "BloodravenKeyringNotSealed@iad",
            "BloodravenPITRArchiveLagging@pdx",
        ],
        "operator-down": ["BloodravenOperatorDown"],
        "split-brain-resolved": [
            "BloodravenFailoverOccurred@iad",
            "BloodravenSplitBrainResolved@iad",
        ],
    }
    errors = []
    for name, want in expected.items():
        got = harness.firing(name)
        if got != want:
            missing = [k for k in want if k not in got]
            spurious = [k for k in got if k not in want]
            detail = []
            if missing:
                detail.append("missing " + ", ".join(missing))
            if spurious:
                detail.append("spurious " + ", ".join(spurious))
            errors.append(f"fixture {name}: " + "; ".join(detail))
    return _verdict(errors)


# ---------------------------------------------------------------------------


def only_shipped_metrics_and_full_runbook_map():
    """Structural: both checks are implemented, and the pack passes them."""
    errors = []
    rules = harness.rules()
    runbooks = harness.runbooks()

    present = {r["alert"] for r in rules}
    for name in golive.REQUIRED_ALERTS:
        if name not in present:
            errors.append(f"required alert {name} is missing from the rules file")

    findings = harness.normalise(golive.check_metric_allowlist(rules))
    if findings is None:
        errors.append("check_metric_allowlist still returns None (TODO F)")
    elif findings:
        errors.append(
            "check_metric_allowlist flags the finished rules file: " + str(findings)
        )

    if findings is not None:
        planted = copy.deepcopy(rules) + [
            {
                "alert": "BogusAlert",
                "expr": "bloodraven_backup_age_seconds > 86400",
                "for": "5m",
                "labels": {"severity": "warning"},
                "annotations": {},
                "group": "planted",
            }
        ]
        planted_findings = harness.normalise(golive.check_metric_allowlist(planted))
        if planted_findings != [("BogusAlert", "bloodraven_backup_age_seconds")]:
            errors.append(
                "check_metric_allowlist should return "
                "[('BogusAlert', 'bloodraven_backup_age_seconds')] for a rule reading a "
                "metric the operator does not export, got " + str(planted_findings)
            )

    coverage = harness.normalise(golive.check_runbook_coverage(rules, runbooks))
    if coverage is None:
        errors.append("check_runbook_coverage still returns None (TODO G)")
    elif coverage:
        errors.append("check_runbook_coverage flags the finished map: " + str(coverage))

    if coverage is not None:
        victim = "BloodravenNoWritableSite"
        stripped = {k: v for k, v in runbooks.items() if k != victim}
        missing_findings = harness.normalise(
            golive.check_runbook_coverage(rules, stripped)
        )
        if [f[0] for f in missing_findings or []] != [victim]:
            errors.append(
                f"removing the {victim} runbook entry should be flagged exactly once, "
                "got " + str(missing_findings)
            )

        blanked = copy.deepcopy(runbooks)
        blanked["BloodravenBackupStale"] = {
            "anchor": "runbook.md#backup-stale",
            "firstCommand": "",
        }
        blank_findings = harness.normalise(
            golive.check_runbook_coverage(rules, blanked)
        )
        if [f[0] for f in blank_findings or []] != ["BloodravenBackupStale"]:
            errors.append(
                "an empty firstCommand should be flagged exactly once, got "
                + str(blank_findings)
            )

    return _verdict(errors)


# ---------------------------------------------------------------------------


def drill_record_separates_proved_from_assumed():
    """The drill record must claim only what a verification and a restore proved."""
    errors = []
    drill = harness.drill()

    problems = golive.check_drill(drill)
    if problems:
        errors.append("check_drill reports: " + "; ".join(problems))

    proved = set(drill.get("proved") or [])
    assumed = set(drill.get("assumed") or [])

    for item in sorted(golive.ASSUMED_REQUIRED):
        if item not in assumed:
            errors.append(f"assumed[] is missing {item!r}")
        if item in proved:
            errors.append(f"proved[] claims {item!r}, which no verification proves")

    if not proved:
        errors.append("proved[] is empty")
    if proved - golive.PROVED_VOCABULARY:
        errors.append("proved[] uses terms outside the vocabulary")
    if assumed - golive.ASSUMED_VOCABULARY:
        errors.append("assumed[] uses terms outside the vocabulary")

    if drill.get("backupSourceSite") in golive.READ_ONLY_SITES:
        errors.append("backupSourceSite names a read-only site")
    if drill.get("backupSourceReason") not in golive.BACKUP_SOURCE_REASONS:
        errors.append("backupSourceReason is not one of the three reason strings")
    if not str(drill.get("applicationSideAlertOwner", "")).strip():
        errors.append("applicationSideAlertOwner is empty")

    return _verdict(errors)
