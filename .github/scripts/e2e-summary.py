#!/usr/bin/env python3
"""Render a playground-chaos JUnit report as a Markdown job summary.

Usage: e2e-summary.py <title> <junit.xml> [<planned-ids-file>]

The planned-ids file holds the scenario IDs the job set out to run, one per
line or comma-separated. A planned scenario with no result in the report is
listed as "not run": run-all rewrites the report after every scenario, so
that is what a job killed at its timeout (or a list stopped at its first
failure) leaves behind. Always exits 0: a summary must never fail a job.
"""
import re
import sys
import xml.etree.ElementTree as ET


def duration(seconds):
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    return f"{seconds // 60}m{seconds % 60:02d}s"


def cell(text, limit=160):
    text = " ".join(text.split()).replace("|", "\\|")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return
    title, junit = argv[1], argv[2]
    planned = []
    if len(argv) > 3:
        try:
            with open(argv[3], encoding="utf-8") as f:
                planned = [p for p in re.split(r"[,\s]+", f.read()) if p]
        except OSError:
            pass

    cases = []
    try:
        cases = ET.parse(junit).getroot().findall("testcase")
    except (OSError, ET.ParseError) as err:
        print(f"### {title}\n\nNo JUnit report ({err}).\n")

    rows, seen, passed, failed = [], set(), 0, 0
    for case in cases:
        name = case.get("name", "")
        seen.add(name)
        failure = case.find("failure")
        took = duration(float(case.get("time") or 0))
        if failure is None:
            passed += 1
            rows.append(f"| `{name}` | pass | {took} | |")
        else:
            failed += 1
            where = failure.get("type", "")
            why = cell(failure.get("message", ""))
            rows.append(f"| `{name}` | **FAIL** | {took} | {where}: {why} |")
    missing = [p for p in planned if p not in seen]
    for name in missing:
        rows.append(f"| `{name}` | not run | | no result: the job ended first |")

    if cases:
        print(f"### {title}\n")
    counts = f"{passed} passed, {failed} failed"
    if missing:
        counts += f", {len(missing)} not run"
    print(counts + "\n")
    if rows:
        print("| Scenario | Result | Duration | Failure |")
        print("| --- | --- | --- | --- |")
        print("\n".join(rows))
        print()


if __name__ == "__main__":
    try:
        main(sys.argv)
    except Exception as err:  # noqa: BLE001 - never fail the job
        print(f"e2e-summary: {err}")
