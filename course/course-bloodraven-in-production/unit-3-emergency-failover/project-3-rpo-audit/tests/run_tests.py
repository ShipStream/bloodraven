#!/usr/bin/env python3
"""Run every testCase in project.json exactly as the grader would.

    python3 tests/run_tests.py            # grades ./braudit.py (or ./starter/braudit.py)
    BRAUDIT=/path/to/braudit.py python3 tests/run_tests.py

Each test case is executed in its own process with the project directory as the
working directory, and its stdout is compared with the case's expectedOutput.
"""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)


def main():
    with open(os.path.join(PROJECT, "project.json"), "r", encoding="utf-8") as fh:
        spec = json.load(fh)

    earned = 0
    total = 0
    for case in spec["testCases"]:
        total += case["weight"]
        proc = subprocess.run([sys.executable, "-c", case["code"]],
                              cwd=PROJECT, capture_output=True, text=True)
        got = proc.stdout.strip()
        ok = got == case["expectedOutput"].strip()
        earned += case["weight"] if ok else 0
        print("%-4s %-3d %s" % ("PASS" if ok else "FAIL", case["weight"], case["name"]))
        if not ok:
            for row in (got or "(no stdout)").splitlines():
                print("       %s" % row)
            if proc.stderr.strip():
                print("       stderr: %s" % proc.stderr.strip().splitlines()[-1])
    print("score: %d/%d" % (earned, total))
    return 0 if earned == total else 1


if __name__ == "__main__":
    sys.exit(main())
