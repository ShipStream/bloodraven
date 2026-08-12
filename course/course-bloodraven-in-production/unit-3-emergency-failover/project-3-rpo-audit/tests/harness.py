"""Shared helpers for the braudit test cases.

Locates the learner's braudit.py, loads fixtures, and runs the CLI.
Prints nothing: each test case owns its own PASS / FAIL line.
"""

import importlib.util
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "fixtures")

_CANDIDATES = [
    os.environ.get("BRAUDIT"),
    os.path.join(os.getcwd(), "braudit.py"),
    os.path.join(os.getcwd(), "starter", "braudit.py"),
    os.path.join(os.path.dirname(HERE), "braudit.py"),
    os.path.join(os.path.dirname(HERE), "starter", "braudit.py"),
]


def braudit_path():
    for candidate in _CANDIDATES:
        if candidate and os.path.isfile(candidate):
            return os.path.abspath(candidate)
    raise SystemExit("braudit.py not found (looked in %s)" % ", ".join(c for c in _CANDIDATES if c))


_module = None


def braudit():
    """Import the learner's braudit.py once and return the module."""
    global _module
    if _module is None:
        spec = importlib.util.spec_from_file_location("braudit_under_test", braudit_path())
        _module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_module)
    return _module


def fixture(name):
    """Load a fixture capture as a fresh dict (safe to mutate)."""
    with open(os.path.join(FIXTURES, name), "r", encoding="utf-8") as fh:
        return json.load(fh)


def at(text):
    """RFC 3339 string -> aware datetime, using the learner's own parser."""
    return braudit().parse_time(text)


def audit(name, now, mutate=None):
    """Audit a fixture at a given instant, optionally mutating the capture first."""
    obj = fixture(name)
    if mutate is not None:
        mutate(obj)
    return braudit().audit(obj, at(now))


def run_cli(name, now):
    """Run braudit.py as a process. Returns (exit_code, stdout)."""
    proc = subprocess.run(
        [sys.executable, braudit_path(), os.path.join(FIXTURES, name), "--now", now],
        capture_output=True, text=True,
    )
    return proc.returncode, proc.stdout


def line(stdout, key):
    """Return the value of a `key: value` line, or None."""
    for row in stdout.splitlines():
        if row.startswith(key + ": "):
            return row[len(key) + 2:]
    return None
