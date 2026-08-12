"""Test harness for the brstatus project.

Finds the learner's brstatus.py, runs it against a fixture, and parses the
one-screen output back into something a test can assert on. Tests assert on
meaning - which cell holds which value - not on column widths.
"""

from __future__ import annotations

import ast
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"

COLUMNS = ("SITE", "ROLE", "STATE", "REPL", "LAG", "SERVING")


def find_tool():
    """Locate brstatus.py. Set BRSTATUS=<path> to override."""
    override = os.environ.get("BRSTATUS")
    if override:
        path = pathlib.Path(override).resolve()
        if not path.is_file():
            raise AssertionError("BRSTATUS=%s does not exist" % override)
        return path

    roots = [pathlib.Path.cwd().resolve(), HERE.parent]
    roots += list(pathlib.Path.cwd().resolve().parents)[:3]
    seen = []
    for root in roots:
        for candidate in (root / "brstatus.py", root / "starter" / "brstatus.py"):
            if candidate in seen:
                continue
            seen.append(candidate)
            if candidate.is_file():
                return candidate.resolve()
    raise AssertionError(
        "could not find brstatus.py; looked in %s" % ", ".join(str(p) for p in seen)
    )


def fixture(name):
    path = FIXTURES / name
    if not path.is_file():
        raise AssertionError("missing fixture %s" % path)
    return path


class Result:
    def __init__(self, code, stdout, stderr):
        self.code = code
        self.stdout = stdout
        self.stderr = stderr
        self.header = {}
        self.rows = {}
        self.verdict = ""
        self._parse()

    def _parse(self):
        lines = [line.rstrip() for line in self.stdout.splitlines() if line.strip()]
        if not lines:
            return
        for token in lines[0].split("  "):
            token = token.strip()
            if "=" in token:
                key, _, value = token.partition("=")
                self.header[key.strip()] = value.strip()
        in_table = False
        for line in lines[1:]:
            fields = line.split()
            if fields[:1] == [COLUMNS[0]]:
                in_table = True
                continue
            if line.startswith("VERDICT:"):
                self.verdict = line.split(":", 1)[1].strip()
                in_table = False
                continue
            if in_table and len(fields) == len(COLUMNS):
                self.rows[fields[0]] = dict(zip(COLUMNS, fields))

    def site(self, name):
        if name not in self.rows:
            raise AssertionError(
                "no row for site %r in output:\n%s" % (name, self.stdout)
            )
        return self.rows[name]

    def explain(self, message):
        return "%s\n--- exit=%s ---\n%s%s" % (
            message,
            self.code,
            self.stdout,
            self.stderr,
        )


def run(fixture_name):
    """Run brstatus.py against one fixture and parse the result."""
    tool = find_tool()
    proc = subprocess.run(
        [sys.executable, str(tool), str(fixture(fixture_name))],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return Result(proc.returncode, proc.stdout, proc.stderr)


def source():
    """The learner's brstatus.py as (text, parsed module)."""
    text = find_tool().read_text(encoding="utf-8")
    return text, ast.parse(text)


def function_def(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError("brstatus.py defines no function named %r" % name)


def calls_within(node):
    """Every function name called anywhere inside a function definition."""
    names = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            func = child.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def strings_within(node):
    return {
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }
