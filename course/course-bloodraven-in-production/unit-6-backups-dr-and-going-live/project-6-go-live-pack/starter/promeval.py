"""A deliberately small PromQL-subset evaluator.

This file is scaffolding. You do not need to change it, but you do need
to know what it supports, because it decides whether your alert rules
fire against the fixtures.

Supported expression shapes (whitespace is flexible):

    <selector> <cmp> <number>
    absent(<selector>)
    absent(<selector> <cmp> <number>)
    time() - <selector> <cmp> <number>
    increase(<selector>[<duration>]) <cmp> <number>

A selector is ``metric_name`` or ``metric_name{label="v",other!="w"}``.
Matchers support ``=``, ``!=``, ``=~`` and ``!~``; the regex forms are
anchored (``re.fullmatch``), exactly like Prometheus.

Comparisons are ``>``, ``>=``, ``<``, ``<=``, ``==``, ``!=``.

Two deliberate simplifications, so you know what the checker is and is
not proving:

  * Evaluation is instantaneous. A rule's ``for:`` duration is never
    simulated — a fixture is one scrape, not a window. ``for:`` is still
    required on every paging rule, because a rule without one pages on a
    single bad scrape.
  * ``increase(m[15m])`` does not compute anything. It reads the
    pre-computed series the fixture stores under ``increases["15m"]``.
    The window in the expression must match a key in that block.

Fixture format (JSON)::

    {
      "id": "primary-lost",
      "title": "...",
      "now": 1786550400,
      "samples":   [{"name": "...", "labels": {...}, "value": 1}],
      "increases": {"15m": [{"name": "...", "labels": {...}, "value": 1}]},
      "expectedAlerts": ["BloodravenNoWritableSite"]
    }
"""

from __future__ import annotations

import json
import re
from pathlib import Path

CMP_OPS = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}

_FUNCS = {"absent", "increase", "rate", "time", "sum", "max", "min", "avg", "count"}

_SELECTOR = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{(.*)\})?$", re.S)
_MATCHER = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"([^"]*)"')
_ABSENT = re.compile(r"^absent\s*\(\s*(.*?)\s*\)\s*$", re.S)
_TIME = re.compile(
    r"^time\s*\(\s*\)\s*-\s*(.+?)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$", re.S
)
_INCREASE = re.compile(
    r"^increase\s*\(\s*(.+?)\s*\[\s*([0-9]+[smhdw])\s*\]\s*\)\s*"
    r"(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$",
    re.S,
)
_COMPARISON = re.compile(r"^(.+?)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$", re.S)


class ExprError(ValueError):
    """Raised when an expression is outside the supported subset."""


def load_fixture(path):
    """Load a metric fixture from JSON."""
    data = json.loads(Path(path).read_text())
    data.setdefault("samples", [])
    data.setdefault("increases", {})
    data.setdefault("expectedAlerts", [])
    return data


def metric_names_in(expr):
    """Return the sorted metric names an expression references.

    Label blocks and range selectors are stripped first, so label names
    and durations are never mistaken for metric names. Function names
    (``absent``, ``increase``, ``time`` ...) are excluded.
    """
    stripped = re.sub(r"\{[^}]*\}", "", str(expr))
    stripped = re.sub(r"\[[^\]]*\]", "", stripped)
    names = set()
    for match in re.finditer(r"[a-zA-Z_:][a-zA-Z0-9_:]*", stripped):
        name = match.group(0)
        if name in _FUNCS:
            continue
        names.add(name)
    return sorted(names)


def _parse_selector(text):
    match = _SELECTOR.match(text.strip())
    if not match:
        raise ExprError(f"not a selector: {text!r}")
    name = match.group(1)
    raw = match.group(2) or ""
    matchers = []
    consumed = 0
    for m in _MATCHER.finditer(raw):
        matchers.append((m.group(1), m.group(2), m.group(3)))
        consumed += len(m.group(0))
    if raw.strip(" ,") and consumed == 0:
        raise ExprError(f"unparsable label matchers in {text!r}")
    return name, matchers


def _matches(sample, name, matchers):
    if sample.get("name") != name:
        return False
    labels = sample.get("labels", {})
    for key, op, value in matchers:
        actual = labels.get(key, "")
        if op == "=" and actual != value:
            return False
        if op == "!=" and actual == value:
            return False
        if op == "=~" and not re.fullmatch(value, actual):
            return False
        if op == "!~" and re.fullmatch(value, actual):
            return False
    return True


def _select(samples, text):
    name, matchers = _parse_selector(text)
    return [s for s in samples if _matches(s, name, matchers)]


def _compare(series, op, threshold):
    fn = CMP_OPS[op]
    return [s for s in series if fn(float(s["value"]), float(threshold))]


def eval_expr(expr, fixture):
    """Evaluate one expression against one fixture.

    Returns the result vector: a list of ``{"labels": ..., "value": ...}``
    entries. An empty list means the rule does not fire.
    """
    expr = str(expr).strip()

    absent = _ABSENT.match(expr)
    if absent:
        inner = eval_expr(absent.group(1), fixture)
        return [] if inner else [{"labels": {}, "value": 1.0}]

    timed = _TIME.match(expr)
    if timed:
        now = float(fixture.get("now", 0))
        series = _select(fixture["samples"], timed.group(1))
        shifted = [
            {"labels": s.get("labels", {}), "value": now - float(s["value"])}
            for s in series
        ]
        return _compare(shifted, timed.group(2), timed.group(3))

    increased = _INCREASE.match(expr)
    if increased:
        window = increased.group(2)
        pool = fixture.get("increases", {}).get(window)
        if pool is None:
            raise ExprError(
                f"fixture {fixture.get('id')!r} has no increases block for window {window!r}"
            )
        series = _select(pool, increased.group(1))
        out = [{"labels": s.get("labels", {}), "value": float(s["value"])} for s in series]
        return _compare(out, increased.group(3), increased.group(4))

    compared = _COMPARISON.match(expr)
    if compared:
        series = _select(fixture["samples"], compared.group(1))
        out = [{"labels": s.get("labels", {}), "value": float(s["value"])} for s in series]
        return _compare(out, compared.group(2), compared.group(3))

    series = _select(fixture["samples"], expr)
    return [{"labels": s.get("labels", {}), "value": float(s["value"])} for s in series]
