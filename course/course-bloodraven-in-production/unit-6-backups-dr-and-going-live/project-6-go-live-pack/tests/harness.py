"""Locates the learner's pack and loads it. Do not edit anything under tests/."""

from __future__ import annotations

import sys
from pathlib import Path


def _root():
    here = Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "starter" / "golive.py").exists():
            return candidate
    raise SystemExit("cannot locate starter/golive.py above " + str(here))


ROOT = _root()
STARTER = ROOT / "starter"
PACK = STARTER / "pack"
FIXTURES = Path(__file__).resolve().parent / "fixtures"

if str(STARTER) not in sys.path:
    sys.path.insert(0, str(STARTER))

import golive  # noqa: E402
import promeval  # noqa: E402


def rules():
    return golive.load_rules(PACK / "alerts.yml")


def runbooks():
    return golive.load_runbooks(PACK / "runbooks.yml")


def drill():
    return golive.load_drill(PACK / "drill.json")


def fixture(name):
    return promeval.load_fixture(FIXTURES / (name + ".json"))


def firing(name):
    """Sorted Alert / Alert@site keys the learner's rules produce."""
    return golive.firing_keys(rules(), fixture(name))


def normalise(findings):
    """Accept list-of-tuples or list-of-lists from a learner's check."""
    if findings is None:
        return None
    return [tuple(item) for item in findings]
