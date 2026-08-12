#!/usr/bin/env python3
"""brstatus - one screen of truth about a MysqlFailoverGroup.

    python brstatus.py <group.json>

It reads the JSON of one MysqlFailoverGroup, exactly as printed by

    kubectl -n bloodraven-playground get mysqlfailovergroup orders -o json

and prints a header line, one line per site, and a verdict. The exit code
carries the verdict so you can put brstatus in a loop or a check script:

    0  OK        the group is not degraded
    1  DEGRADED  the group is degraded and still has an active site
    2  CRITICAL  the group is degraded and has no active site
    3  the input could not be read as a MysqlFailoverGroup

Three functions are stubbed out. Find TODO A, TODO B and TODO C.
"""

from __future__ import annotations

import json
import sys

# spec.sites[].role is enum-validated and defaults to primary-candidate.
DEFAULT_ROLE = "primary-candidate"

# spec.replication.maxLagSeconds defaults to 300. An object built outside
# admission may omit it, so the tool applies the same default the operator does.
DEFAULT_MAX_LAG_SECONDS = 300

COLUMNS = ("SITE", "ROLE", "STATE", "REPL", "LAG", "SERVING")


# ---------------------------------------------------------------------------
# Scaffolding. Already wired. Read it, call it, do not rewrite it.
# ---------------------------------------------------------------------------

def load_group(path):
    """Load a MysqlFailoverGroup from a JSON file. Raises ValueError if it
    is not one - a list from `kubectl get -o json` without a name is the
    usual mistake."""
    with open(path, "r", encoding="utf-8") as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict) or obj.get("kind") != "MysqlFailoverGroup":
        raise ValueError(
            "expected one MysqlFailoverGroup object, got kind=%r "
            "(name the group: kubectl get mysqlfailovergroup orders -o json)"
            % (obj.get("kind") if isinstance(obj, dict) else type(obj).__name__)
        )
    return obj


def site_role(site_spec):
    """The site's effective role, applying the CRD default."""
    return site_spec.get("role") or DEFAULT_ROLE


def effective_max_lag(spec):
    """spec.replication.maxLagSeconds, or the API default."""
    replication = spec.get("replication") or {}
    value = replication.get("maxLagSeconds")
    if not value:
        return DEFAULT_MAX_LAG_SECONDS
    return int(value)


def effective_readonly_max_lag(spec):
    """spec.replication.readOnlyMaxLagSeconds. It has no default of its own:
    absent inherits maxLagSeconds, but an explicit 0 is meaningful and means
    the reader must report zero lag."""
    replication = spec.get("replication") or {}
    if replication.get("readOnlyMaxLagSeconds") is not None:
        return int(replication["readOnlyMaxLagSeconds"])
    return effective_max_lag(spec)


def condition(status, condition_type):
    """One entry of status.conditions by type, or None."""
    for entry in status.get("conditions") or []:
        if entry.get("type") == condition_type:
            return entry
    return None


def canonical_host(host):
    """Compare replication source hosts the way the operator does: lowercase,
    trimmed, without the :3306 suffix and without a trailing dot."""
    host = (host or "").strip().lower()
    if host.endswith(":3306"):
        host = host[: -len(":3306")]
    return host.rstrip(".")


def expected_source_host(group_name, namespace, active_site):
    """The one source host a converged follower is allowed to have: the
    internal Service of the active site."""
    return "mysql-%s-%s-internal.%s.svc.cluster.local" % (
        group_name,
        active_site,
        namespace,
    )


def site_status_by_name(status):
    return {entry.get("name"): entry for entry in status.get("sites") or []}


# ---------------------------------------------------------------------------
# TODO A - render the lag cell.
# ---------------------------------------------------------------------------

def format_lag(site_status):
    """Return the LAG cell for one site.

    status.sites[].secondsBehindSource is a pointer. It is absent whenever
    the operator has no replication reading for the site at all - the active
    primary, a site it could not poll, a replica whose threads are stopped.
    Absent is not zero, and printing it as zero is how a detached replica
    ends up looking perfectly caught up.

    TODO A: return "unknown" when secondsBehindSource is absent or null, and
    "<n>s" otherwise (for example "0s", "300s").
    """
    return "%ss" % (site_status.get("secondsBehindSource") or 0)


# ---------------------------------------------------------------------------
# TODO B - decide whether a site is serving reads.
# ---------------------------------------------------------------------------

def is_serving(site_spec, site_status, group):
    """Is this site currently behind the group read endpoint,
    mysql-<group>-replicas?

    That Service selects on three labels: instance, role=replica and
    healthy=yes. The operator stamps healthy on each pod, and the rule it
    uses depends on the site's role.

    TODO B: replace the placeholder below with the operator's own rule.

      * For a site whose role is "read-only", healthy=yes needs all five of
        these to hold at once:
          1. sourceConvergenceState is "Converged"
          2. replicating is true
          3. secondsBehindSource is present (not null)
          4. canonical_host(sourceHost) equals expected_source_host(...) for
             the active site - a chained replica does not count
          5. secondsBehindSource is at or under effective_readonly_max_lag(spec)

      * For every other role, healthy=yes as soon as state is "writable" or
        "read-only". There is no lag gate on those sites. Read that sentence
        twice before you write the code.
    """
    status = group.get("status") or {}
    spec = group.get("spec") or {}
    active = status.get("activeSite") or ""

    # Invalid or incomplete authority deliberately sheds every endpoint.
    if not active:
        return False

    # The active primary carries role=primary, so it never matches the
    # role=replica selector on mysql-<group>-replicas.
    if site_status.get("name") == active:
        return False

    group_name = (group.get("metadata") or {}).get("name", "")
    namespace = (group.get("metadata") or {}).get("namespace", "")
    expected = expected_source_host(group_name, namespace, active)
    role = site_role(site_spec)

    return site_status.get("state") == "read-only"


# ---------------------------------------------------------------------------
# TODO C - the verdict and the exit code.
# ---------------------------------------------------------------------------

def verdict(group):
    """Return (word, exit_code) for the group.

    The operator has already done this work. Every poll it writes a Degraded
    condition whose reason is one of Healthy, Degraded, SplitBrain,
    NoPrimary or TotalLoss, plus the replication reasons
    ReplicationBroken, ReplicationLagging, ReplicationError and
    ReplicationSourceMismatch. Read the condition. Do not re-derive group
    health from the site rows.

    TODO C:
      * Degraded absent, or its status is not "True"  -> ("OK", 0)
      * Degraded is "True" and status.activeSite is set -> ("DEGRADED", 1)
      * Degraded is "True" and status.activeSite is empty -> ("CRITICAL", 2)
    """
    status = group.get("status") or {}
    return ("OK", 0)


# ---------------------------------------------------------------------------
# Scaffolding again: rendering and the entry point.
# ---------------------------------------------------------------------------

def build_rows(group):
    spec = group.get("spec") or {}
    status = group.get("status") or {}
    observed = site_status_by_name(status)
    rows = []
    for site_spec in spec.get("sites") or []:
        name = site_spec.get("name", "")
        site_status = observed.get(name, {"name": name})
        rows.append(
            (
                name,
                site_role(site_spec),
                site_status.get("state") or "unknown",
                "yes" if site_status.get("replicating") else "no",
                format_lag(site_status),
                "yes" if is_serving(site_spec, site_status, group) else "no",
            )
        )
    return rows


def render(group, rows, word):
    metadata = group.get("metadata") or {}
    status = group.get("status") or {}

    ready = condition(status, "Ready")
    degraded = condition(status, "Degraded")
    ready_cell = ready.get("status", "Unknown") if ready else "Unknown"
    if degraded:
        degraded_cell = "%s(%s)" % (
            degraded.get("status", "Unknown"),
            degraded.get("reason", "none"),
        )
    else:
        degraded_cell = "Unknown(none)"

    lines = [
        "%s/%s  active=%s  ready=%s  degraded=%s"
        % (
            metadata.get("name", "?"),
            metadata.get("namespace", "?"),
            status.get("activeSite") or "none",
            ready_cell,
            degraded_cell,
        )
    ]

    if degraded and degraded.get("status") == "True" and degraded.get("message"):
        lines.append("ALERT: %s" % degraded["message"])

    widths = [len(head) for head in COLUMNS]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    template = "  ".join("%%-%ds" % width for width in widths).rstrip()
    lines.append((template % COLUMNS).rstrip())
    for row in rows:
        lines.append((template % row).rstrip())

    lines.append("VERDICT: %s" % word)
    return "\n".join(lines)


def main(argv):
    if len(argv) != 2:
        print("usage: brstatus.py <group.json>", file=sys.stderr)
        return 3
    try:
        group = load_group(argv[1])
    except (OSError, ValueError, json.JSONDecodeError) as err:
        print("brstatus: %s" % err, file=sys.stderr)
        return 3

    word, code = verdict(group)
    print(render(group, build_rows(group), word))
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
