#!/usr/bin/env mysqlsh --py -f
# Bloodraven restore script. Executed inside a restore Job running the
# mysqlsh image. Loads a previous dump into an empty MySQL instance via
# util.loadDump(), and optionally replays archived binlogs up to a
# point-in-time target (PITR) on top of the loaded dump.
#
# Required env:
#   BLOODRAVEN_MYSQL_HOST       host[:port] or [ipv6]:port of the target
#   BLOODRAVEN_INPUT_URL        dump source (local path or s3 prefix)
#   BLOODRAVEN_LOAD_OPTIONS     JSON object with util.loadDump() options
#   BLOODRAVEN_MYSQL_CREDS_DIR  directory with MYSQL_USER / MYSQL_PASSWORD files
#
# Optional env (full-dump only):
#   BLOODRAVEN_TLS=1                  require TLS on the session
#   BLOODRAVEN_S3_BUCKET              bucket name (S3 sources)
#   BLOODRAVEN_S3_ENDPOINT_OVERRIDE   non-AWS endpoint
#   BLOODRAVEN_AWS_CREDS_DIR          directory with AWS_* files (S3 sources)
#   MYSQL_USER / MYSQL_PASSWORD       legacy env-var fallback (not recommended)
#
# Optional in-place restore env (destructive; used by spec.restoreInPlace):
#   BLOODRAVEN_DROP_SCHEMAS           comma-separated list of schemas to DROP
#                                     before running util.loadDump(). Used by
#                                     in-place per-schema restore to clear the
#                                     target schema in the live primary.
#   BLOODRAVEN_DROP_ALL_USER_SCHEMAS=1  drop every non-system schema before
#                                     loading. Used by in-place full-instance
#                                     restore to wipe the live primary's
#                                     user data. System schemas (mysql,
#                                     information_schema, performance_schema,
#                                     sys) are never dropped.
#   BLOODRAVEN_RESET_REPLICATION=1    run STOP REPLICA; RESET REPLICA ALL
#                                     before loading. Used by in-place
#                                     full-instance restore on the active
#                                     primary so the incoming load does not
#                                     collide with the old replication
#                                     metadata.
#
# Optional PITR env (enables post-load binlog replay):
#   BLOODRAVEN_PITR_STOP_DATETIME     mysqlbinlog --stop-datetime target
#                                     (RFC 3339 or MySQL form).
#   BLOODRAVEN_PITR_EXCLUDE_GTIDS     optional mysqlbinlog --exclude-gtids set.
#   BLOODRAVEN_PITR_LOCAL_DIR         local directory populated by the
#                                     `bloodraven pitr-download` init
#                                     container. Contains per-site
#                                     subdirs with sealed binlog files;
#                                     this script just globs them and
#                                     pipes through mysqlbinlog | mysql.
#   BLOODRAVEN_PITR_FILTER_DATABASE   when set, add mysqlbinlog --database=<x>
#                                     to the replay pipeline. Used by
#                                     in-place per-schema restore so PITR
#                                     replay only applies events for the
#                                     target schema. NOTE: --database
#                                     filters on the session's default
#                                     database at log time, not on the
#                                     actually-referenced schemas — not
#                                     airtight for apps that issue
#                                     cross-schema statements.
import datetime
import glob
import json
import os
import subprocess
import sys
import urllib.parse

import mysqlsh  # type: ignore


def _bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _tls_options():
    """MySQL TLS options for this job, as (ssl_mode, ssl_ca).

    BLOODRAVEN_TLS is set by the operator whenever the failover group has
    spec.tls, which also means mysqld runs with
    require_secure_transport=ON. The CA the operator mounts is used when
    it is present so the connection is verified rather than merely
    encrypted; REQUIRED is the fallback when no CA reached the pod.
    PREFERRED (the client default) is kept for non-TLS groups so their
    behaviour is unchanged.
    """
    if not _bool("BLOODRAVEN_TLS"):
        return "PREFERRED", ""
    ca = (os.environ.get("BLOODRAVEN_TLS_CA_FILE") or "").strip()
    if ca and os.path.exists(ca):
        return "VERIFY_CA", ca
    return "REQUIRED", ""



def _host_port(addr, default_port=3306):
    if not addr:
        return addr, default_port
    if addr.startswith("["):
        close = addr.find("]")
        if close == -1:
            return addr, default_port
        host = addr[1:close]
        rest = addr[close + 1:]
        if rest.startswith(":"):
            try:
                return host, int(rest[1:])
            except ValueError:
                return host, default_port
        return host, default_port
    if ":" in addr:
        host, port = addr.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            return addr, default_port
    return addr, default_port


def _read_cred_file(dirpath, key):
    path = os.path.join(dirpath, key)
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except OSError:
        return None


def _int_env(name):
    try:
        return int(os.environ.get(name, "0") or "0")
    except ValueError:
        return 0


def _escape_token(value):
    return urllib.parse.quote(str(value or ""), safe="")


def _resolve_credentials():
    creds_dir = os.environ.get("BLOODRAVEN_MYSQL_CREDS_DIR")
    user = None
    password = None
    if creds_dir:
        user = _read_cred_file(creds_dir, "MYSQL_USER")
        password = _read_cred_file(creds_dir, "MYSQL_PASSWORD")
    if not user:
        user = os.environ.get("MYSQL_USER")
    if not password:
        password = os.environ.get("MYSQL_PASSWORD")
    if not user:
        print("BLOODRAVEN_LOAD_FAILED: no MYSQL_USER in creds dir or env",
              file=sys.stderr, flush=True)
        sys.exit(2)
    return user, password or ""


def _configure_aws_creds_dir():
    aws_dir = os.environ.get("BLOODRAVEN_AWS_CREDS_DIR")
    if not aws_dir:
        return
    access_key = _read_cred_file(aws_dir, "AWS_ACCESS_KEY_ID")
    secret_key = _read_cred_file(aws_dir, "AWS_SECRET_ACCESS_KEY")
    session = _read_cred_file(aws_dir, "AWS_SESSION_TOKEN")
    region = _read_cred_file(aws_dir, "AWS_REGION")
    if not access_key or not secret_key:
        return
    home = os.environ.get("HOME", "/tmp")
    aws_conf_dir = os.path.join(home, ".aws")
    try:
        os.makedirs(aws_conf_dir, exist_ok=True)
    except OSError:
        return
    creds_path = os.path.join(aws_conf_dir, "credentials")
    lines = [
        "[default]",
        "aws_access_key_id = {}".format(access_key),
        "aws_secret_access_key = {}".format(secret_key),
    ]
    if session:
        lines.append("aws_session_token = {}".format(session))
    try:
        with open(creds_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.chmod(creds_path, 0o600)
    except OSError:
        return
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = creds_path
    if region and not os.environ.get("AWS_REGION"):
        os.environ["AWS_REGION"] = region


# --------------------------------------------------------------------
# In-place restore preflight: drop schemas / reset replication
# --------------------------------------------------------------------
#
# For in-place restore (spec.restoreInPlace) the target mysqld is
# already live and may hold either an older copy of the schema we are
# about to re-load (per-schema) or a full pre-restore dataset
# (full-instance). util.loadDump() will not replace existing objects,
# so we have to clear the target explicitly before calling it.
#
# These helpers are no-ops unless the corresponding env vars are set,
# so the bootstrap restore path (initFromBackup) continues to run
# against an empty datadir with no preflight side effects.

# System schemas that must never be dropped.
_SYSTEM_SCHEMAS = frozenset({
    "mysql",
    "information_schema",
    "performance_schema",
    "sys",
})


def _maybe_reset_replication(session):
    """STOP REPLICA; RESET REPLICA ALL on the target before loading.

    Used by in-place full-instance restore so the loaded dump cannot
    collide with the old replication metadata (e.g. a replication
    channel pointing at the peer site that is about to be re-cloned).
    """
    if not _bool("BLOODRAVEN_RESET_REPLICATION"):
        return
    print("BLOODRAVEN_RESET_REPLICATION_START", flush=True)
    # Both statements are best-effort. A MySQL instance that has never
    # been configured as a replica (no channel metadata — common for
    # the primary of a fresh-deployed failover group) will error on
    # either STOP REPLICA or RESET REPLICA ALL depending on the server
    # version, and a legitimate in-place restore against such a
    # primary must still succeed. We swallow the errors rather than
    # sniff specific error codes because the desired post-condition
    # ("replication metadata cleared") is already true in the
    # no-channel case.
    try:
        session.run_sql("STOP REPLICA")
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_RESET_REPLICATION_STOP_IGNORED: {}".format(e),
              flush=True)
    try:
        session.run_sql("RESET REPLICA ALL")
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_RESET_REPLICATION_RESET_IGNORED: {}".format(e),
              flush=True)
    print("BLOODRAVEN_RESET_REPLICATION_DONE", flush=True)


def _list_user_schemas(session):
    """Return every schema on the target minus the system schemas."""
    res = session.run_sql("SHOW DATABASES")
    rows = res.fetch_all()
    return [r[0] for r in rows if r[0] not in _SYSTEM_SCHEMAS]


def _drop_schemas(session, schemas):
    """DROP DATABASE IF EXISTS on each schema, refusing to touch the
    system schemas even if the caller accidentally lists one."""
    for s in schemas:
        if not s:
            continue
        if s in _SYSTEM_SCHEMAS:
            print("BLOODRAVEN_DROP_SKIPPED_SYSTEM_SCHEMA schema={}".format(s),
                  flush=True)
            continue
        print("BLOODRAVEN_DROP_SCHEMA schema={}".format(s), flush=True)
        session.run_sql("DROP DATABASE IF EXISTS `{}`".format(s.replace("`", "``")))


def _maybe_preflight_drops(session):
    """Perform any destructive pre-load cleanup requested via env vars.

    Supports two modes:
      - BLOODRAVEN_DROP_SCHEMAS: explicit comma-separated list.
      - BLOODRAVEN_DROP_ALL_USER_SCHEMAS=1: drop every non-system schema.

    Both can be set; the explicit list runs first for predictable
    logging, then the full-instance sweep picks up anything left."""
    explicit = os.environ.get("BLOODRAVEN_DROP_SCHEMAS", "").strip()
    if explicit:
        schemas = [s.strip() for s in explicit.split(",") if s.strip()]
        if schemas:
            print("BLOODRAVEN_DROP_SCHEMAS_START count={}".format(len(schemas)),
                  flush=True)
            _drop_schemas(session, schemas)
            print("BLOODRAVEN_DROP_SCHEMAS_DONE", flush=True)

    if _bool("BLOODRAVEN_DROP_ALL_USER_SCHEMAS"):
        user_schemas = _list_user_schemas(session)
        print("BLOODRAVEN_DROP_ALL_USER_SCHEMAS_START count={}".format(
              len(user_schemas)), flush=True)
        _drop_schemas(session, user_schemas)
        print("BLOODRAVEN_DROP_ALL_USER_SCHEMAS_DONE", flush=True)


# --------------------------------------------------------------------
# PITR: binlog replay on top of the loaded dump
# --------------------------------------------------------------------
#
# The `bloodraven pitr-download` init container has already fetched
# every archived binlog that could contribute to the replay window and
# dropped them into BLOODRAVEN_PITR_LOCAL_DIR, organized per site:
#
#   /pitr-binlogs/
#   ├── us-east-1a/
#   │   ├── mysql-bin.000042
#   │   └── mysql-bin.000043
#   └── us-east-1b/
#       └── mysql-bin.000001
#
# All this script has to do is glob them in deterministic order and
# pipe `mysqlbinlog --stop-datetime=<target>` through `mysql
# --binary-mode` to the target instance. GTID dedup on the server
# skips transactions already present from the dump load, so the order
# across sites doesn't matter for correctness.


def _parse_pitr_target(s):
    """Normalize the user-provided datetime to mysqlbinlog's expected
    'YYYY-MM-DD HH:MM:SS' form. Accepts RFC 3339 ("2026-04-15T09:30:00Z"
    or with offset) for k8s-style timestamp pasting."""
    s = s.strip()
    if "T" in s:
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return s


def _collect_binlogs(local_dir):
    """Return every binlog file under local_dir in a deterministic
    order (site, then filename). Missing / empty dirs return an empty
    list so callers can treat "nothing to replay" as a no-op."""
    files = []
    if not os.path.isdir(local_dir):
        return files
    for site in sorted(os.listdir(local_dir)):
        site_dir = os.path.join(local_dir, site)
        if not os.path.isdir(site_dir):
            continue
        for fn in sorted(glob.glob(os.path.join(site_dir, "mysql-bin.*"))):
            # Skip the partial-download marker files the init container
            # uses for atomic rename; shouldn't exist by the time we
            # run, but guard against it anyway.
            if fn.endswith(".part"):
                continue
            files.append(fn)
    return files


def _run_pitr(host, port, user, password, stop_datetime, exclude_gtids, local_dir,
              filter_database):
    files = _collect_binlogs(local_dir)
    if not files:
        print("BLOODRAVEN_PITR_NOOP: no archived binlogs in {}; dump load is final state".format(
              local_dir), flush=True)
        return "", 0

    binlog_cmd = ["mysqlbinlog", "--stop-datetime=" + stop_datetime]
    if exclude_gtids:
        binlog_cmd += ["--exclude-gtids=" + exclude_gtids]
    if filter_database:
        # --database filters on the session default DB at log time, not
        # on actually-referenced schemas. Caller is responsible for
        # understanding the caveat (see header docstring).
        binlog_cmd += ["--database=" + filter_database]
    binlog_cmd += files
    mysql_cmd = [
        "mysql", "--binary-mode",
        "-h", host, "-P", str(port),
        "-u", user,
    ]
    # The C client defaults to --ssl-mode=PREFERRED, so this pipeline
    # happened to survive require_secure_transport=ON — but unverified.
    # Match the mysqlsh session above instead of relying on the default.
    replay_ssl_mode, replay_ssl_ca = _tls_options()
    if replay_ssl_mode != "PREFERRED":
        mysql_cmd.append("--ssl-mode=" + replay_ssl_mode)
        if replay_ssl_ca:
            mysql_cmd.append("--ssl-ca=" + replay_ssl_ca)

    print("BLOODRAVEN_PITR_START stop_datetime={} files={}".format(
          stop_datetime, len(files)), flush=True)

    # Pass the password via MYSQL_PWD rather than -p so it never lands
    # on argv / procfs.
    env = dict(os.environ)
    env["MYSQL_PWD"] = password
    p1 = subprocess.Popen(binlog_cmd, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(mysql_cmd, stdin=p1.stdout, env=env)
    p1.stdout.close()
    rc2 = p2.wait()
    rc1 = p1.wait()
    if rc1 != 0:
        print("BLOODRAVEN_PITR_FAILED: mysqlbinlog exit={}".format(rc1),
              file=sys.stderr, flush=True)
        sys.exit(2)
    if rc2 != 0:
        print("BLOODRAVEN_PITR_FAILED: mysql exit={}".format(rc2),
              file=sys.stderr, flush=True)
        sys.exit(2)
    print("BLOODRAVEN_PITR_COMPLETE stop_datetime={}".format(stop_datetime),
          flush=True)
    return files[-1], len(files)


# --------------------------------------------------------------------
# local_infile: a hard precondition of the load
# --------------------------------------------------------------------
#
# util.loadDump() loads table data with LOAD DATA LOCAL INFILE, which the
# server rejects unless the global system variable local_infile is ON.
# Production primaries run with the hardened MySQL 8 default local_infile
# =OFF, so the load simply cannot succeed there until it is enabled.
#
# That makes it a PRECONDITION, not a best-effort nicety: it is enabled
# and verified BEFORE the destructive preflight runs. Enabling it after
# the drops (or letting a failed enable through) is what dropped a live
# schema and then failed inside load_dump() with "local_infile disabled
# in server" — the schema was gone with nothing to reload it from. If it
# cannot be turned on we abort while the data is still intact.
#
# The prior value is put back once the load finishes, so the security
# posture outside the load window is unchanged. The verification path
# already starts its throwaway mysqld with --local-infile=1, so there the
# value reads back ON and this is a no-op with nothing to restore.


def _read_local_infile(session):
    """Return @@GLOBAL.local_infile as 1/0, or None if it cannot be read."""
    try:
        row = session.run_sql("SELECT @@GLOBAL.local_infile").fetch_one()
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_LOCAL_INFILE_READ_FAILED: {}".format(e),
              file=sys.stderr, flush=True)
        return None
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        # Some builds report ON/OFF rather than 1/0.
        return 1 if str(row[0]).strip().upper() in ("ON", "1", "TRUE") else 0


def _prepare_local_infile(session, state):
    """Guarantee @@GLOBAL.local_infile=ON before anything destructive runs.

    Returns the value to restore after the load (0), or None when the server
    was already ON and there is nothing to put back. Exits(2) — with NOTHING
    yet dropped — when the variable cannot be read, enabled, or verified,
    because a load that is certain to fail must not be preceded by a DROP.
    """
    prev = _read_local_infile(session)
    if prev is None:
        print("BLOODRAVEN_LOCAL_INFILE_UNAVAILABLE: cannot read "
              "@@GLOBAL.local_infile; refusing to run a destructive restore "
              "whose load would then fail", file=sys.stderr, flush=True)
        sys.exit(2)
    state["previous"] = prev
    if prev == 1:
        # Already ON — nothing to change and nothing to restore.
        return None

    try:
        session.run_sql("SET GLOBAL local_infile=ON")
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_LOCAL_INFILE_UNAVAILABLE: cannot enable "
              "local_infile (the restore user needs SYSTEM_VARIABLES_ADMIN): "
              "{}".format(e), file=sys.stderr, flush=True)
        sys.exit(2)

    # Read it back: a SET that did not stick would put us right back in the
    # drop-then-fail hole this check exists to prevent.
    if _read_local_infile(session) != 1:
        print("BLOODRAVEN_LOCAL_INFILE_UNAVAILABLE: local_infile is still OFF "
              "after SET GLOBAL local_infile=ON", file=sys.stderr, flush=True)
        sys.exit(2)

    print("BLOODRAVEN_LOCAL_INFILE_ENABLED prev=OFF", flush=True)
    return prev


def _restore_local_infile(session, prev):
    """Restore @@GLOBAL.local_infile to the value _prepare_local_infile
    captured. No-op when prev is None (unchanged / already ON)."""
    if prev is None:
        return
    want = "ON" if prev == 1 else "OFF"
    session.run_sql("SET GLOBAL local_infile={}".format(want))
    if _read_local_infile(session) != prev:
        raise RuntimeError("local_infile readback did not match restored value {}".format(want))
    print("BLOODRAVEN_LOCAL_INFILE_RESTORED value={}".format(want), flush=True)


def _target_coordinates(session):
    gtid = ""
    try:
        row = session.run_sql("SELECT @@global.gtid_executed").fetch_one()
        if row and row[0] is not None:
            gtid = str(row[0]).replace("\n", "")
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_COORDINATES_GTID_IGNORED: {}".format(e), flush=True)

    binlog_file = ""
    binlog_pos = 0
    try:
        try:
            row = session.run_sql("SHOW BINARY LOG STATUS").fetch_one()
        except Exception:  # noqa: BLE001
            row = session.run_sql("SHOW MASTER STATUS").fetch_one()
        if row:
            binlog_file = str(row[0] or "")
            binlog_pos = int(row[1] or 0)
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_COORDINATES_BINLOG_IGNORED: {}".format(e), flush=True)
    return gtid, binlog_file, binlog_pos


def _print_restore_complete(session, pitr_stop_datetime, pitr_file, pitr_count):
    target_gtid, target_file, target_pos = _target_coordinates(session)
    parts = [
        "BLOODRAVEN_RESTORE_COMPLETE",
        "sourceSizeBytes={}".format(_int_env("BLOODRAVEN_SOURCE_SIZE_BYTES")),
        "sourceGtidExecuted={}".format(_escape_token(os.environ.get("BLOODRAVEN_SOURCE_GTID_EXECUTED", ""))),
        "sourceBinlogFile={}".format(_escape_token(os.environ.get("BLOODRAVEN_SOURCE_BINLOG_FILE", ""))),
        "sourceBinlogPos={}".format(_int_env("BLOODRAVEN_SOURCE_BINLOG_POS")),
        "targetGtidExecuted={}".format(_escape_token(target_gtid)),
        "targetBinlogFile={}".format(_escape_token(target_file)),
        "targetBinlogPos={}".format(target_pos),
        "pitrStopDatetime={}".format(_escape_token(pitr_stop_datetime)),
        "pitrReplayedBinlogFile={}".format(_escape_token(pitr_file)),
        "pitrReplayedBinlogCount={}".format(int(pitr_count or 0)),
    ]
    print(" ".join(parts), flush=True)


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------


def main():
    host = os.environ["BLOODRAVEN_MYSQL_HOST"]
    input_url = os.environ["BLOODRAVEN_INPUT_URL"]
    user, password = _resolve_credentials()

    opts = json.loads(os.environ.get("BLOODRAVEN_LOAD_OPTIONS") or "{}")

    bucket = os.environ.get("BLOODRAVEN_S3_BUCKET")
    if bucket:
        opts["s3BucketName"] = bucket
    endpoint = os.environ.get("BLOODRAVEN_S3_ENDPOINT_OVERRIDE")
    if endpoint:
        opts["s3EndpointOverride"] = endpoint

    # Verification runs mount the backup source read-only, so mysqlsh
    # can't write its default `load-progress.<uid>.json` alongside the
    # dump. When BLOODRAVEN_LOAD_PROGRESS_FILE is set the caller takes
    # over progressFile selection; an empty value disables tracking.
    progress_file = os.environ.get("BLOODRAVEN_LOAD_PROGRESS_FILE")
    if progress_file is not None:
        opts["progressFile"] = progress_file

    _configure_aws_creds_dir()

    host_only, port = _host_port(host)
    _ssl_mode, _ssl_ca = _tls_options()
    conn = {
        "host": host_only,
        "port": port,
        "user": user,
        "password": password,
        "ssl-mode": _ssl_mode,
    }
    if _ssl_ca:
        conn["ssl-ca"] = _ssl_ca

    print("BLOODRAVEN_LOAD_START host={} input={}".format(host, input_url),
          flush=True)

    mysqlsh.globals.shell.connect(conn)

    session = mysqlsh.globals.session

    # util.loadDump() needs @@GLOBAL.local_infile=ON. Establish that FIRST:
    # everything below this line is destructive, and a load that cannot
    # possibly succeed must not be preceded by a DROP. Exits(2) here leave
    # the target untouched. The prior value is put back in the finally,
    # which also runs on the sys.exit(2) paths below, so a hardened primary
    # ends up OFF again whatever happens.
    local_infile_state = {"previous": None}
    try:
        _prepare_local_infile(session, local_infile_state)
        # Destructive preflight for in-place restore. The bootstrap restore
        # path does not set any of the triggering env vars, so these are
        # no-ops outside of spec.restoreInPlace.
        try:
            _maybe_reset_replication(session)
            _maybe_preflight_drops(session)
        except Exception as e:  # noqa: BLE001
            print("BLOODRAVEN_PREFLIGHT_FAILED: {}".format(e), file=sys.stderr,
                  flush=True)
            sys.exit(2)

        try:
            mysqlsh.globals.util.load_dump(input_url, opts)
        except Exception as e:  # noqa: BLE001
            print("BLOODRAVEN_LOAD_FAILED: {}".format(e), file=sys.stderr,
                  flush=True)
            sys.exit(2)
    finally:
        _restore_local_infile(session, local_infile_state["previous"])

    print("BLOODRAVEN_LOAD_COMPLETE input={}".format(input_url), flush=True)

    # Optional PITR replay on top of the loaded dump.
    stop_dt = os.environ.get("BLOODRAVEN_PITR_STOP_DATETIME")
    pitr_stop_datetime = ""
    pitr_file = ""
    pitr_count = 0
    if stop_dt:
        stop_dt_mysql = _parse_pitr_target(stop_dt)
        pitr_stop_datetime = stop_dt_mysql
        exclude_gtids = os.environ.get("BLOODRAVEN_PITR_EXCLUDE_GTIDS", "")
        local_dir = os.environ.get("BLOODRAVEN_PITR_LOCAL_DIR", "/pitr-binlogs")
        filter_db = os.environ.get("BLOODRAVEN_PITR_FILTER_DATABASE", "")
        pitr_file, pitr_count = _run_pitr(host_only, port, user, password,
                                          stop_dt_mysql, exclude_gtids,
                                          local_dir, filter_db)

    _print_restore_complete(session, pitr_stop_datetime, pitr_file, pitr_count)


if __name__ == "__main__":
    main()
