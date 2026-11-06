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
#   BLOODRAVEN_REQUIRE_OFFLINE_MODE=1 refuse to run the destructive preflight
#                                     unless @@GLOBAL.offline_mode is ON. The
#                                     operator sets it to fence application
#                                     sessions during a full-instance restore.
#   BLOODRAVEN_RESET_REPLICATION=1    run STOP REPLICA; RESET REPLICA ALL
#                                     before loading. Used by in-place
#                                     full-instance restore on the active
#                                     primary so the incoming load does not
#                                     collide with the old replication
#                                     metadata.
#
# Optional PITR env (enables post-load binlog replay; design WS-D §4.13):
#   BLOODRAVEN_PITR_MODE              timestamp or latest. Its presence turns
#                                     replay on.
#   BLOODRAVEN_PITR_STOP_DATETIME     the target, timestamp mode only, in the
#                                     canonical form 2006-01-02T15:04:05.000000Z
#                                     (the operator normalizes the spec).
#   BLOODRAVEN_PITR_ALLOW_PARTIAL     true accepts an archive with no
#                                     transaction at or after the target.
#   BLOODRAVEN_PITR_EXCLUDE_GTIDS     X, never replayed.
#   BLOODRAVEN_PITR_LOCAL_DIR         the staging directory the
#                                     `bloodraven pitr-download` init
#                                     container filled: coverage.json
#                                     (version 2) plus exactly the files it
#                                     lists.
#   BLOODRAVEN_PITR_EXPECTED_SOURCE_UID  the source group's UID, when known.
#   BLOODRAVEN_PITR_VERDICT_PATH      where the success verdict is written;
#                                     defaults to /dev/termination-log.
#   BLOODRAVEN_VERIFY_PITR_MODE       set by verification Jobs, which never
#                                     accept a Partial outcome.
import hashlib
import json
import os
import re
import shutil
import signal
import ssl
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
    require_secure_transport=ON. TLS-enabled jobs require the mounted CA
    to be present and usable so they cannot silently downgrade to an
    encrypted-but-unverified connection. PREFERRED (the client default)
    is kept for non-TLS groups so their behaviour is unchanged.
    """
    if not _bool("BLOODRAVEN_TLS"):
        return "PREFERRED", ""
    ca = (os.environ.get("BLOODRAVEN_TLS_CA_FILE") or "").strip()
    if not ca:
        raise RuntimeError(
            "BLOODRAVEN_TLS=1 requires BLOODRAVEN_TLS_CA_FILE"
        )
    try:
        # Validate before either mysqlsh or the PITR mysql client connects.
        ssl.create_default_context(cafile=ca)
    except (OSError, ssl.SSLError) as exc:
        raise RuntimeError(
            "BLOODRAVEN_TLS_CA_FILE is not a usable CA file: {}".format(exc)
        ) from exc
    return "VERIFY_CA", ca

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


def _read_mysql_cred_file(dirpath, key):
    """Preserve Secret contents, including whitespace and line endings."""
    try:
        with open(os.path.join(dirpath, key), "rb") as f:
            return f.read().decode("utf-8")
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
        user = _read_mysql_cred_file(creds_dir, "MYSQL_USER")
        password = _read_mysql_cred_file(creds_dir, "MYSQL_PASSWORD")
    if user is None:
        user = os.environ.get("MYSQL_USER")
    if password is None:
        password = os.environ.get("MYSQL_PASSWORD")
    if not user:
        print("BLOODRAVEN_LOAD_FAILED: no MYSQL_USER in creds dir or env",
              file=sys.stderr, flush=True)
        sys.exit(2)
    return user, password or ""


def _aws_creds_failed(msg):
    print("BLOODRAVEN_LOAD_FAILED: {}".format(msg), file=sys.stderr, flush=True)
    sys.exit(2)


def _configure_aws_creds_dir():
    """When BLOODRAVEN_AWS_CREDS_DIR is set, read the AWS_* files out of
    it and assemble a standard ~/.aws/credentials file so the AWS SDK
    inside mysqlsh's S3 client picks them up. The reconciler mounts the
    required credentialsSecret here, so a missing key or an unwritable
    credentials file fails the job rather than letting the SDK fall back
    to ambient (instance profile / IRSA) credentials."""
    aws_dir = os.environ.get("BLOODRAVEN_AWS_CREDS_DIR")
    if not aws_dir:
        return
    access_key = _read_cred_file(aws_dir, "AWS_ACCESS_KEY_ID")
    secret_key = _read_cred_file(aws_dir, "AWS_SECRET_ACCESS_KEY")
    session = _read_cred_file(aws_dir, "AWS_SESSION_TOKEN")
    region = _read_cred_file(aws_dir, "AWS_REGION")
    if not access_key or not secret_key:
        _aws_creds_failed(
            "S3 credentials Secret mounted at {} must contain non-empty "
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".format(aws_dir))
    home = os.environ.get("HOME", "/tmp")
    aws_conf_dir = os.path.join(home, ".aws")
    try:
        os.makedirs(aws_conf_dir, exist_ok=True)
    except OSError as e:
        _aws_creds_failed("could not create {}: {}".format(aws_conf_dir, e))
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
    except OSError as e:
        _aws_creds_failed("could not write {}: {}".format(creds_path, e))
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
# PITR: segment replay on top of the loaded dump (design WS-D §4.13)
# --------------------------------------------------------------------
#
# The `bloodraven dump-meta` and `bloodraven pitr-download` init containers
# have already identified the dump and the source archive, planned the
# replay (internal/pitrplan) and staged exactly the files the plan reads,
# with coverage.json version 2 beside them in BLOODRAVEN_PITR_LOCAL_DIR:
#
#   /pitr-binlogs/
#   ├── coverage.json
#   ├── a/mysql-bin.000010.<id>
#   └── b/mysql-bin.000004.<id>
#
# This script trusts none of it. Before the load it re-checks the plan's
# structure, digests and identity (_read_pitr_coverage), and its set algebra
# with the server's own GTID functions (_verify_pitr_plan, R1 and R3-R5).
# After the load it proves the loaded state is the plan's baseline (R2),
# replays every segment with `mysqlbinlog --include-gtids=<segment>` into ONE
# mysql session (_run_pitr_segments), and proves the result is exactly the
# target history minus the exclusions (_verify_pitr_result, R6). Only then
# does it write the verdict the controller certifies the Job from.

_PITR_STOP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")
_ARCHIVE_ID = re.compile(r"^ar1-[a-z2-7]{26}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GTID_SET_CHARS = re.compile(r"^[0-9A-Za-z:,_-]*$")
_GTID_INTERVAL = re.compile(r"^([0-9]+)(?:-([0-9]+))?$")
_MYSQLSH_MYSQLBINLOG = "/usr/libexec/mysqlsh/mysqlbinlog"
_PITR_OUTCOMES = {"timestamp": ("TargetCovered", "Partial"), "latest": ("ArchiveHead",)}
# The verdict must fit the kubelet's 4096-byte termination message together
# with the RESULT line (bounded to 2 KiB of GTIDs) and the optional
# on-history line.
_VERDICT_MAX = 1024
_RESULT_GTIDS_MAX = 2048
_ON_HISTORY_GTIDS_MAX = 768


def _write_termination(message, path=None, limit=3072):
    """Record a compact message for the controller. The kubelet keeps only
    the last 4096 bytes, so the stable reason comes first and stays short."""
    if len(message) > limit:
        message = message[:limit] + " ... (truncated; see container log)"
    try:
        with open(path or os.environ.get("BLOODRAVEN_TERMINATION_LOG", "/dev/termination-log"), "w") as f:
            f.write(message)
    except OSError:
        pass


def _fail(message):
    print(message, file=sys.stderr, flush=True)
    _write_termination(message)
    sys.exit(2)


class _PITRRefusal(Exception):
    """A refusal whose message starts with its token."""


def _gtid_normalize(value):
    return "".join(str(value or "").split())


def _gtid_count(value):
    """The number of GTIDs in a canonical set, tags included
    ("uuid:tag:1-5:7,uuid2:3" counts 7). Only used for reporting."""
    total = 0
    for member in _gtid_normalize(value).split(","):
        if not member:
            continue
        for part in member.split(":")[1:]:
            m = _GTID_INTERVAL.match(part)
            if m is None:
                continue  # a tag
            lo = int(m.group(1))
            hi = int(m.group(2) or lo)
            total += max(0, hi - lo + 1)
    return total


def _sql_text(session, sql, args):
    row = session.run_sql(sql, list(args)).fetch_one()
    return "" if row is None or row[0] is None else _gtid_normalize(row[0])


def _gtid_subtract(session, a, b):
    return _sql_text(session, "SELECT GTID_SUBTRACT(?, ?)", (a, b))


def _gtid_subset(session, a, b):
    row = session.run_sql("SELECT GTID_SUBSET(?, ?)", [a, b]).fetch_one()
    return row is not None and row[0] is not None and int(row[0]) == 1


def _gtid_equal(session, a, b):
    return _gtid_subset(session, a, b) and _gtid_subset(session, b, a)


def _gtid_intersect(session, a, b):
    return _gtid_subtract(session, a, _gtid_subtract(session, a, b))


def _gtid_union(*sets):
    return ",".join(_gtid_normalize(s) for s in sets if _gtid_normalize(s))


def _gtid_executed(session):
    return _sql_text(session, "SELECT @@global.gtid_executed", ())


def _pitr_request():
    """The replay the operator rendered, or None without PITR."""
    mode = os.environ.get("BLOODRAVEN_PITR_MODE", "").strip()
    if not mode:
        return None
    stop = os.environ.get("BLOODRAVEN_PITR_STOP_DATETIME", "").strip()
    if mode not in _PITR_OUTCOMES:
        _fail("PITRCoverageInvalid: unsupported BLOODRAVEN_PITR_MODE {!r}".format(mode))
    if mode == "timestamp" and not _PITR_STOP.match(stop):
        _fail("PITRCoverageInvalid: BLOODRAVEN_PITR_STOP_DATETIME {!r} is not a canonical microsecond UTC time".format(stop))
    if mode == "latest" and stop:
        _fail("PITRCoverageInvalid: latest mode has no stop time, but BLOODRAVEN_PITR_STOP_DATETIME is set")
    return {
        "mode": mode,
        "stop": stop if mode == "timestamp" else None,
        "allowPartial": os.environ.get("BLOODRAVEN_PITR_ALLOW_PARTIAL", "false") == "true",
        "excluded": _gtid_normalize(os.environ.get("BLOODRAVEN_PITR_EXCLUDE_GTIDS", "")),
        "localDir": os.environ.get("BLOODRAVEN_PITR_LOCAL_DIR", "/pitr-binlogs"),
        "expectedSourceUID": os.environ.get("BLOODRAVEN_PITR_EXPECTED_SOURCE_UID", "").strip(),
        "verification": os.environ.get("BLOODRAVEN_VERIFY_PITR_MODE", "none") not in ("", "none"),
    }


def _require(cond, message):
    if not cond:
        raise ValueError(message)


def _gtid_field(obj, key):
    value = obj[key]
    _require(isinstance(value, str), "{} must be a GTID set string".format(key))
    _require(_GTID_SET_CHARS.match(value) is not None, "{} is not a GTID set".format(key))
    return value


def _staged_files(local_dir):
    """Every regular file under local_dir except coverage.json, relative."""
    root = os.path.realpath(local_dir)
    found = set()
    for dirpath, _dirs, names in os.walk(root):
        for name in names:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            if rel == "coverage.json":
                continue
            _require(os.path.isfile(full) and not os.path.islink(full), "staged entry {} is not a regular file".format(rel))
            found.add(rel)
    return found


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_pitr_coverage(req):
    """Load and re-validate coverage.json (§4.13 checks 1-6) without the
    server. Identity disagreements refuse PITRArchiveIdentityMismatch; every
    other failure refuses PITRCoverageInvalid."""
    local_dir = req["localDir"]
    try:
        with open(os.path.join(local_dir, "coverage.json")) as f:
            cov = json.load(f)
        _require(isinstance(cov, dict), "coverage must be an object")
        # 1. version
        _require(type(cov.get("version")) is int and cov["version"] == 2, "unsupported coverage version {!r}".format(cov.get("version")))
        # 3. a refusal is never replayed
        _require(cov.get("refusalReason", "") == "", "coverage records a refusal: {}".format(cov.get("refusalReason")))
        # 2. the request
        _require(cov.get("mode") == req["mode"], "coverage mode {!r} differs from the requested mode {!r}".format(cov.get("mode"), req["mode"]))
        _require(cov.get("stopTime") == req["stop"], "coverage stop time {!r} differs from the requested {!r}".format(cov.get("stopTime"), req["stop"]))
        _require(type(cov.get("allowPartial")) is bool and cov["allowPartial"] == req["allowPartial"], "coverage partial policy differs from the request")
        # 3. the outcome
        outcome = cov.get("outcome")
        _require(outcome in _PITR_OUTCOMES[req["mode"]], "outcome {!r} is not valid in {} mode".format(outcome, req["mode"]))
        if outcome == "Partial":
            _require(req["allowPartial"], "a Partial outcome was not authorized")
            _require(not req["verification"], "a verification does not accept a Partial outcome")
        if outcome == "TargetCovered":
            _require(isinstance(cov.get("witness"), dict) and cov["witness"].get("gtid"), "a TargetCovered plan names no witness")
        for key in ("baseline", "excluded", "lineage", "replay", "postStop", "divergent", "onHistoryExclusions", "lost"):
            _gtid_field(cov, key)
        _require(_gtid_normalize(cov["excluded"]) == req["excluded"], "coverage exclusions differ from BLOODRAVEN_PITR_EXCLUDE_GTIDS")
        # 5. staged files: paths under the staging directory, digests, set
        root = os.path.realpath(local_dir)
        files = cov.get("files")
        _require(isinstance(files, list), "files must be a list")
        listed = set()
        for i, entry in enumerate(files):
            _require(isinstance(entry, dict), "files[{}] must be an object".format(i))
            rel = entry.get("path")
            _require(isinstance(rel, str) and rel and not os.path.isabs(rel), "files[{}].path is invalid".format(i))
            full = os.path.realpath(os.path.join(root, rel))
            _require(os.path.commonpath([root, full]) == root and full != root, "files[{}].path escapes the staging directory".format(i))
            rel = os.path.relpath(full, root)
            _require(rel not in listed, "files[{}].path {} is listed twice".format(i, rel))
            _require(_SHA256.match(str(entry.get("sha256", ""))) is not None, "files[{}].sha256 is invalid".format(i))
            _require(isinstance(entry.get("site"), str) and entry["site"], "files[{}].site is empty".format(i))
            _require(isinstance(entry.get("name"), str) and entry["name"], "files[{}].name is empty".format(i))
            _require(os.path.isfile(full), "staged file {} is missing".format(rel))
            _require(_sha256_file(full) == entry["sha256"], "staged file {} differs from the verified archive object".format(rel))
            entry["_path"] = full
            listed.add(rel)
        staged = _staged_files(local_dir)
        _require(staged == listed, "staged files differ from coverage: unlisted {}, missing {}".format(sorted(staged - listed), sorted(listed - staged)))
        # 6. segments
        segments = cov.get("segments")
        _require(isinstance(segments, list), "segments must be a list")
        transactions = 0
        for k, seg in enumerate(segments):
            _require(isinstance(seg, dict), "segments[{}] must be an object".format(k))
            _require(type(seg.get("file")) is int and 0 <= seg["file"] < len(files), "segments[{}].file is out of range".format(k))
            _require(_gtid_normalize(_gtid_field(seg, "gtids")) != "", "segments[{}] includes no GTID".format(k))
            _gtid_field(seg, "startPredecessors")
            _require(type(seg.get("transactions")) is int and seg["transactions"] > 0, "segments[{}].transactions is invalid".format(k))
            transactions += seg["transactions"]
        _require(type(cov.get("replayedTransactionCount")) is int and cov["replayedTransactionCount"] == transactions,
                 "replayedTransactionCount differs from the segments' transactions")
        sites = cov.get("sites")
        _require(isinstance(sites, list) and all(isinstance(x, str) and x for x in sites), "sites must be a list of names")
        archive, dump = cov.get("archive"), cov.get("dump")
        _require(isinstance(archive, dict) and isinstance(dump, dict), "archive and dump must be objects")
        _require(_ARCHIVE_ID.match(str(archive.get("archiveID", ""))) is not None, "archive.archiveID is malformed")
        _require(archive.get("encryptionMode") in ("None", "Passphrase"), "archive.encryptionMode is invalid")
        source = archive.get("source")
        _require(isinstance(source, dict) and source.get("uid") and source.get("namespace") and source.get("name"), "archive.source is incomplete")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
        _fail("PITRCoverageInvalid: {}".format(e))
    # 4. identity
    if source["uid"] != dump.get("sourceUID"):
        _fail("PITRArchiveIdentityMismatch: the archive was created by group uid {} but the dump was taken from uid {}".format(source["uid"], dump.get("sourceUID")))
    if req["expectedSourceUID"] and source["uid"] != req["expectedSourceUID"]:
        _fail("PITRArchiveIdentityMismatch: the archive was created by group uid {}, but the restored backup's source group is uid {}".format(source["uid"], req["expectedSourceUID"]))
    if dump.get("pitrArchiveID") and dump["pitrArchiveID"] != archive["archiveID"]:
        _fail("PITRArchiveIdentityMismatch: the dump was stamped with archive {} but the plan read archive {}".format(dump["pitrArchiveID"], archive["archiveID"]))
    return cov


def _resolve_mysqlbinlog():
    """D-32: PATH first (mysqlsh puts its libexec on it), then mysqlsh's
    bundled copy; without either, replay is unsupported."""
    found = shutil.which("mysqlbinlog")
    if found:
        return found
    if os.path.isfile(_MYSQLSH_MYSQLBINLOG) and os.access(_MYSQLSH_MYSQLBINLOG, os.X_OK):
        return _MYSQLSH_MYSQLBINLOG
    _fail("PITRReplayUnsupported: no mysqlbinlog on PATH or at {}".format(_MYSQLSH_MYSQLBINLOG))


def _verify_pitr_plan(session, cov):
    """R1 and R3-R5 with the server's GTID functions. They depend only on the
    plan, so they run before the load touches the target; R4 takes E0 as the
    plan's baseline, which R2 proves after the load."""
    baseline, excluded, lineage = cov["baseline"], cov["excluded"], cov["lineage"]
    replay = cov["replay"]
    segments = cov["segments"]
    try:
        # R1: disjoint segments whose union is replay = lineage ∖ baseline ∖ excluded.
        union = ""
        for k, seg in enumerate(segments):
            overlap = _gtid_intersect(session, seg["gtids"], union)
            if overlap:
                raise _PITRRefusal("PITRCoverageInvalid: R1: segment {} repeats {}".format(k, overlap))
            union = _gtid_union(union, seg["gtids"])
        if not _gtid_equal(session, union, replay):
            raise _PITRRefusal("PITRCoverageInvalid: R1: the segments replay {} but the plan's replay set is {}".format(union, replay))
        want = _gtid_subtract(session, _gtid_subtract(session, lineage, baseline), excluded)
        if not _gtid_equal(session, replay, want):
            raise _PITRRefusal("PITRCoverageInvalid: R1: replay {} is not lineage minus baseline minus excluded ({})".format(replay, want))
        # R3
        if not _gtid_subset(session, baseline, lineage):
            raise _PITRRefusal("PITRCoverageInvalid: R3: the baseline holds {}, outside the lineage".format(_gtid_subtract(session, baseline, lineage)))
        common = _gtid_intersect(session, baseline, excluded)
        if common:
            raise _PITRRefusal("PITRExcludeIntersectsBackup: excludeGtids holds {}, which the backup already contains".format(common))
        for key in ("divergent", "lost", "onHistoryExclusions"):
            if not _gtid_subset(session, cov[key], excluded):
                raise _PITRRefusal("PITRCoverageInvalid: R3: {} holds {}, which is not excluded".format(key, _gtid_subtract(session, cov[key], excluded)))
        # R4: every segment starts where the previous one left the server.
        applied = baseline
        for k, seg in enumerate(segments):
            start = _gtid_subtract(session, seg["startPredecessors"], excluded)
            if not _gtid_equal(session, start, applied):
                raise _PITRRefusal("PITRCoverageInvalid: R4: segment {} starts after {} but the server will hold {}".format(k, start, applied))
            applied = _gtid_union(applied, seg["gtids"])
        # R5
        if cov["outcome"] in ("ArchiveHead", "Partial") and not _gtid_normalize(replay):
            raise _PITRRefusal("PITRNoArchivedReplay: nothing is archived beyond the backup")
    except _PITRRefusal as e:
        _fail(str(e))
    except Exception as e:  # noqa: BLE001
        _fail("PITRCoverageInvalid: cannot check the plan with the server's GTID functions: {}".format(e))


def _verify_pitr_baseline(session, cov):
    """R2: the loaded server's gtid_executed is the plan's baseline (E0)."""
    try:
        executed = _gtid_executed(session)
        if not _gtid_equal(session, executed, cov["baseline"]):
            _fail("PITRCoverageInvalid: R2: the loaded dump holds {} but the plan starts from {}".format(executed, cov["baseline"]))
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        _fail("PITRCoverageInvalid: cannot read the loaded dump's gtid_executed: {}".format(e))


def _mysql_replay_command(host, port, user):
    cmd = ["mysql", "--binary-mode", "-h", host, "-P", str(port), "-u", user]
    # The C client defaults to --ssl-mode=PREFERRED; match the mysqlsh
    # session instead of relying on that default.
    ssl_mode, ssl_ca = _tls_options()
    if ssl_mode != "PREFERRED":
        cmd.append("--ssl-mode=" + ssl_mode)
        if ssl_ca:
            cmd.append("--ssl-ca=" + ssl_ca)
    return cmd


def _kill(proc):
    try:
        proc.send_signal(signal.SIGKILL)
    except (OSError, ValueError):
        pass
    try:
        proc.wait()
    except Exception:  # noqa: BLE001
        pass


def _run_pitr_segments(host, port, user, password, cov, mysqlbinlog):
    """Replay every segment, in order, into ONE mysql session. If any
    mysqlbinlog fails, mysql is SIGKILLed before it can commit what it has
    buffered of a partial stream (closing stdin would let it run that)."""
    segments = cov["segments"]
    files = cov["files"]
    print("BLOODRAVEN_PITR_START mode={} stop={} segments={} transactions={}".format(
          cov["mode"], cov.get("stopTime") or "none", len(segments), cov["replayedTransactionCount"]), flush=True)
    if not segments:
        print("BLOODRAVEN_PITR_COMPLETE segments=0", flush=True)
        return
    env = dict(os.environ)
    # MYSQL_PWD keeps the password off argv and procfs.
    env["MYSQL_PWD"] = password
    mysql = subprocess.Popen(_mysql_replay_command(host, port, user), stdin=subprocess.PIPE, env=env)
    for k, seg in enumerate(segments):
        entry = files[seg["file"]]
        print("BLOODRAVEN_PITR_SEGMENT k={} file={} gtids={} transactions={}".format(
              k, _escape_token(entry["site"] + "/" + entry["name"]), _gtid_count(seg["gtids"]), seg["transactions"]), flush=True)
        binlog = subprocess.Popen([mysqlbinlog, "--include-gtids=" + _gtid_normalize(seg["gtids"]), entry["_path"]],
                                  stdout=subprocess.PIPE)
        broken = False
        try:
            shutil.copyfileobj(binlog.stdout, mysql.stdin, 1024 * 1024)
        except (BrokenPipeError, ConnectionResetError, OSError):
            broken = True
        finally:
            binlog.stdout.close()
        if broken:
            _kill(binlog)
            rc = mysql.wait()
            _fail("BLOODRAVEN_PITR_FAILED: mysql exit={} segment={}".format(rc, k))
        rc = binlog.wait()
        if rc != 0:
            _kill(mysql)
            _fail("BLOODRAVEN_PITR_FAILED: mysqlbinlog exit={} segment={}".format(rc, k))
    try:
        mysql.stdin.close()
    except OSError:
        pass
    rc = mysql.wait()
    if rc != 0:
        _fail("BLOODRAVEN_PITR_FAILED: mysql exit={} segment={}".format(rc, len(segments) - 1))
    print("BLOODRAVEN_PITR_COMPLETE segments={}".format(len(segments)), flush=True)


def _verify_pitr_result(session, cov):
    """R6: the server holds exactly the target history minus X, and nothing
    staged after the stop or outside the history. Returns S."""
    try:
        executed = _gtid_executed(session)
        want = _gtid_subtract(session, cov["lineage"], cov["excluded"])
        if not _gtid_equal(session, executed, want):
            raise _PITRRefusal("PITRReplayMismatch: R6: replay left gtid_executed {} but the target history minus excludeGtids is {} (missing {}, extra {})".format(
                executed, want, _gtid_subtract(session, want, executed), _gtid_subtract(session, executed, want)))
        leaked = _gtid_intersect(session, executed, _gtid_union(cov["postStop"], cov["divergent"]))
        if leaked:
            raise _PITRRefusal("PITRReplayMismatch: R6: replay applied {}, which is after the stop or outside the target history".format(leaked))
    except _PITRRefusal as e:
        _fail(str(e))
    except Exception as e:  # noqa: BLE001
        _fail("PITRReplayMismatch: cannot read the replayed gtid_executed: {}".format(e))
    return executed


def _print_pitr_findings(cov):
    if cov["outcome"] == "Partial":
        print("BLOODRAVEN_PITR_PARTIAL requested={} newest={}".format(
              cov.get("stopTime"), cov.get("newestReplayedCommitTime") or "none"), flush=True)
    divergent = _gtid_normalize(cov["divergent"])
    if divergent:
        print("BLOODRAVEN_PITR_DIVERGENT count={}".format(_gtid_count(divergent)), flush=True)
        print("BLOODRAVEN_PITR_DIVERGENT_SET gtids={}".format(divergent[:65536]), flush=True)
    on_history = _gtid_normalize(cov["onHistoryExclusions"])
    if on_history:
        print("BLOODRAVEN_PITR_ON_HISTORY_EXCLUSIONS gtids={}".format(on_history[:65536]), flush=True)


def _bounded(items, limit=8):
    items = list(items)
    if len(items) > limit:
        items = items[:limit] + ["+{}".format(len(items) - limit)]
    return ",".join(items)


def _pitr_verdict_message(cov, executed):
    """The controller certifies a PITR Job only from this message, read from
    the main container's termination message. It is written last, after
    every check and the replay. Line 1 is the verdict (≤ 1 KiB); line 2
    carries S, or only its digest when S is longer than 2 KiB; line 3, when
    present, carries the on-history exclusions."""
    segments, files = cov["segments"], cov["files"]
    used = sorted({seg["file"] for seg in segments})
    last = ""
    if segments:
        entry = files[segments[-1]["file"]]
        last = entry["site"] + "/" + entry["name"]
    archive = cov["archive"]
    executed_sha = hashlib.sha256(executed.encode("utf-8")).hexdigest()

    def verdict(sites):
        return " ".join([
            "BLOODRAVEN_PITR_VERDICT",
            "v=2",
            "mode={}".format(cov["mode"]),
            "stop={}".format(_escape_token(cov.get("stopTime") or "")),
            "outcome={}".format(cov["outcome"]),
            "archive={}".format(archive["archiveID"]),
            "encryptionMode={}".format(archive["encryptionMode"]),
            "sourceUID={}".format(_escape_token(archive["source"]["uid"])),
            "sourceNamespace={}".format(_escape_token(archive["source"]["namespace"])),
            "sourceName={}".format(_escape_token(archive["source"]["name"])),
            "segments={}".format(len(segments)),
            "files={}".format(len(used)),
            "transactions={}".format(int(cov["replayedTransactionCount"])),
            "lastFile={}".format(_escape_token(last)),
            "newest={}".format(_escape_token(cov.get("newestReplayedCommitTime") or "")),
            "divergent={}".format(_gtid_count(cov["divergent"])),
            "onHistoryExcluded={}".format(_gtid_count(cov["onHistoryExclusions"])),
            "executedSha256={}".format(executed_sha),
            "sites={}".format(_escape_token(sites)),
        ])

    line = verdict(_bounded(cov["sites"]))
    if len(line) > _VERDICT_MAX:
        line = verdict("+{}".format(len(cov["sites"])))
    if len(line) > _VERDICT_MAX:
        _fail("PITRCoverageInvalid: the verdict exceeds {} bytes".format(_VERDICT_MAX))
    lines = [line]
    if len(executed) <= _RESULT_GTIDS_MAX:
        lines.append("BLOODRAVEN_PITR_RESULT v=1 executed={}".format(executed))
    else:
        lines.append("BLOODRAVEN_PITR_RESULT v=1 executedSha256={}".format(executed_sha))
    on_history = _gtid_normalize(cov["onHistoryExclusions"])
    if on_history and len(on_history) <= _ON_HISTORY_GTIDS_MAX:
        lines.append("BLOODRAVEN_PITR_ON_HISTORY_EXCLUSIONS gtids={}".format(on_history))
    return "\n".join(lines)


def _write_pitr_verdict(cov, executed):
    message = _pitr_verdict_message(cov, executed)
    print(message.split("\n")[1], flush=True)
    _write_termination(message, os.environ.get("BLOODRAVEN_PITR_VERDICT_PATH"), limit=4000)


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


def _require_offline_mode(session):
    """Exit(2), with nothing dropped, when BLOODRAVEN_REQUIRE_OFFLINE_MODE is
    set and @@GLOBAL.offline_mode is not ON: the operator's application fence
    is missing, so connected applications would race the DROP and load."""
    if not _bool("BLOODRAVEN_REQUIRE_OFFLINE_MODE"):
        return
    try:
        row = session.run_sql("SELECT @@GLOBAL.offline_mode").fetch_one()
        on = bool(row) and str(row[0]).strip().upper() in ("1", "ON", "TRUE")
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_OFFLINE_FENCE_MISSING: cannot read "
              "@@GLOBAL.offline_mode: {}".format(e), file=sys.stderr, flush=True)
        sys.exit(2)
    if not on:
        print("BLOODRAVEN_OFFLINE_FENCE_MISSING: @@GLOBAL.offline_mode is OFF; "
              "refusing to drop schemas while application sessions can connect",
              file=sys.stderr, flush=True)
        sys.exit(2)
    print("BLOODRAVEN_OFFLINE_FENCE_OK", flush=True)


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


def _print_restore_complete(session):
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

    # mysqlsh writes its default `load-progress.<uid>.json` beside the dump:
    # impossible on the read-only local mounts, and unwanted in an S3 backup
    # prefix. Both Job builders set BLOODRAVEN_LOAD_PROGRESS_FILE="" to
    # disable tracking; when the variable is set the caller owns progressFile.
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

    # Every coverage refusal is known before the load: validate it while the
    # target is still untouched.
    pitr = _pitr_request()
    coverage = None
    mysqlbinlog = None
    if pitr is not None:
        coverage = _read_pitr_coverage(pitr)
        if coverage["segments"]:
            mysqlbinlog = _resolve_mysqlbinlog()

    print("BLOODRAVEN_LOAD_START host={} input={}".format(host, input_url),
          flush=True)

    mysqlsh.globals.shell.connect(conn)

    session = mysqlsh.globals.session
    if coverage is not None:
        # The plan's set algebra, before anything destructive runs.
        _verify_pitr_plan(session, coverage)

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
        _require_offline_mode(session)
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
    executed = None
    if coverage is not None:
        _verify_pitr_baseline(session, coverage)
        _print_pitr_findings(coverage)
        _run_pitr_segments(host_only, port, user, password, coverage, mysqlbinlog)
        executed = _verify_pitr_result(session, coverage)

    _print_restore_complete(session)
    if coverage is not None:
        _write_pitr_verdict(coverage, executed)


if __name__ == "__main__":
    main()
