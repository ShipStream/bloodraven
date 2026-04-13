#!/usr/bin/env mysqlsh --py -f
# Bloodraven dump script. Executed inside a backup Job running the
# mysqlsh image. All configuration is passed via environment variables
# (mostly paths) plus credential files mounted under
# /run/bloodraven/{mysql,aws}-creds so that plaintext secrets never end
# up in /proc/PID/environ.
#
# Required env:
#   BLOODRAVEN_MYSQL_HOST       host[:port] or [ipv6]:port of the MySQL service
#   BLOODRAVEN_OUTPUT_URL       dump destination (local path or s3 prefix)
#   BLOODRAVEN_DUMP_OPTIONS     JSON object with util.dumpInstance() options
#   BLOODRAVEN_MYSQL_CREDS_DIR  directory with MYSQL_USER / MYSQL_PASSWORD files
#
# Optional env:
#   BLOODRAVEN_TLS=1                  require TLS on the session
#   BLOODRAVEN_S3_BUCKET              bucket name (S3 targets)
#   BLOODRAVEN_S3_ENDPOINT_OVERRIDE   non-AWS endpoint
#   BLOODRAVEN_AWS_CREDS_DIR          directory with AWS_* files (S3 targets)
#   MYSQL_USER / MYSQL_PASSWORD       legacy env-var fallback (not recommended)
import json
import os
import sys

import mysqlsh  # type: ignore


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _host_port(addr, default_port=3306):
    """Split host[:port], tolerating IPv6 literals like '[::1]:3306'."""
    if not addr:
        return addr, default_port
    if addr.startswith("["):
        # IPv6 literal: "[::1]:3306" or "[::1]".
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


def _resolve_credentials():
    """Read MYSQL_USER / MYSQL_PASSWORD from the mounted creds dir, falling
    back to environment variables for backward compatibility. The file
    path wins; this matches the mount layout the reconciler prefers."""
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
        print("BLOODRAVEN_DUMP_FAILED: no MYSQL_USER in creds dir or env",
              file=sys.stderr, flush=True)
        sys.exit(2)
    return user, password or ""


def _configure_aws_creds_dir():
    """When BLOODRAVEN_AWS_CREDS_DIR is set, read the AWS_* files out of
    it and assemble a standard ~/.aws/credentials file so the AWS SDK
    inside mysqlsh's S3 client picks them up. This is the preferred
    path compared to injecting AWS_* env vars."""
    aws_dir = os.environ.get("BLOODRAVEN_AWS_CREDS_DIR")
    if not aws_dir:
        return
    access_key = _read_cred_file(aws_dir, "AWS_ACCESS_KEY_ID")
    secret_key = _read_cred_file(aws_dir, "AWS_SECRET_ACCESS_KEY")
    session = _read_cred_file(aws_dir, "AWS_SESSION_TOKEN")
    region = _read_cred_file(aws_dir, "AWS_REGION")
    if not access_key or not secret_key:
        # Nothing we can do — either the secret is incomplete, or the
        # user is relying on IRSA / instance profile.
        return
    home = os.environ.get("HOME", "/tmp")
    aws_conf_dir = os.path.join(home, ".aws")
    try:
        os.makedirs(aws_conf_dir, exist_ok=True)
    except OSError as e:  # noqa: BLE001
        print("BLOODRAVEN_DUMP_WARN: could not create {}: {}".format(
            aws_conf_dir, e), file=sys.stderr, flush=True)
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
    except OSError as e:  # noqa: BLE001
        print("BLOODRAVEN_DUMP_WARN: could not write {}: {}".format(
            creds_path, e), file=sys.stderr, flush=True)
        return
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = creds_path
    if region and not os.environ.get("AWS_REGION"):
        os.environ["AWS_REGION"] = region


def _capture_gtid_metadata(session):
    """Best-effort capture of GTID + binlog coordinates at dump time.
    Any exception is swallowed so non-GTID or old-MySQL instances don't
    fail the backup. Returns a dict with gtidExecuted, binlogFile,
    binlogPos — any of which may be empty strings / 0."""
    meta = {"gtidExecuted": "", "binlogFile": "", "binlogPos": 0}
    try:
        r = session.run_sql("SELECT @@global.gtid_executed").fetch_one()
        if r and r[0] is not None:
            meta["gtidExecuted"] = str(r[0]).replace("\n", "").replace(" ", "")
    except Exception:  # noqa: BLE001
        pass
    # MySQL 8.4+: SHOW BINARY LOG STATUS. Older: SHOW MASTER STATUS.
    for stmt in ("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"):
        try:
            res = session.run_sql(stmt)
            row = res.fetch_one()
            if row is None:
                continue
            cols = [c.get_column_label() for c in res.get_columns()]
            row_map = dict(zip(cols, list(row)))
            if "File" in row_map and row_map["File"]:
                meta["binlogFile"] = str(row_map["File"])
            if "Position" in row_map and row_map["Position"] is not None:
                try:
                    meta["binlogPos"] = int(row_map["Position"])
                except (TypeError, ValueError):
                    pass
            break
        except Exception:  # noqa: BLE001
            continue
    return meta


def _dump_size_bytes(output, opts):
    """Return the total size in bytes of a local dump output directory.
    Remote (S3) outputs return 0 — we don't walk the bucket. The mysqlsh
    side of the dump utility returns neither a size nor a manifest, so
    the Go parser treats 0 as 'unknown'."""
    if not output or output.startswith("s3://"):
        return 0
    if "s3BucketName" in (opts or {}):
        return 0
    if not os.path.isdir(output):
        return 0
    total = 0
    for root, _dirs, files in os.walk(output):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
    return total


def _human_bytes(n):
    """Binary-unit human-readable byte count matching the Go helper."""
    unit = 1024.0
    if n < unit:
        return "{} B".format(int(n))
    suffixes = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    value = float(n) / unit
    idx = 0
    while value >= unit and idx < len(suffixes) - 1:
        value /= unit
        idx += 1
    return "{:.1f} {}".format(value, suffixes[idx])


def _escape_token(v):
    """Replace spaces with underscores so the whitespace-splitting Go
    parser can round-trip multi-word values (e.g. '1.4 GiB')."""
    if v is None:
        return ""
    return str(v).replace(" ", "_")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------


def main():
    host = os.environ["BLOODRAVEN_MYSQL_HOST"]
    output = os.environ["BLOODRAVEN_OUTPUT_URL"]
    user, password = _resolve_credentials()

    opts = json.loads(os.environ.get("BLOODRAVEN_DUMP_OPTIONS") or "{}")

    # Overlay S3 settings from dedicated env vars so the reconciler does
    # not need to embed the bucket into the shared options JSON.
    bucket = os.environ.get("BLOODRAVEN_S3_BUCKET")
    if bucket:
        opts["s3BucketName"] = bucket
    endpoint = os.environ.get("BLOODRAVEN_S3_ENDPOINT_OVERRIDE")
    if endpoint:
        opts["s3EndpointOverride"] = endpoint

    _configure_aws_creds_dir()

    host_only, port = _host_port(host)
    conn = {
        "host": host_only,
        "port": port,
        "user": user,
        "password": password,
        "ssl-mode": "REQUIRED" if _bool("BLOODRAVEN_TLS") else "PREFERRED",
    }

    print("BLOODRAVEN_DUMP_START host={} output={}".format(host, output),
          flush=True)

    mysqlsh.globals.shell.connect(conn)
    session = mysqlsh.globals.session

    meta = _capture_gtid_metadata(session)

    try:
        mysqlsh.globals.util.dump_instance(output, opts)
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_DUMP_FAILED: {}".format(e), file=sys.stderr,
              flush=True)
        sys.exit(2)

    size_bytes = _dump_size_bytes(output, opts)
    human = _human_bytes(size_bytes) if size_bytes > 0 else ""

    # Deterministic terminal line that the reconciler parses from the
    # Job pod's log tail to populate MysqlBackup.status. Keys with
    # spaces round-trip via an underscore escape so the Go parser can
    # recover them with a split-fields + unescape pair.
    tokens = [
        "location={}".format(_escape_token(output)),
        "sizeBytes={}".format(size_bytes),
        "size={}".format(_escape_token(human)),
        "gtidExecuted={}".format(_escape_token(meta.get("gtidExecuted", ""))),
        "binlogFile={}".format(_escape_token(meta.get("binlogFile", ""))),
        "binlogPos={}".format(int(meta.get("binlogPos", 0) or 0)),
    ]
    print("BLOODRAVEN_DUMP_COMPLETE " + " ".join(tokens), flush=True)


if __name__ == "__main__":
    main()
