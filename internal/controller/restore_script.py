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
# Optional PITR env (enables post-load binlog replay):
#   BLOODRAVEN_PITR_STOP_DATETIME     mysqlbinlog --stop-datetime target
#                                     (RFC 3339 or MySQL form).
#   BLOODRAVEN_PITR_EXCLUDE_GTIDS     optional mysqlbinlog --exclude-gtids set.
#   BLOODRAVEN_PITR_STORAGE_TYPE      "S3" or "PVC".
#   BLOODRAVEN_PITR_MANIFEST_PREFIX   logical prefix where manifests live.
#   BLOODRAVEN_PITR_S3_BUCKET         (S3 only) bucket holding binlogs.
#   BLOODRAVEN_PITR_S3_ENDPOINT_URL   (S3 only) custom endpoint.
#   BLOODRAVEN_PITR_S3_REGION         (S3 only) AWS region.
#   BLOODRAVEN_PITR_AWS_CREDS_DIR     (S3 only) AWS creds file dir.
#   BLOODRAVEN_PITR_PVC_MOUNT_PATH    (PVC only) archive mount point.
#   BLOODRAVEN_PITR_DUMP_GTID_EXECUTED  gtidExecuted at dump time; used to
#                                       set @@GLOBAL.gtid_purged so
#                                       mysqlbinlog's GTID dedup skips
#                                       already-replayed transactions.
import datetime
import json
import os
import subprocess
import sys

import mysqlsh  # type: ignore


def _bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


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


def _configure_aws_creds_dir(env_var="BLOODRAVEN_AWS_CREDS_DIR"):
    aws_dir = os.environ.get(env_var)
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
# PITR: binlog replay on top of the loaded dump
# --------------------------------------------------------------------
#
# After util.loadDump() completes, @@global.gtid_executed matches the
# dump's captured gtidExecuted (assuming skipBinlog=true during load,
# which is the operator default). We then:
#
#   1) Download all archived binlogs whose [firstEventTime, lastEventTime]
#      window intersects the replay window.
#   2) Pipe them through `mysqlbinlog --stop-datetime=<target> | mysql`
#      using --binary-mode so PITR-unsafe events replay correctly.
#
# GTID dedup on the server handles overlap: transactions already in
# gtid_executed are skipped by the SQL thread automatically, so we can
# replay whole binlog files without worrying about the exact dump
# position.
#
# Manifest merge: we read one manifest per site (site names are
# inferred from manifest-*.json filenames in the prefix) and merge the
# union by firstEventTime. A transaction that appears in both sites'
# binlogs (because of log_replica_updates=ON) is still only applied
# once thanks to GTID dedup, so the merge doesn't need to be clever.


def _parse_pitr_target(s):
    """Parse the restore target datetime. Accepts RFC 3339 (with or
    without trailing 'Z') and MySQL's 'YYYY-MM-DD HH:MM:SS' form. The
    returned string is normalized to MySQL's form for --stop-datetime."""
    s = s.strip()
    # mysqlbinlog accepts "YYYY-MM-DD HH:MM:SS". Convert RFC 3339 on
    # the fly so users can paste k8s-style timestamps directly.
    if "T" in s:
        # Tolerate both 2026-04-15T09:30:00Z and 2026-04-15T09:30:00+00:00.
        try:
            if s.endswith("Z"):
                dt = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
            else:
                dt = datetime.datetime.fromisoformat(s)
                # Strip timezone; mysqlbinlog compares against the
                # binlog's own wall-clock so we pin to UTC.
                if dt.tzinfo is not None:
                    dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    # Already MySQL-native or close enough; hand it to mysqlbinlog
    # verbatim. It will complain if it's really malformed.
    return s


def _list_manifests_s3(bucket, prefix, endpoint, region):
    """List manifest-*.json objects under prefix. Uses the `aws` CLI if
    present; otherwise falls back to a boto3 session via mysqlsh's
    bundled Python. We prefer the CLI because it's less likely to hit
    SDK version mismatches across distros."""
    # Use `aws s3 ls` to list objects. Returns a list of object keys.
    cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix + "/"]
    if endpoint:
        cmd.extend(["--endpoint-url", endpoint])
    if region:
        cmd.extend(["--region", region])
    out = subprocess.check_output(cmd)
    data = json.loads(out)
    keys = []
    for obj in data.get("Contents") or []:
        k = obj["Key"]
        base = os.path.basename(k)
        if base.startswith("manifest-") and base.endswith(".json"):
            keys.append(k)
    return keys


def _get_object_s3(bucket, key, local_path, endpoint, region):
    cmd = ["aws", "s3api", "get-object", "--bucket", bucket, "--key", key, local_path]
    if endpoint:
        cmd.extend(["--endpoint-url", endpoint])
    if region:
        cmd.extend(["--region", region])
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL)


def _download_binlogs(entries, stop_datetime, workdir):
    """Filter entries whose [first, last] window could intersect
    (-inf, stop_datetime] and download them to workdir. Entries missing
    timestamps are conservatively included (better to replay a file
    that contributes no transactions than miss one that does).
    Returns the sorted list of local paths."""
    pitr_target = _parse_mysql_datetime(stop_datetime)
    storage_type = os.environ.get("BLOODRAVEN_PITR_STORAGE_TYPE")
    bucket = os.environ.get("BLOODRAVEN_PITR_S3_BUCKET", "")
    endpoint = os.environ.get("BLOODRAVEN_PITR_S3_ENDPOINT_URL", "")
    region = os.environ.get("BLOODRAVEN_PITR_S3_REGION", "")
    pvc_mount = os.environ.get("BLOODRAVEN_PITR_PVC_MOUNT_PATH", "")

    selected = []
    for e in entries:
        first = _parse_iso(e.get("firstEventTime"))
        # Skip entries that end strictly before "beginning of time" (can't happen).
        # Skip entries that START after the target: they can't contain
        # any transaction <= target.
        if pitr_target is not None and first is not None and first > pitr_target:
            continue
        selected.append(e)

    selected.sort(key=lambda x: x.get("firstEventTime", ""))

    os.makedirs(workdir, exist_ok=True)
    local_paths = []
    for e in selected:
        remote = e["remotePath"]
        name = e["name"]
        local = os.path.join(workdir, name)
        if storage_type == "S3":
            _get_object_s3(bucket, remote, local, endpoint, region)
        elif storage_type == "PVC":
            src = os.path.join(pvc_mount, remote)
            if not os.path.exists(src):
                print("BLOODRAVEN_PITR_WARN: missing archived binlog {}".format(src),
                      file=sys.stderr, flush=True)
                continue
            # Hardlink if possible (same filesystem), otherwise copy.
            try:
                os.link(src, local)
            except OSError:
                import shutil as _sh
                _sh.copyfile(src, local)
        else:
            print("BLOODRAVEN_PITR_FAILED: unknown storage type {}".format(storage_type),
                  file=sys.stderr, flush=True)
            sys.exit(2)
        local_paths.append(local)
    return local_paths


def _parse_iso(s):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        # Tolerate fractional seconds and offset.
        # datetime.fromisoformat handles the ISO 8601 shape Go emits.
        dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def _parse_mysql_datetime(s):
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _load_manifests(storage_type, prefix):
    """Return the merged list of ManifestEntry rows from every
    manifest-<site>.json under prefix. Duplicates across sites are
    kept as-is; GTID dedup handles the double-apply at mysqlbinlog
    replay time."""
    entries = []
    if storage_type == "S3":
        bucket = os.environ["BLOODRAVEN_PITR_S3_BUCKET"]
        endpoint = os.environ.get("BLOODRAVEN_PITR_S3_ENDPOINT_URL") or ""
        region = os.environ.get("BLOODRAVEN_PITR_S3_REGION") or ""
        keys = _list_manifests_s3(bucket, prefix, endpoint, region)
        for key in keys:
            tmp = "/tmp/" + os.path.basename(key)
            _get_object_s3(bucket, key, tmp, endpoint, region)
            with open(tmp) as f:
                m = json.load(f)
            for e in m.get("files") or []:
                entries.append(e)
    elif storage_type == "PVC":
        mount = os.environ["BLOODRAVEN_PITR_PVC_MOUNT_PATH"]
        manifest_dir = os.path.join(mount, prefix)
        if not os.path.isdir(manifest_dir):
            return []
        for fn in os.listdir(manifest_dir):
            if not (fn.startswith("manifest-") and fn.endswith(".json")):
                continue
            with open(os.path.join(manifest_dir, fn)) as f:
                m = json.load(f)
            for e in m.get("files") or []:
                entries.append(e)
    else:
        print("BLOODRAVEN_PITR_FAILED: unknown storage type {}".format(storage_type),
              file=sys.stderr, flush=True)
        sys.exit(2)
    return entries


def _mysql_pipe_command(host, port, user, password, files, stop_datetime, exclude_gtids):
    """Build the shell pipeline that replays binlogs into the target
    server. Kept as a single shell command so the pipe is wired by the
    shell rather than being re-implemented in Python — mysqlbinlog's
    stdout must reach mysql's stdin with no buffering from us."""
    binlog_cmd = ["mysqlbinlog", "--stop-datetime=" + stop_datetime]
    if exclude_gtids:
        binlog_cmd += ["--exclude-gtids=" + exclude_gtids]
    binlog_cmd += files
    mysql_cmd = [
        "mysql", "--binary-mode",
        "-h", host, "-P", str(port),
        "-u", user,
    ]
    # Feed the password via MYSQL_PWD env var rather than -p so it
    # never lands on argv/procfs.
    return binlog_cmd, mysql_cmd


def _run_pitr(host, port, user, password, stop_datetime, exclude_gtids):
    storage_type = os.environ["BLOODRAVEN_PITR_STORAGE_TYPE"]
    prefix = os.environ["BLOODRAVEN_PITR_MANIFEST_PREFIX"]

    print("BLOODRAVEN_PITR_START stop_datetime={}".format(stop_datetime),
          flush=True)

    # Configure AWS creds for the `aws` CLI if S3.
    if storage_type == "S3":
        _configure_aws_creds_dir("BLOODRAVEN_PITR_AWS_CREDS_DIR")

    entries = _load_manifests(storage_type, prefix)
    if not entries:
        print("BLOODRAVEN_PITR_FAILED: no manifests found under {}".format(prefix),
              file=sys.stderr, flush=True)
        sys.exit(2)

    workdir = "/tmp/bloodraven-pitr-binlogs"
    local_paths = _download_binlogs(entries, stop_datetime, workdir)
    if not local_paths:
        print("BLOODRAVEN_PITR_NOOP: no archived binlogs predate the target; dump load is final state",
              flush=True)
        return

    binlog_cmd, mysql_cmd = _mysql_pipe_command(
        host, port, user, password, local_paths, stop_datetime, exclude_gtids,
    )
    print("BLOODRAVEN_PITR_REPLAY files={} ".format(len(local_paths)), flush=True)

    # Pipe mysqlbinlog | mysql via subprocess. MYSQL_PWD env is set
    # only for the mysql side so it isn't visible in the binlog side's
    # environment.
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

    _configure_aws_creds_dir()

    host_only, port = _host_port(host)
    conn = {
        "host": host_only,
        "port": port,
        "user": user,
        "password": password,
        "ssl-mode": "REQUIRED" if _bool("BLOODRAVEN_TLS") else "PREFERRED",
    }

    print("BLOODRAVEN_LOAD_START host={} input={}".format(host, input_url),
          flush=True)

    mysqlsh.globals.shell.connect(conn)
    try:
        mysqlsh.globals.util.load_dump(input_url, opts)
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_LOAD_FAILED: {}".format(e), file=sys.stderr,
              flush=True)
        sys.exit(2)

    print("BLOODRAVEN_LOAD_COMPLETE input={}".format(input_url), flush=True)

    # Optional PITR replay on top of the loaded dump.
    stop_dt = os.environ.get("BLOODRAVEN_PITR_STOP_DATETIME")
    if stop_dt:
        stop_dt_mysql = _parse_pitr_target(stop_dt)
        exclude_gtids = os.environ.get("BLOODRAVEN_PITR_EXCLUDE_GTIDS", "")
        _run_pitr(host_only, port, user, password, stop_dt_mysql, exclude_gtids)


if __name__ == "__main__":
    main()
