#!/usr/bin/env mysqlsh --py -f
# Bloodraven restore script. Executed inside a restore Job running the
# mysqlsh image. Loads a previous dump into an empty MySQL instance via
# util.loadDump().
#
# Required env:
#   BLOODRAVEN_MYSQL_HOST       host[:port] or [ipv6]:port of the target
#   BLOODRAVEN_INPUT_URL        dump source (local path or s3 prefix)
#   BLOODRAVEN_LOAD_OPTIONS     JSON object with util.loadDump() options
#   BLOODRAVEN_MYSQL_CREDS_DIR  directory with MYSQL_USER / MYSQL_PASSWORD files
#
# Optional env:
#   BLOODRAVEN_TLS=1                  require TLS on the session
#   BLOODRAVEN_S3_BUCKET              bucket name (S3 sources)
#   BLOODRAVEN_S3_ENDPOINT_OVERRIDE   non-AWS endpoint
#   BLOODRAVEN_AWS_CREDS_DIR          directory with AWS_* files (S3 sources)
#   MYSQL_USER / MYSQL_PASSWORD       legacy env-var fallback (not recommended)
import json
import os
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


if __name__ == "__main__":
    main()
