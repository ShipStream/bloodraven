#!/usr/bin/env mysqlsh --py -f
# Bloodraven restore script. Executed inside a restore Job running the
# community-shell image. Loads a previous dump into an empty MySQL
# instance via util.loadDump().
#
# Required env:
#   BLOODRAVEN_MYSQL_HOST   - host[:port] of the target MySQL service
#   BLOODRAVEN_INPUT_URL    - dump source (local path or s3 prefix)
#   BLOODRAVEN_LOAD_OPTIONS - JSON object with util.loadDump() options
#   MYSQL_USER              - restore user (from derived creds secret)
#   MYSQL_PASSWORD          - restore password
#
# Optional env:
#   BLOODRAVEN_TLS=1                 - enable required TLS on the session
#   BLOODRAVEN_S3_BUCKET             - bucket name (S3 sources)
#   BLOODRAVEN_S3_ENDPOINT_OVERRIDE  - non-AWS endpoint
#   AWS_*                            - forwarded to mysqlsh implicitly
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
    if ":" in addr:
        host, port = addr.rsplit(":", 1)
        return host, int(port)
    return addr, default_port


def main():
    host = os.environ["BLOODRAVEN_MYSQL_HOST"]
    input_url = os.environ["BLOODRAVEN_INPUT_URL"]
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]

    opts = json.loads(os.environ.get("BLOODRAVEN_LOAD_OPTIONS") or "{}")

    bucket = os.environ.get("BLOODRAVEN_S3_BUCKET")
    if bucket:
        opts["s3BucketName"] = bucket
    endpoint = os.environ.get("BLOODRAVEN_S3_ENDPOINT_OVERRIDE")
    if endpoint:
        opts["s3EndpointOverride"] = endpoint

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
