#!/usr/bin/env mysqlsh --py -f
# Bloodraven dump script. Executed inside a backup Job running the
# community-shell image. All configuration is passed via environment
# variables so that the same static script file can be reused across
# every MysqlBackup CR.
#
# Required env:
#   BLOODRAVEN_MYSQL_HOST   - host[:port] of the MySQL service
#   BLOODRAVEN_OUTPUT_URL   - dump destination (local path or s3 prefix)
#   BLOODRAVEN_DUMP_OPTIONS - JSON object with util.dumpInstance() options
#   MYSQL_USER              - backup user (from derived creds secret)
#   MYSQL_PASSWORD          - backup password
#
# Optional env:
#   BLOODRAVEN_TLS=1                 - enable required TLS on the session
#   BLOODRAVEN_S3_BUCKET             - bucket name (S3 targets)
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
    output = os.environ["BLOODRAVEN_OUTPUT_URL"]
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]

    opts = json.loads(os.environ.get("BLOODRAVEN_DUMP_OPTIONS") or "{}")

    # Overlay S3 settings from dedicated env vars so the reconciler does
    # not need to embed the bucket into the shared options JSON.
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

    print("BLOODRAVEN_DUMP_START host={} output={}".format(host, output),
          flush=True)

    mysqlsh.globals.shell.connect(conn)
    try:
        mysqlsh.globals.util.dump_instance(output, opts)
    except Exception as e:  # noqa: BLE001
        print("BLOODRAVEN_DUMP_FAILED: {}".format(e), file=sys.stderr,
              flush=True)
        sys.exit(2)

    # Deterministic terminal line that the reconciler parses from the
    # Job pod's log tail to populate MysqlBackup.status.location/size.
    print("BLOODRAVEN_DUMP_COMPLETE location={}".format(output), flush=True)


if __name__ == "__main__":
    main()
