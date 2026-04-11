#!/usr/bin/env mysqlsh --py -f
# Bloodraven artifact cleanup script. Executed inside the finalizer's
# cleanup Job when a MysqlBackup CR is being deleted. Dispatches on the
# BLOODRAVEN_STORAGE_TYPE env var and deletes the underlying dump
# artifact (S3 prefix or PVC subdirectory) before the CR is released.
#
# Required env:
#   BLOODRAVEN_STORAGE_TYPE   "S3" or "PVC"
#   BLOODRAVEN_OUTPUT_URL     the resolved dump location from MysqlBackup.status.location
#
# S3 path (BLOODRAVEN_STORAGE_TYPE=S3):
#   BLOODRAVEN_S3_BUCKET              bucket name
#   BLOODRAVEN_S3_ENDPOINT_OVERRIDE   optional non-AWS endpoint
#   BLOODRAVEN_AWS_CREDS_DIR          creds dir mounted by the reconciler
#
# PVC path (BLOODRAVEN_STORAGE_TYPE=PVC):
#   BLOODRAVEN_PVC_MOUNT_PATH   usually "/backups"
#
# Exit codes:
#   0 = done (may be a no-op if the artifact is already gone)
#   2 = hard config error — finalizer should give up
#   3 = transient — finalizer should retry
import os
import shutil
import sys

import mysqlsh  # type: ignore


def _read_cred_file(dirpath, key):
    path = os.path.join(dirpath, key)
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except OSError:
        return None


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


def _is_already_gone(err):
    msg = str(err).lower()
    for tok in ("not found", "does not exist", "no such key", "nosuchkey"):
        if tok in msg:
            return True
    return False


def _cleanup_s3(prefix):
    bucket = os.environ.get("BLOODRAVEN_S3_BUCKET")
    if not bucket:
        print("BLOODRAVEN_CLEANUP_FAILED: S3 cleanup requires BLOODRAVEN_S3_BUCKET",
              file=sys.stderr, flush=True)
        return 2
    if not prefix:
        print("BLOODRAVEN_CLEANUP_FAILED: empty prefix for S3 cleanup",
              file=sys.stderr, flush=True)
        return 2

    _configure_aws_creds_dir()

    opts = {"s3BucketName": bucket}
    endpoint = os.environ.get("BLOODRAVEN_S3_ENDPOINT_OVERRIDE")
    if endpoint:
        opts["s3EndpointOverride"] = endpoint

    util = mysqlsh.globals.util
    rmdump = getattr(util, "rmdump", None)
    if rmdump is None:
        # Pre-8.0.32 mysqlsh doesn't expose util.rmdump. There is no
        # reasonable polyfill (we would need to shell out to aws-cli),
        # so fail loudly and let the operator remove the finalizer by
        # hand to force-delete the CR.
        print("BLOODRAVEN_CLEANUP_FAILED: util.rmdump is unavailable in this "
              "mysqlsh build; upgrade to 8.0.32+ or remove the finalizer manually",
              file=sys.stderr, flush=True)
        return 2

    try:
        rmdump(prefix, opts)
    except Exception as e:  # noqa: BLE001
        if _is_already_gone(e):
            print("BLOODRAVEN_CLEANUP_SKIPPED prefix={} reason=already_gone".format(prefix),
                  flush=True)
            return 0
        print("BLOODRAVEN_CLEANUP_FAILED: {}".format(e), file=sys.stderr,
              flush=True)
        return 3

    print("BLOODRAVEN_CLEANUP_COMPLETE prefix={}".format(prefix), flush=True)
    return 0


def _cleanup_pvc(output):
    mount = os.environ.get("BLOODRAVEN_PVC_MOUNT_PATH", "/backups")
    if not output:
        print("BLOODRAVEN_CLEANUP_FAILED: empty path for PVC cleanup",
              file=sys.stderr, flush=True)
        return 2

    # Refuse to delete the mount root itself or anything outside it.
    real_mount = os.path.realpath(mount)
    target = output if os.path.isabs(output) else os.path.join(real_mount, output)
    real_target = os.path.realpath(target)

    if real_target == real_mount or not real_target.startswith(real_mount + os.sep):
        print("BLOODRAVEN_CLEANUP_FAILED: refusing to delete {!r} "
              "(outside mount {!r})".format(real_target, real_mount),
              file=sys.stderr, flush=True)
        return 2

    if not os.path.exists(real_target):
        print("BLOODRAVEN_CLEANUP_SKIPPED path={} reason=already_gone".format(real_target),
              flush=True)
        return 0

    try:
        if os.path.isdir(real_target):
            shutil.rmtree(real_target)
        else:
            os.remove(real_target)
    except OSError as e:
        print("BLOODRAVEN_CLEANUP_FAILED: {}".format(e), file=sys.stderr,
              flush=True)
        return 3

    print("BLOODRAVEN_CLEANUP_COMPLETE path={}".format(real_target), flush=True)
    return 0


def main():
    storage = (os.environ.get("BLOODRAVEN_STORAGE_TYPE") or "").strip()
    output = os.environ.get("BLOODRAVEN_OUTPUT_URL") or ""

    print("BLOODRAVEN_CLEANUP_START type={} output={}".format(storage, output),
          flush=True)

    if storage == "S3":
        code = _cleanup_s3(output)
    elif storage == "PVC":
        code = _cleanup_pvc(output)
    else:
        print("BLOODRAVEN_CLEANUP_FAILED: unknown storage type {!r}".format(storage),
              file=sys.stderr, flush=True)
        code = 2

    sys.exit(code)


if __name__ == "__main__":
    main()
