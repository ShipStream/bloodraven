"""Real projected-token TLS client for the deployment coordination scenarios."""
import json
from datetime import datetime, timezone
import os
import ssl
import time
import urllib.error
import urllib.request

base = os.environ["DEPLOY_URL"]
operation = os.environ["OPERATION_ID"]
instance = os.environ["INSTANCE"]
mode = os.environ["MODE"]
ttl = 120 if mode == "emergency" else 30
tls = ssl.create_default_context(cafile="/deploy-ca/ca.crt")
leases = {}
expires = {}


def request(method, path="", body=None):
    with open("/deploy-token/token") as token_file:
        bearer = token_file.read().strip()
    req = urllib.request.Request(
        base + path,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"Authorization": "Bearer " + bearer, "Content-Type": "application/json"},
    )
    try:
        # Emergency promotion can spend 30 seconds draining relay logs.
        response = urllib.request.urlopen(req, context=tls, timeout=45)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


def record(action, kind, status, body):
    # Never emit bearer or lease tokens, even on an unexpected server response.
    safe = {key: body[key] for key in (
        "expiresAt", "topologyGeneration", "error", "reason", "activeSite", "stable"
    ) if key in body}
    print(json.dumps(dict(action=action, kind=kind, status=status, body=safe,
                          at=datetime.now(timezone.utc).isoformat())), flush=True)


status, snapshot = request("GET")
record("snapshot", "", status, snapshot)
if status != 200 or not snapshot.get("stable"):
    raise SystemExit(1)
generation = snapshot["topologyGeneration"]
kinds = ["migration"] if mode == "renew" else ["migration", "failover-hold"]
for kind in kinds:
    status, body = request("POST", "/leases", {
        "kind": kind, "operationId": operation, "instance": instance, "ttlSeconds": ttl,
    })
    record("grant", kind, status, body)
    if (status != 201 or body.get("topologyGeneration") != generation
            or not body.get("token") or not body.get("expiresAt")
            or body.get("kind") != kind or body.get("operationId") != operation):
        raise SystemExit(1)
    leases[kind] = body["token"]
    expires[kind] = datetime.fromisoformat(body["expiresAt"])

if mode == "expiry":
    # Stay alive but deliberately stop heartbeating. No DELETE: only TTL may unblock.
    time.sleep(600)
else:
    next_tick = time.monotonic() + 5
    fenced = set()
    while True:
        time.sleep(max(0, next_tick - time.monotonic()))
        next_tick += 5
        for kind, token in leases.items():
            if kind in fenced:
                continue
            status, body = request("PUT", "/leases/" + kind + "/" + operation,
                                   {"token": token, "ttlSeconds": ttl})
            record("renew", kind, status, body)
            if status == 409:
                if (body.get("error") != "revoked"
                        or body.get("reason") not in ("topology_changed", "operator_revoked")
                        or body.get("topologyGeneration", -1) <= generation):
                    raise SystemExit(1)
                fenced.add(kind)
            elif (status == 423 and body.get("error") == "unstable"
                  and body.get("reason") == "TopologyPersistencePending"):
                # Promotion may wait for relay drain. Poll for fencing, but a
                # rejected renewal never extends the last confirmed lease expiry.
                if datetime.now(timezone.utc) >= expires[kind]:
                    raise SystemExit(1)
            elif status != 200 or body.get("topologyGeneration") != generation:
                raise SystemExit(1)
            else:
                expires[kind] = datetime.fromisoformat(body["expiresAt"])
                # A delayed 200 is not authority if its renewed lease already expired.
                if datetime.now(timezone.utc) >= expires[kind]:
                    raise SystemExit(1)
        if len(fenced) == len(leases):
            print(json.dumps({"action": "stopped", "status": 409}), flush=True)
            break
