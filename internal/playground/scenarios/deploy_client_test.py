"""Protocol and stop-on-fence tests without Kubernetes or third-party packages."""
import contextlib
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import runpy
import unittest
from unittest.mock import mock_open, patch
from urllib.error import HTTPError


class DeploymentClientTest(unittest.TestCase):
    def run_client(self, responses, mode="renew"):
        calls = []
        output = io.StringIO()
        tls = object()
        clock = [0]

        class ClockDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return (datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
                        + timedelta(seconds=clock[0])).astimezone(tz)

        def advance(seconds):
            clock[0] += seconds

        def respond(req, context, timeout):
            self.assertIs(context, tls)
            self.assertEqual(timeout, 45)
            self.assertEqual(req.headers["Authorization"], "Bearer projected-secret")
            self.assertTrue(req.full_url.startswith("https://operator:8443/deploy/v1/groups/group"))
            calls.append((req.method, req.full_url, json.loads(req.data) if req.data else None))
            reply = responses.pop(0)
            status, body = reply[:2]
            if len(reply) == 3:
                delay = reply[2]
                self.assertLess(delay, timeout)
                advance(delay)
            stream = io.BytesIO(json.dumps(body).encode())
            if status >= 400:
                raise HTTPError(req.full_url, status, "test response", {}, stream)
            stream.status = status
            return stream

        env = {"DEPLOY_URL": "https://operator:8443/deploy/v1/groups/group",
               "OPERATION_ID": "attempt", "INSTANCE": "tenant", "MODE": mode}
        code = 0
        with patch.dict("os.environ", env), patch("ssl.create_default_context", return_value=tls) as ca:
            with patch("urllib.request.urlopen", side_effect=respond), patch("time.sleep", side_effect=advance) as sleep:
                with (patch("builtins.open", mock_open(read_data="projected-secret")),
                      patch("time.monotonic", side_effect=lambda: clock[0]),
                      patch("datetime.datetime", ClockDatetime), contextlib.redirect_stdout(output)):
                    try:
                        runpy.run_path(str(Path(__file__).with_name("deploy_client.py")))
                    except SystemExit as error:
                        code = error.code
            ca.assert_called_once_with(cafile="/deploy-ca/ca.crt")
        text = output.getvalue()
        self.assertNotIn("projected-secret", text)
        self.assertNotIn("lease-secret", text)
        return code, calls, [json.loads(line) for line in text.splitlines()], sleep

    @staticmethod
    def snapshot():
        return 200, {"stable": True, "topologyGeneration": 7, "activeSite": "iad"}

    @staticmethod
    def grant(kind="migration", ttl=30):
        expires = datetime(2026, 9, 9, 12, tzinfo=timezone.utc) + timedelta(seconds=ttl)
        return 201, {"kind": kind, "operationId": "attempt", "token": "lease-secret",
                     "expiresAt": expires.isoformat(), "topologyGeneration": 7}

    @staticmethod
    def revoked(generation=8):
        return 409, {"error": "revoked", "reason": "topology_changed", "topologyGeneration": generation}

    def test_renewal_and_409_stop(self):
        responses = [self.snapshot(), self.grant(),
                     (200, {"expiresAt": "2026-09-09T12:00:35Z", "topologyGeneration": 7}), self.revoked()]
        code, calls, records, sleep = self.run_client(responses)
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[1][2], {"kind": "migration", "operationId": "attempt", "instance": "tenant", "ttlSeconds": 30})
        self.assertEqual(calls[2][1], "https://operator:8443/deploy/v1/groups/group/leases/migration/attempt")
        self.assertEqual(calls[2][2], {"token": "lease-secret", "ttlSeconds": 30})
        self.assertEqual(records[-1]["action"], "stopped")
        self.assertAlmostEqual(sleep.call_args_list[0].args[0], 5, delta=0.1)
        self.assertFalse(responses)

    def test_emergency_probes_both_revoked_leases(self):
        responses = [self.snapshot(), self.grant(ttl=120), self.grant("failover-hold", ttl=120), self.revoked(), self.revoked()]
        code, calls, records, _ = self.run_client(responses, mode="emergency")
        self.assertEqual(code, 0)
        self.assertTrue(calls[-1][1].endswith("/leases/failover-hold/attempt"))
        self.assertEqual([r["kind"] for r in records if r.get("status") == 409 and r["action"] == "renew"], ["migration", "failover-hold"])
        self.assertEqual(records[-1]["action"], "stopped")
        self.assertTrue(all(call[2]["ttlSeconds"] == 120 for call in calls[1:]))

    def test_emergency_pending_drain_then_both_409(self):
        pending = (423, {"error": "unstable", "reason": "TopologyPersistencePending"})
        responses = [self.snapshot(), self.grant(ttl=120), self.grant("failover-hold", ttl=120)]
        responses += [pending, pending] * 6 + [self.revoked(), self.revoked()]
        code, calls, records, sleep = self.run_client(responses, mode="emergency")
        self.assertEqual(code, 0)
        self.assertFalse(responses)
        self.assertEqual([r["status"] for r in records if r["action"] == "renew"], [423] * 12 + [409, 409])
        revoked = [r for r in records if r["action"] == "renew" and r["status"] == 409]
        self.assertEqual([r["kind"] for r in revoked], ["migration", "failover-hold"])
        self.assertEqual(datetime.fromisoformat(revoked[0]["at"]).second, 35)
        self.assertTrue(all(call[2]["ttlSeconds"] == 120 for call in calls[1:]))
        self.assertTrue(all(call.args == (5,) for call in sleep.call_args_list))
        self.assertEqual(records[-1]["action"], "stopped")

    def test_partial_fence_still_consumes_other_lease_verdict(self):
        pending = (423, {"error": "unstable", "reason": "TopologyPersistencePending"})
        responses = [self.snapshot(), self.grant(ttl=120), self.grant("failover-hold", ttl=120),
                     self.revoked(), pending, self.revoked()]
        code, calls, records, _ = self.run_client(responses, mode="emergency")
        self.assertEqual(code, 0)
        self.assertFalse(responses)
        self.assertTrue(calls[-1][1].endswith("/leases/failover-hold/attempt"))
        self.assertEqual(records[-1]["action"], "stopped")

    def test_slow_emergency_verdict_preserves_expiry_and_stops(self):
        responses = [self.snapshot(), self.grant(ttl=120), self.grant("failover-hold", ttl=120),
                     (*self.revoked(), 35), self.revoked()]
        code, calls, records, _ = self.run_client(responses, mode="emergency")
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 5)
        revoked = [r for r in records if r["action"] == "renew"]
        self.assertEqual([r["status"] for r in revoked], [409, 409])
        self.assertEqual(datetime.fromisoformat(revoked[0]["at"]).second, 40)
        self.assertEqual(records[-1]["action"], "stopped")

    def test_pending_does_not_extend_confirmed_expiry(self):
        pending = (423, {"error": "unstable", "reason": "TopologyPersistencePending"})
        responses = [self.snapshot(), self.grant(ttl=120), self.grant("failover-hold", ttl=120)]
        responses += [pending, pending] * 24
        code, _, records, sleep = self.run_client(responses, mode="emergency")
        self.assertEqual(code, 1)
        self.assertEqual(sum(call.args[0] for call in sleep.call_args_list), 120)
        self.assertNotIn("stopped", [r["action"] for r in records])
        self.assertNotIn(200, [r["status"] for r in records if r["action"] == "renew"])

    def test_delayed_successful_renewal_must_still_be_unexpired(self):
        for delay, valid in [(29, True), (30, False), (35, False)]:
            with self.subTest(delay=delay):
                responses = [self.snapshot(), self.grant(),
                             (200, {"expiresAt": "2026-09-09T12:00:35Z", "topologyGeneration": 7}, delay)]
                if valid:
                    responses.append(self.revoked())
                code, calls, records, _ = self.run_client(responses)
                self.assertEqual(code, 0 if valid else 1)
                self.assertEqual(len(calls), 4 if valid else 3)
                self.assertEqual(records[-1]["action"] == "stopped", valid)
                self.assertFalse(responses)

    def test_expiry_never_renews_or_releases(self):
        code, calls, _, sleep = self.run_client([self.snapshot(), self.grant(), self.grant("failover-hold")], mode="expiry")
        self.assertEqual(code, 0)
        self.assertEqual([call[0] for call in calls], ["GET", "POST", "POST"])
        sleep.assert_called_once_with(600)

    def test_bad_renewal_is_not_accepted_as_topology_fence(self):
        for reply in [(404, {"error": "not_found"}), self.revoked(generation=7),
                      (200, {"topologyGeneration": 8}), (403, {"error": "token_mismatch"}),
                      (423, {"error": "unstable", "reason": "OtherReason"})]:
            with self.subTest(reply=reply):
                code, _, records, _ = self.run_client([self.snapshot(), self.grant(), reply])
                self.assertEqual(code, 1)
                self.assertNotIn("stopped", [record["action"] for record in records])

    def test_unstable_snapshot_does_not_acquire(self):
        code, calls, _, _ = self.run_client([(200, {"stable": False})])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
