"""Acceptance-test evidence and real TCP fault-controller regression tests."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location("recovery_e2e", Path(__file__).with_name("recovery-e2e.py"))
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class RecoveryEvidenceTests(unittest.TestCase):
    def test_negative_probe_retries_only_admission_and_keeps_exact_request(self):
        harness = object.__new__(recovery.Harness)
        harness.gateway_port = 12345
        harness.admin_key = "test"
        def response(code, value):
            return HTTPError("http://test", code, "test", {}, io.BytesIO(json.dumps(value).encode()))
        with patch.object(recovery.urllib.request, "urlopen", side_effect=[
            response(429, {"error": {"code": "org_concurrency_limited"}}),
            response(409, {"error": {"code": "stale_claim"}}),
        ]) as send, patch.object(recovery.time, "sleep"):
            out = harness.api("POST", "/v1/tasks/test/renew", {"fence": "4294967297"}, "token",
                              expected=409, retry=False, retry_admission=True)
            self.assertEqual(out["error"]["code"], "stale_claim")
            self.assertEqual(send.call_count, 2)
            requests = [call.args[0] for call in send.call_args_list]
            self.assertEqual(requests[0].data, requests[1].data)
        for code in (200, 503):
            with self.subTest(code=code), patch.object(recovery.urllib.request, "urlopen", side_effect=response(code, {})) as send:
                with self.assertRaises(AssertionError):
                    harness.api("POST", "/v1/tasks/test/renew", {"fence": "4294967297"}, "token",
                                expected=409, retry=False, retry_admission=True)
                self.assertEqual(send.call_count, 1)

    def test_promotion_compares_numeric_wal_positions(self):
        recovery.require_replayed("F/FFFFFFFF", "10/0")
        recovery.require_replayed("10/0", "10/0")
        with self.assertRaisesRegex(AssertionError, "unsafe promotion"):
            recovery.require_replayed("10/0", "F/FFFFFFFF")
        with self.assertRaisesRegex(AssertionError, "unsafe promotion"):
            recovery.require_replayed("0/1", "")

    def test_independent_ack_ledger_detects_missing_and_corrupt_completion(self):
        expected = {"id": "task", "state": "completed", "result": {"number": 1}, "fence": "12"}
        recovery.validate_acknowledged([expected], [copy.deepcopy(expected)])
        with self.assertRaisesRegex(AssertionError, "lost acknowledged completion"):
            recovery.validate_acknowledged([expected], [])
        for field, value in (("state", "claimed"), ("fence", "11"), ("result", {"number": True})):
            changed = {**expected, field: value}
            with self.subTest(field=field), self.assertRaisesRegex(AssertionError, "mismatch"):
                recovery.validate_acknowledged([expected], [changed])
        with self.assertRaisesRegex(AssertionError, "duplicate"):
            recovery.validate_acknowledged([expected], [expected, expected])

    def test_refuses_reusing_nonempty_evidence_before_owning_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / "postmaster.pid"
            evidence.write_text("123456")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                recovery.Harness(1, directory)
            self.assertEqual(evidence.read_text(), "123456")


class PartitionProxyTests(unittest.TestCase):
    def setUp(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(10)
        self.listener.settimeout(.2)
        self.closed = False
        self.connections = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()
        self.proxy = recovery.TCPProxy(self.listener.getsockname()[1])

    def accept(self):
        while not self.closed:
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connections.append(sock)
            threading.Thread(target=self.echo, args=(sock,), daemon=True).start()

    def echo(self, sock):
        try:
            while data := sock.recv(1024):
                sock.sendall(data)
        except OSError:
            pass
        finally:
            sock.close()

    def tearDown(self):
        self.proxy.close()
        self.closed = True
        self.listener.close()
        for sock in self.connections:
            sock.close()
        self.thread.join(timeout=1)

    def test_partition_drops_open_stream_and_refuses_new_connections_then_heals(self):
        address = ("127.0.0.1", self.proxy.port)
        with socket.create_connection(address, timeout=2) as client:
            client.sendall(b"before")
            self.assertEqual(client.recv(6), b"before")
            self.proxy.partition(True)
            self.assertEqual(client.recv(1), b"")
        with socket.create_connection(address, timeout=2) as client:
            client.sendall(b"during")
            try:
                response = client.recv(10)
            except ConnectionResetError:
                response = b""
            self.assertEqual(response, b"")
        self.proxy.partition(False)
        with socket.create_connection(address, timeout=2) as client:
            client.sendall(b"after")
            self.assertEqual(client.recv(5), b"after")


if __name__ == "__main__":
    unittest.main()
