#!/usr/bin/env python3
"""Exercise the real dispatcher against an owned, durable local tool receiver."""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


def validate_receiver_effects(messages, rows):
    """Compare independently persisted effects to original immutable intents."""
    expected = {m["id"]: m for m in messages}
    assert len(expected) == len(messages) == len(rows), "missing or duplicate external effect"
    assert {row[0] for row in rows} == set(expected), "unexpected external effect identity"
    for message_id, key, digest, body in rows:
        message = expected[message_id]
        assert key == message["idempotency_key"], "changed external idempotency key"
        assert hashlib.sha256(body.encode()).hexdigest() == digest, "receiver body digest mismatch"
        envelope = {k: message[k] for k in ("id", "org_id", "task_id", "key", "destination", "payload", "idempotency_key")}
        actual = json.dumps(json.loads(body), sort_keys=True, separators=(",", ":"))
        intended = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        assert actual == intended, "external effect differs from committed intent"


def connect_ledger(directory):
    conn = sqlite3.connect(directory / "receiver.sqlite", timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def receiver(directory, port):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.send_response(200)
            self.end_headers()

        def do_POST(self):
            status, body = 200, b'{"ok":true}'
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size < 1 or size > 300000:
                    raise ValueError("invalid body size")
                raw = self.rfile.read(size)
                data = json.loads(raw)
                message_id = str(uuid.UUID(data["id"]))
                key = data["idempotency_key"]
                assert self.headers.get("Idempotency-Key") == key
                assert key == "swarmpg:" + str(uuid.UUID(data["org_id"])) + ":" + message_id
                assert self.headers.get("Authorization") == "Bearer local-outbox-test"
                assert int(self.headers.get("X-SwarmPG-Delivery-Fence", "0")) > 0
                digest = hashlib.sha256(raw).hexdigest()
                mode = data["payload"].get("mode", "normal")
                with connect_ledger(directory) as db:
                    db.execute("BEGIN IMMEDIATE")
                    previous = db.execute("SELECT digest FROM effects WHERE key=?", (key,)).fetchone()
                    attempts = db.execute("SELECT count(*) FROM requests WHERE key=?", (key,)).fetchone()[0] + 1
                    allowed = db.execute("SELECT 1 FROM allowed WHERE id=?", (message_id,)).fetchone()
                    if previous and previous[0] != digest:
                        status = 409
                    elif mode == "retry" and attempts <= 2:
                        status = 503
                    elif mode == "rate" and attempts == 1:
                        status = 429
                    elif mode == "permanent" and not allowed:
                        status = 422
                    else:
                        db.execute("INSERT OR IGNORE INTO effects VALUES(?,?,?,?,?)", (key, message_id, digest, raw.decode(), time.time()))
                    db.execute("INSERT INTO requests(key,id,digest,status,at) VALUES(?,?,?,?,?)", (key, message_id, digest, status, time.time()))
                # The effect and its deduplication key are durable before this
                # gate. Tests kill the sender/receiver here, without a receipt.
                if mode == "block" and attempts == 1:
                    deadline = time.monotonic() + 40
                    while not (directory / ("release-" + message_id)).exists() and time.monotonic() < deadline:
                        time.sleep(.02)
                if mode == "lost" and attempts == 1:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
            except (AssertionError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                status, body = 400, b'{"error":"invalid delivery"}'
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    class Server(http.server.ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    Server(("127.0.0.1", port), Handler).serve_forever()


class Recovery:
    def __init__(self):
        self.directory = Path(os.environ["COORD_RUN_DIR"])
        self.gateway = "http://127.0.0.1:" + os.environ["COORD_GATEWAY_PORT"]
        self.admin_key = os.environ["ADMIN_KEY"]
        self.children = []
        self.logs = []
        self.receiver_process = None
        self.deliveries = []
        self.fault_until = 0
        self.report = {
            "passed": False, "source_commit": os.environ["COORD_SOURCE_COMMIT"],
            "source_tree_sha256": os.environ["COORD_SOURCE_TREE_SHA256"],
            "postgres": os.environ["COORD_DB_VERSION"], "kernel": os.environ.get("KERNEL") == "1",
            "cases": [], "faults": [], "errors": [], "observer_fault_retries": 0,
            "contract": "at-least-once HTTP delivery; receiver atomically deduplicates stable key with effect",
        }
        self.started = time.monotonic()
        with connect_ledger(self.directory) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE effects(key TEXT PRIMARY KEY,id TEXT NOT NULL UNIQUE,digest TEXT NOT NULL,body TEXT NOT NULL,at REAL NOT NULL);
                CREATE TABLE requests(seq INTEGER PRIMARY KEY,key TEXT NOT NULL,id TEXT NOT NULL,digest TEXT NOT NULL,status INTEGER NOT NULL,at REAL NOT NULL);
                CREATE TABLE allowed(id TEXT PRIMARY KEY);
            """)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.receiver_port = s.getsockname()[1]

    def api(self, method, path, body=None, token=None, status=200):
        headers = {"Content-Type": "application/json"}
        headers["Authorization" if token else "X-Admin-Key"] = "Bearer " + token if token else self.admin_key
        encoded = None if body is None else json.dumps(body).encode()
        # Keep the encoded body stable for the explicitly replayable task
        # operations used to set up fixtures after a database crash. Do not
        # retry organization/agent creation or arbitrary administrative writes.
        retry_safe = method == "GET" or (method == "POST" and (
            path in ("/v1/tasks", "/v1/tasks/claim") or
            (path.startswith("/v1/tasks/") and path.endswith("/complete"))))
        for attempt in range(20):
            request = urllib.request.Request(self.gateway + path, data=encoded, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    code, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, raw = error.code, error.read()
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                if not retry_safe or time.monotonic() >= self.fault_until or attempt == 19:
                    raise
                self.report["observer_fault_retries"] += 1
                time.sleep(.1)
                continue
            value = json.loads(raw)
            pgcode = value.get("error", {}).get("code", "")
            transient = code >= 500 or pgcode in ("57P01", "57P02", "57P03") or pgcode.startswith("08")
            # Only safe reads/replays within the injected service fault. A
            # healthy endpoint's failures remain test failures.
            if retry_safe and transient and time.monotonic() < self.fault_until and attempt < 19:
                self.report["observer_fault_retries"] += 1
                time.sleep(.1)
                continue
            assert code == status, (method, path, code, status, value)
            return value

    def sql(self, statement):
        return subprocess.check_output([
            str(Path(os.environ["PG_BIN_DIR"]) / "psql"), os.environ["DATABASE_URL"],
            "-X", "-v", "ON_ERROR_STOP=1", "-tAc", statement,
        ], text=True).strip()

    def wait(self, predicate, description, timeout=35):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.05)
        raise AssertionError("timed out: " + description)

    def record(self, name):
        self.report["cases"].append({"name": name, "passed": True})
        print("PASS " + name, flush=True)

    def spawn(self, name, argv, env=None):
        log = (self.directory / (name + ".log")).open("ab")
        self.logs.append(log)
        child = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.children.append(child)
        return child

    def start_receiver(self):
        self.receiver_process = self.spawn("receiver", [sys.executable, str(Path(__file__).resolve()), "--receiver", "--directory", str(self.directory), "--port", str(self.receiver_port)])
        def healthy():
            try:
                with urllib.request.urlopen("http://127.0.0.1:" + str(self.receiver_port), timeout=.5):
                    return True
            except (OSError, urllib.error.URLError):
                return False
        self.wait(healthy, "receiver start", 10)

    def start_worker(self, name, destination, concurrency=1):
        env = {**os.environ, "OUTBOX_ADMIN_KEY": self.admin_key, "OUTBOX_TARGET_AUTHORIZATION": "Bearer local-outbox-test"}
        return self.spawn(name, [str(self.directory / "outbox-dispatcher"), "-gateway", self.gateway,
            "-org-id", self.org, "-destination", destination,
            "-endpoint", "http://127.0.0.1:" + str(self.receiver_port),
            "-lease-ms", "2000", "-request-timeout", "2s", "-poll-interval", "50ms",
            "-concurrency", str(concurrency)], env)

    def stop(self, child, crash=False):
        if child.poll() is None:
            child.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)

    def create_agent(self, name):
        org = self.api("POST", "/v1/orgs", {"name": name}, status=201)["id"]
        agent = self.api("POST", "/v1/agents", {"name": "producer", "org_id": org}, status=201)
        return org, agent["id"], agent["token"]

    def create(self, destination, mode="normal", count=1, token=None, org=None, complete=True):
        token, org = token or self.token, org or self.org
        unique = str(uuid.uuid4())
        task = self.api("POST", "/v1/tasks", {"queue": unique, "key": unique, "payload": {"test": unique}}, token)["task"]
        claim = self.api("POST", "/v1/tasks/claim", {"queue": unique, "request_id": str(uuid.uuid4()), "lease_ms": 30000}, token)["task"]
        effects = [{"key": "effect-" + str(i), "destination": destination, "payload": {"mode": mode, "value": i, "fixture": unique}} for i in range(count)]
        body = {"fence": claim["fence"], "result": {"value": 42}, "effects": effects}
        if not complete:
            return task, body
        receipt = self.api("POST", "/v1/tasks/" + task["id"] + "/complete", body, token)
        replay = self.api("POST", "/v1/tasks/" + task["id"] + "/complete", body, token)
        assert not receipt["replayed"] and replay["replayed"]
        messages = self.api("GET", "/v1/tasks/" + task["id"] + "/outbox", token=token)["messages"]
        assert len(messages) == count
        for message in messages:
            assert message["org_id"] == org and message["task_id"] == task["id"]
            assert message["state"] == "pending" and message["attempts"] == 0
        return messages

    def get(self, message):
        return self.api("GET", "/v1/admin/outbox/" + message["id"] + "?org_id=" + message["org_id"])

    def requests(self, message):
        with connect_ledger(self.directory) as db:
            return db.execute("SELECT count(*) FROM requests WHERE id=?", (message["id"],)).fetchone()[0]

    def effect_exists(self, message):
        with connect_ledger(self.directory) as db:
            return bool(db.execute("SELECT 1 FROM effects WHERE id=?", (message["id"],)).fetchone())

    def await_delivery(self, message):
        value = self.wait(lambda: (m if (m := self.get(message))["state"] == "delivered" else None), "delivery " + message["id"])
        self.deliveries.append(message)
        return value

    def admin_claim(self, destination, worker, lease=30000, request=None):
        return self.api("POST", "/v1/admin/outbox/claim", {"org_id": self.org, "destination": destination, "worker_id": worker, "request_id": request or str(uuid.uuid4()), "lease_ms": lease})

    def execute(self):
        self.start_receiver()
        self.org, self.agent, self.token = self.create_agent("outbox-recovery-" + str(uuid.uuid4()))
        foreign_org, _, foreign_token = self.create_agent("foreign-" + str(uuid.uuid4()))

        # A response-budget failure happens after completion/outbox writes; SQL
        # must roll back all of them rather than leave an externally actionable intent.
        task, body = self.create("rollback", complete=False)
        old_budget = self.sql("SELECT max_result_bytes FROM swarmpg.agents WHERE id='" + self.agent + "'")
        self.sql("UPDATE swarmpg.agents SET max_result_bytes=64 WHERE id='" + self.agent + "'")
        try:
            self.api("POST", "/v1/tasks/" + task["id"] + "/complete", body, self.token, status=413)
        finally:
            self.sql("UPDATE swarmpg.agents SET max_result_bytes=" + old_budget + " WHERE id='" + self.agent + "'")
        assert self.sql("SELECT state FROM swarmpg.tasks WHERE id='" + task["id"] + "'") == "claimed"
        assert self.sql("SELECT count(*) FROM swarmpg.outbox_messages WHERE task_id='" + task["id"] + "'") == "0"
        self.record("completion failure rolls back result, history and all outbox intents")

        # Actual SIGKILL between receiver commit and database acknowledgement.
        message = self.create("dispatcher-crash", "block")[0]
        first = self.start_worker("dispatcher-before-crash", "dispatcher-crash")
        self.wait(lambda: self.effect_exists(message), "external effect committed")
        self.stop(first, crash=True)
        assert self.get(message)["state"] != "delivered"
        (self.directory / ("release-" + message["id"])).touch()
        second = self.start_worker("dispatcher-after-crash", "dispatcher-crash")
        final = self.await_delivery(message)
        assert int(final["fence"]) > int(message["fence"]) and self.requests(message) >= 2
        self.stop(second)
        self.report["faults"].append({"kind": "dispatcher_SIGKILL_after_effect", "id": message["id"]})
        self.record("dispatcher crash after external commit retries one stable effect")

        # Kill the external server itself after its SQLite commit; its durable
        # idempotency ledger must survive process restart too.
        message = self.create("receiver-crash", "block")[0]
        worker = self.start_worker("receiver-crash-worker", "receiver-crash")
        self.wait(lambda: self.effect_exists(message), "receiver durable effect")
        self.stop(self.receiver_process, crash=True)
        self.start_receiver()
        self.await_delivery(message)
        assert self.requests(message) >= 2
        self.stop(worker)
        self.report["faults"].append({"kind": "receiver_SIGKILL_after_effect", "id": message["id"]})
        self.record("receiver restart preserves effect deduplication")

        # Real gateway and database failures while delivery is outstanding.
        for service in ("gateway", "database"):
            message = self.create(service + "-crash", "block")[0]
            worker = self.start_worker(service + "-crash-worker", service + "-crash")
            self.wait(lambda: self.effect_exists(message), service + " effect before crash")
            started = time.monotonic()
            self.fault_until = started + 15
            subprocess.run([str(self.directory / "control.sh"), service], check=True, timeout=35)
            (self.directory / ("release-" + message["id"])).touch()
            self.await_delivery(message)
            self.stop(worker)
            self.report["faults"].append({"kind": service + "_crash", "id": message["id"], "recovery_seconds": time.monotonic() - started})
            self.record(service + " crash preserves committed intent and completion")

        for mode in ("lost", "retry", "rate"):
            message = self.create(mode, mode)[0]
            worker = self.start_worker(mode + "-worker", mode)
            self.await_delivery(message)
            assert self.requests(message) >= (3 if mode == "retry" else 2)
            self.stop(worker)
            self.record(mode + " response retries without duplicate effect")

        # Operator retry is a new fenced cycle, never a new idempotency key.
        message = self.create("permanent", "permanent")[0]
        worker = self.start_worker("permanent-worker", "permanent")
        failed = self.wait(lambda: (m if (m := self.get(message))["state"] == "failed" else None), "permanent failure")
        assert not self.effect_exists(message)
        with connect_ledger(self.directory) as db:
            db.execute("INSERT INTO allowed VALUES(?)", (message["id"],))
        retried = self.api("POST", "/v1/admin/outbox/" + message["id"] + "/retry", {"org_id": self.org, "reason": "receiver repaired"})
        assert retried["idempotency_key"] == message["idempotency_key"]
        assert int(retried["fence"]) > int(failed["fence"])
        final = self.await_delivery(message)
        assert final["total_attempts"] >= 2
        self.stop(worker)
        self.record("permanent failure and operator retry preserve identity")

        # An older worker cannot acknowledge or renew a reclaimed delivery.
        message = self.create("stale")[0]
        old, new = str(uuid.uuid4()), str(uuid.uuid4())
        claim = self.admin_claim("stale", old, 100)["message"]
        time.sleep(.16)
        fresh = self.admin_claim("stale", new)["message"]
        assert fresh["id"] == message["id"] and int(fresh["fence"]) > int(claim["fence"])
        for action, extra in (("ack", {"response": {"status": 200}}), ("renew", {"lease_ms": 1000})):
            result = self.api("POST", "/v1/admin/outbox/" + message["id"] + "/" + action,
                {"org_id": self.org, "worker_id": old, "fence": claim["fence"], **extra}, status=409)
            assert result["error"]["code"] == "stale_claim"
        self.api("POST", "/v1/admin/outbox/" + message["id"] + "/nack", {"org_id": self.org, "worker_id": new, "fence": fresh["fence"], "error": "fixture releases valid claim", "retry_ms": 0, "permanent": False})
        worker = self.start_worker("stale-recovery-worker", "stale")
        self.await_delivery(message)
        self.stop(worker)
        self.record("stale delivery owner cannot renew or acknowledge")

        # Replaying an uncertain claim response cannot claim a second message.
        messages = self.create("claim-replay", count=2)
        worker_id, request_id = str(uuid.uuid4()), str(uuid.uuid4())
        claimed = self.admin_claim("claim-replay", worker_id, request=request_id)
        replay = self.admin_claim("claim-replay", worker_id, request=request_id)
        assert replay["replayed"] and claimed["message"] == replay["message"]
        m = claimed["message"]
        self.api("POST", "/v1/admin/outbox/" + m["id"] + "/nack", {"org_id": self.org, "worker_id": worker_id, "fence": m["fence"], "error": "fixture release", "retry_ms": 0, "permanent": False})
        worker = self.start_worker("claim-replay-worker", "claim-replay", 2)
        for m in messages:
            self.await_delivery(m)
        self.stop(worker)
        self.record("claim response replay cannot acquire a second message")

        # Two dispatchers process a batch concurrently. Different tenant and
        # destination intents remain untouched by their scoped configuration.
        foreign = self.create("batch", token=foreign_token, org=foreign_org)[0]
        unconfigured = self.create("unconfigured")[0]
        batch = self.create("batch", mode="block", count=32)
        workers = [self.start_worker("batch-worker-" + str(i), "batch", 4) for i in range(2)]
        history_filter = " FROM swarmpg.outbox_history h JOIN swarmpg.outbox_messages m ON m.id=h.message_id WHERE m.task_id='" + batch[0]["task_id"] + "'"
        self.wait(lambda: int(self.sql("SELECT count(DISTINCT h.worker_id)" + history_filter + " AND h.event='claimed'")) >= 2,
                  "both dispatchers claim concurrently")
        assert all(worker.poll() is None for worker in workers), "batch dispatcher exited early"
        for message in batch:
            (self.directory / ("release-" + message["id"])).touch()
        for message in batch:
            self.await_delivery(message)
        assert all(worker.poll() is None for worker in workers), "batch dispatcher exited during delivery"
        batch_workers = int(self.sql("SELECT count(DISTINCT h.worker_id)" + history_filter + " AND h.event='delivered'"))
        assert batch_workers >= 2, "one dispatcher did all the delivery work"
        self.report["batch_dispatchers_completing"] = batch_workers
        for worker in workers:
            self.stop(worker)
        assert self.get(foreign)["state"] == "pending" and self.requests(foreign) == 0
        assert self.get(unconfigured)["state"] == "pending" and self.requests(unconfigured) == 0
        self.record("concurrent dispatchers deliver each effect once within configured org and destination")

        # Final proof uses the independently persisted receiver ledger and DB
        # history, not successful HTTP responses observed by the sender.
        with connect_ledger(self.directory) as db:
            effects = db.execute("SELECT id,key,digest,body FROM effects").fetchall()
            validate_receiver_effects(self.deliveries, effects)
            changed = db.execute("SELECT key FROM requests GROUP BY key HAVING count(DISTINCT digest)>1").fetchall()
            assert not changed, changed
            self.report["receiver_requests"] = db.execute("SELECT count(*) FROM requests").fetchone()[0]
        for message in self.deliveries:
            final = self.get(message)
            assert final["state"] == "delivered"
            assert all(final[k] == message[k] for k in ("id", "org_id", "task_id", "key", "destination", "payload", "idempotency_key"))
            transitions = self.sql("SELECT count(*) FROM swarmpg.outbox_history WHERE message_id='" + message["id"] + "' AND event='delivered'")
            assert transitions == "1", (message["id"], transitions)
        self.report.update({"verified_effects": len(effects), "duplicate_effects": 0, "changed_retry_bodies": 0, "lost_deliveries": 0, "passed": True})
        self.record("durable receiver and outbox histories agree with one effect per intent")

    def run(self):
        try:
            self.execute()
        except BaseException as error:
            self.report["errors"].append(repr(error))
            raise
        finally:
            for child in reversed(self.children):
                self.stop(child)
            for log in self.logs:
                log.close()
            self.report["elapsed_seconds"] = time.monotonic() - self.started
            (self.directory / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")
            print(json.dumps({k: self.report.get(k) for k in ("passed", "verified_effects", "receiver_requests", "duplicate_effects", "lost_deliveries", "errors")}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--receiver", action="store_true")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    if args.receiver:
        receiver(args.directory, args.port)
    else:
        def terminate(signum, _frame):
            raise SystemExit(128 + signum)
        signal.signal(signal.SIGTERM, terminate)
        Recovery().run()
