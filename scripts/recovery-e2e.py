#!/usr/bin/env python3
"""Owned-service replication, failover and PITR acceptance test.

Never connects to an existing database. Every database directory, process and
TCP fault proxy is created by this invocation; evidence survives cleanup.
"""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import select
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]


def lsn_number(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def require_replayed(required, actual):
    if not actual or lsn_number(actual) < lsn_number(required):
        raise AssertionError("unsafe promotion: standby replay " + str(actual) + " is below required " + required)


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError("timed out: " + description)


def source_fingerprint():
    paths = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z", "-c", "-o", "--exclude-standard"]).split(b"\0")
    digest = hashlib.sha256()
    for name in sorted(set(p for p in paths if p)):
        path = ROOT / name.decode()
        if path.is_file():
            digest.update(name + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TCPProxy:
    """A real TCP partition: closes existing streams and refuses new streams."""
    def __init__(self, target_port):
        self.target_port = target_port
        self.enabled = True
        self.closed = False
        self.lock = threading.Lock()
        self.connections = set()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(100)
        self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while not self.closed:
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                enabled, target = self.enabled, self.target_port
            if not enabled:
                client.close()
                continue
            threading.Thread(target=self._relay, args=(client, target), daemon=True).start()

    def _relay(self, client, target):
        upstream = None
        pair = (client,)
        try:
            upstream = socket.create_connection(("127.0.0.1", target), timeout=2)
            upstream.settimeout(None)
            pair = (client, upstream)
            with self.lock:
                if not self.enabled or self.closed or target != self.target_port:
                    return
                self.connections.update(pair)
            while not self.closed:
                readable, _, _ = select.select(pair, [], [], .2)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    (upstream if source is client else client).sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            with self.lock:
                self.connections.difference_update(pair)
            for sock in pair:
                sock.close()

    def partition(self, enabled):
        with self.lock:
            self.enabled = not enabled
            peers = list(self.connections)
        for sock in peers:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def redirect(self, port):
        self.partition(True)
        with self.lock:
            self.target_port = port
        self.partition(False)

    def close(self):
        self.closed = True
        self.partition(True)
        self.listener.close()
        self.thread.join(timeout=2)


class OwnedPostgres:
    def __init__(self, owner, name, directory=None):
        self.owner, self.name = owner, name
        self.directory = directory or owner.directory / name
        self.port = unused_port()
        self.started = False
        self.log = owner.directory / (name + ".postgres.log")

    def command(self, program, *args, timeout=30, capture=True):
        return self.owner.command(str(self.owner.pg_bin / program), *args, timeout=timeout, capture=capture)

    def initialize(self):
        self.command("initdb", "-D", str(self.directory), "-U", "recovery_admin", "--auth=trust", "--no-locale", "--encoding=UTF8", timeout=60)
        self.configure()

    def configure(self):
        # Paths are absolute, generated by this harness, and escaped as SQL
        # strings. Archive and restore scripts avoid shell-expanded filenames.
        config = {
            "port": str(self.port), "listen_addresses": "'127.0.0.1'",
            "unix_socket_directories": "''", "max_connections": "180",
            "shared_buffers": "'128MB'", "wal_level": "'replica'",
            "max_wal_senders": "10", "max_replication_slots": "10",
            "hot_standby": "on", "wal_keep_size": "'128MB'",
            "archive_mode": "on", "archive_timeout": "'5s'",
            "synchronous_commit": "on", "wal_receiver_status_interval": "'100ms'",
            "wal_retrieve_retry_interval": "'100ms'",
            "archive_command": self.pg_quote(shlex.quote(str(self.owner.archive_command)) + " '%p' '%f'"),
        }
        if self.owner.kernel:
            config["agent_principal_secret"] = "'recovery-test-principal-secret'"
        with (self.directory / "postgresql.conf").open("a") as handle:
            handle.write("\n# Owned recovery test configuration\n")
            for key, value in config.items():
                handle.write(key + " = " + value + "\n")

    @staticmethod
    def pg_quote(value):
        return "'" + value.replace("'", "''") + "'"

    def start(self):
        self.command("pg_ctl", "-D", str(self.directory), "-l", str(self.log), "-w", "-t", "40", "start", timeout=45)
        self.started = True

    def stop(self, crash=False):
        # pg_ctl can time out after starting a postmaster (for example a failed
        # recovery target). The directory is owned even if start() raised.
        if (self.directory / "postmaster.pid").exists():
            self.command("pg_ctl", "-D", str(self.directory), "-m", "immediate" if crash else "fast", "-w", "stop", timeout=35)
        self.started = False

    def url(self, database="recovery_test"):
        return "postgres://recovery_admin@127.0.0.1:" + str(self.port) + "/" + database + "?sslmode=disable&connect_timeout=2"

    def sql(self, statement, database="recovery_test", timeout=15):
        return self.command("psql", self.url(database), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-tAc", statement, timeout=timeout).strip()

    def reload(self):
        self.command("pg_ctl", "-D", str(self.directory), "reload")

    def setting(self, key, value):
        self.sql("ALTER SYSTEM SET " + key + " = " + self.pg_quote(value))
        self.reload()

    def identity(self):
        return self.sql("SELECT system_identifier::text FROM pg_control_system()"), int(self.sql("SELECT timeline_id FROM pg_control_checkpoint()"))

    def backup(self, destination, replication_port=None):
        self.command("pg_basebackup", "-h", "127.0.0.1", "-p", str(replication_port or self.port), "-U", "recovery_admin",
                     "-D", str(destination), "-X", "stream", "-c", "fast", timeout=90)


def ledger_connect(directory):
    db = sqlite3.connect(directory / "evidence.sqlite", timeout=30)
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA synchronous=FULL")
    return db


def validate_acknowledged(acknowledged, persisted):
    """An independent HTTP-success ledger must match actual recovered rows."""
    actual = {item["id"]: item for item in persisted}
    if len(actual) != len(persisted):
        raise AssertionError("duplicate persisted task identity")
    for expected in acknowledged:
        item = actual.get(expected["id"])
        if item is None:
            raise AssertionError("lost acknowledged completion: " + expected["id"])
        for key in ("state", "result", "fence"):
            if json.dumps(item[key], sort_keys=True) != json.dumps(expected[key], sort_keys=True):
                raise AssertionError("recovered completion mismatch: " + key)


class Receiver:
    def __init__(self, directory):
        self.directory = directory
        self.blocked_effect = threading.Event()
        self.release = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                body = json.loads(raw)
                key = self.headers.get("Idempotency-Key")
                digest = hashlib.sha256(raw).hexdigest()
                status = 200
                with ledger_connect(owner.directory) as db:
                    db.execute("BEGIN IMMEDIATE")
                    previous = db.execute("SELECT digest FROM effects WHERE key=?", (key,)).fetchone()
                    attempts = db.execute("SELECT count(*) FROM deliveries WHERE key=?", (key,)).fetchone()[0]
                    if key != body.get("idempotency_key") or previous and previous[0] != digest:
                        status = 409
                    else:
                        db.execute("INSERT OR IGNORE INTO effects VALUES(?,?,?,?)", (key, body["id"], digest, raw.decode()))
                    db.execute("INSERT INTO deliveries(key,digest,status) VALUES(?,?,?)", (key, digest, status))
                if body.get("payload", {}).get("mode") == "hold-before-ack" and attempts == 0:
                    owner.blocked_effect.set()
                    owner.release.wait(timeout=90)
                    # Lose the response even after the externally durable effect.
                    self.close_connection = True
                    return
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", "11")
                    self.end_headers()
                    self.wfile.write(b'{"ok":true}')
                except OSError:
                    pass

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()


class Harness:
    def __init__(self, duration, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        if any(self.directory.iterdir()):
            raise ValueError("RECOVERY_REPORT_DIR must be empty; evidence and existing services are never reused")
        self.duration = duration
        self.worker_count = int(os.environ.get("RECOVERY_WORKERS", "32"))
        if not 1 <= self.worker_count <= 256:
            raise ValueError("RECOVERY_WORKERS must be between 1 and 256")
        self.kernel = os.environ.get("KERNEL") == "1"
        default_bin = ROOT / ".pginstall/bin" if self.kernel else Path(shutil.which("psql") or "/missing/psql").parent
        self.pg_bin = Path(os.environ.get("PG_BIN_DIR", default_bin)).resolve()
        self.nodes, self.proxies, self.children, self.logs = [], [], [], []
        self.gateway = None
        self.dispatcher = None
        self.receiver = None
        self.workload_stop = threading.Event()
        self.workload_pause = threading.Event()
        self.workers = []
        self.workload_errors = []
        self.task_latencies = []
        self.failure_lock = threading.Lock()
        self.gateway_port = unused_port()
        self.admin_key = "owned-recovery-admin-key"
        self.started = time.monotonic()
        self.report = {"passed": False, "kernel": self.kernel, "duration_requested_seconds": duration,
                       "workers": self.worker_count, "distinct_workload_agent_identities": self.worker_count,
                       "source_commit": subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(),
                       "source_tree_sha256": source_fingerprint(), "cases": [], "faults": [], "errors": [],
                       "contract": "Zero acknowledged completion loss on eligible synchronous failover; PITR quarantines history rewind and external dispatch."}
        self.archive = self.directory / "archive"
        self.archive.mkdir()
        self.archive_command = self.directory / "archive-wal.py"
        self.restore_command = self.directory / "restore-wal.py"
        self.archive_command.write_text("#!/usr/bin/env python3\nimport os,shutil,sys\nfrom pathlib import Path\ndest=Path(" + repr(str(self.archive)) + ")/sys.argv[2]\nif dest.exists():\n sys.exit(0 if dest.read_bytes()==Path(sys.argv[1]).read_bytes() else 1)\nshutil.copyfile(sys.argv[1],str(dest)+'.tmp')\nos.replace(str(dest)+'.tmp',dest)\n")
        self.restore_command.write_text("#!/usr/bin/env python3\nimport shutil,sys\nfrom pathlib import Path\nsource=Path(" + repr(str(self.archive)) + ")/sys.argv[1]\nif not source.exists(): sys.exit(1)\nshutil.copyfile(source,sys.argv[2])\n")
        self.archive_command.chmod(0o700)
        self.restore_command.chmod(0o700)
        with ledger_connect(self.directory) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE acknowledgments(id TEXT PRIMARY KEY, body TEXT NOT NULL, at REAL NOT NULL);
                CREATE TABLE effects(key TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL);
                CREATE TABLE deliveries(seq INTEGER PRIMARY KEY, key TEXT NOT NULL, digest TEXT NOT NULL, status INTEGER NOT NULL);
                CREATE TABLE intents(id TEXT PRIMARY KEY, body TEXT NOT NULL);
            """)

    def command(self, *args, timeout=30, capture=True):
        result = subprocess.run(args, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
        with (self.directory / "commands.log").open("a") as log:
            log.write("$ " + " ".join(str(x) for x in args) + "\n" + result.stdout + "\n")
        if result.returncode:
            raise RuntimeError("command failed (" + str(result.returncode) + "): " + " ".join(str(x) for x in args) + "\n" + result.stdout[-4000:])
        return result.stdout

    def node(self, name):
        node = OwnedPostgres(self, name)
        self.nodes.append(node)
        return node

    def proxy(self, target):
        proxy = TCPProxy(target)
        self.proxies.append(proxy)
        return proxy

    def record(self, name, **data):
        self.report["cases"].append({"name": name, "passed": True, **data})
        print("PASS " + name, flush=True)

    def spawn(self, name, command, env):
        log = (self.directory / (name + ".log")).open("ab")
        self.logs.append(log)
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.children.append(child)
        return child

    @staticmethod
    def stop_child(child, crash=False):
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)

    def api(self, method, path, body=None, token=None, expected=200, retry=True, deadline=60):
        headers = {"Content-Type": "application/json"}
        headers["Authorization" if token else "X-Admin-Key"] = "Bearer " + token if token else self.admin_key
        encoded = None if body is None else json.dumps(body).encode()
        end = time.monotonic() + deadline
        while True:
            request = urllib.request.Request("http://127.0.0.1:" + str(self.gateway_port) + path, data=encoded, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=min(8, max(.1, end - time.monotonic()))) as response:
                    code, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, raw = error.code, error.read()
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                if retry and time.monotonic() < end:
                    time.sleep(.1)
                    continue
                raise
            value = json.loads(raw)
            if code == expected:
                return value
            error = value.get("error", {})
            transient = code >= 500 or code == 429 or error.get("code", "").startswith(("08", "57P"))
            if retry and transient and time.monotonic() < end:
                time.sleep(.1)
                continue
            raise AssertionError((method, path, code, expected, value))

    def raw_status(self, method, path, body=None, token=None, admin_key=None):
        headers = {"Content-Type": "application/json"}
        headers["Authorization" if token else "X-Admin-Key"] = "Bearer " + token if token else (admin_key or self.admin_key)
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request("http://127.0.0.1:" + str(self.gateway_port) + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=4) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            return 0, b"transport unavailable"

    def start_gateway(self, node, allow=False):
        self.stop_child(self.gateway)
        self.database_proxy.redirect(node.port)
        env = {**os.environ, "DATABASE_URL": "postgres://recovery_admin@127.0.0.1:" + str(self.database_proxy.port) + "/recovery_test?sslmode=disable",
               "ADMIN_KEY": self.admin_key, "ADDR": "127.0.0.1:" + str(self.gateway_port),
               "PRINCIPAL_SECRET": "recovery-test-principal-secret", "AUDIT_MODE": "best-effort", "AUDIT_SPOOL_CAPACITY": "100000"}
        env.pop("AUDIT_SPOOL_DIR", None)
        # The production recovery policy contract is populated by configure_policy.
        env.update(self.gateway_recovery_env(node, allow))
        self.gateway = self.spawn("gateway", [str(self.directory / "gateway")], env)
        wait_for(lambda: self.gateway.poll() is not None or self.raw_status("GET", "/healthz")[0] in (200, 503), "gateway listens", 20)
        if self.gateway.poll() is not None:
            raise AssertionError("gateway exited; inspect gateway.log")

    def gateway_recovery_env(self, node, allow):
        return {"RECOVERY_SYSTEM_ID": self.policy_system, "RECOVERY_TIMELINE": str(self.policy_timeline),
                "RECOVERY_GENERATION": str(self.policy_generation)}

    def activate(self, node, mode, generation):
        system, timeline = node.identity()
        result = node.sql("BEGIN; SET LOCAL synchronous_commit=remote_apply; SELECT swarmpg.recovery_activate('" + system + "'," + str(timeline) + "," + str(generation) + ",'" + mode + "','owned acceptance test: fenced old primary and verified synchronous replica'); COMMIT")
        self.policy_system, self.policy_timeline, self.policy_generation = system, timeline, generation
        self.report.setdefault("operator_transitions", []).append({"mode": mode, "system_identifier": system, "timeline": timeline, "generation": generation, "result": result})

    def start_dispatcher(self):
        self.stop_child(self.dispatcher)
        env = {**os.environ, "OUTBOX_ADMIN_KEY": self.admin_key, "OUTBOX_TARGET_AUTHORIZATION": "Bearer recovery-receiver"}
        self.dispatcher = self.spawn("dispatcher", [str(self.directory / "outbox-dispatcher"), "-gateway", "http://127.0.0.1:" + str(self.gateway_port),
            "-org-id", self.org, "-destination", "recovery-tool", "-endpoint", "http://127.0.0.1:" + str(self.receiver.port),
            "-lease-ms", "2000", "-request-timeout", "3s", "-poll-interval", "100ms", "-concurrency", "4"], env)

    def create_standby(self, primary, name):
        node = self.node(name)
        primary.backup(node.directory)
        node.configure()
        proxy = self.proxy(primary.port)
        with (node.directory / "postgresql.auto.conf").open("a") as handle:
            handle.write("\nprimary_conninfo = 'host=127.0.0.1 port=" + str(proxy.port) + " user=recovery_admin application_name=recovery_standby sslmode=disable'\n")
            handle.write("synchronous_standby_names = ''\n")
            handle.write("recovery_target_name = ''\nrecovery_target_time = ''\nrecovery_target_lsn = ''\nrecovery_target_xid = ''\nrecovery_target = ''\n")
        (node.directory / "standby.signal").touch()
        node.start()
        primary.setting("synchronous_standby_names", "FIRST 1 (recovery_standby)")
        wait_for(lambda: primary.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='recovery_standby' AND state='streaming' AND sync_state='sync'") == "1", "synchronous standby", 30)
        return node, proxy

    def setup(self):
        for env_name, module, binary in (("RECOVERY_GATEWAY_BIN", "gateway", "gateway"), ("RECOVERY_OUTBOX_BIN", "outbox", "outbox-dispatcher")):
            supplied = os.environ.get(env_name)
            if supplied:
                shutil.copy2(supplied, self.directory / binary)
            else:
                self.command("go", "build", "-C", str(ROOT / module), "-o", str(self.directory / binary), ".", timeout=120)
        self.report["binary_sha256"] = {name: hashlib.sha256((self.directory / name).read_bytes()).hexdigest() for name in ("gateway", "outbox-dispatcher")}
        self.primary = self.node("primary")
        self.primary.initialize()
        self.primary.start()
        self.primary.command("createdb", "-h", "127.0.0.1", "-p", str(self.primary.port), "-U", "recovery_admin", "recovery_test")
        self.primary.command("psql", self.primary.url(), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", str(ROOT / "sql/schema.sql"), timeout=60)
        self.report["postgres"] = self.primary.sql("SELECT version()")
        self.standby, self.replication_proxy = self.create_standby(self.primary, "standby")
        self.activate(self.primary, "bootstrap", 1)
        self.database_proxy = self.proxy(self.primary.port)
        self.receiver = Receiver(self.directory)
        self.start_gateway(self.primary, allow=True)
        self.org = self.api("POST", "/v1/orgs", {"name": "recovery-" + str(uuid.uuid4())}, expected=201, retry=False)["id"]
        agent = self.api("POST", "/v1/agents", {"org_id": self.org, "name": "recovery-worker", "rps_limit": 10000, "max_concurrency": 100}, expected=201, retry=False)
        self.agent, self.token = agent["id"], agent["token"]
        self.worker_tokens = []
        for index in range(self.worker_count):
            worker_agent = self.api("POST", "/v1/agents", {"org_id": self.org, "name": "sustained-worker-" + str(index), "rps_limit": 10000, "max_concurrency": 4}, expected=201, retry=False)
            self.worker_tokens.append(worker_agent["token"])
        self.backup_directory = self.directory / "physical-backup"
        self.primary.backup(self.backup_directory)
        self.primary.command("pg_verifybackup", str(self.backup_directory), timeout=60)
        self.report["basebackup_manifest_verified"] = True
        self.report["settings"] = json.loads(self.primary.sql("SELECT json_build_object('synchronous_commit',current_setting('synchronous_commit'),'synchronous_standby_names',current_setting('synchronous_standby_names'),'archive_mode',current_setting('archive_mode'),'wal_level',current_setting('wal_level'),'max_connections',current_setting('max_connections'))"))
        self.report["initial_identity"] = self.primary.identity()
        self.start_dispatcher()
        self.record("owned primary and synchronous streaming standby with physical base backup and WAL archive")

    def task(self, mode="normal", queue=None, complete=True, token=None):
        token = token or self.token
        started = time.monotonic()
        unique = str(uuid.uuid4())
        queue = queue or unique
        task = self.api("POST", "/v1/tasks", {"queue": queue, "key": unique, "payload": {"fixture": unique}}, token)["task"]
        claimed = self.api("POST", "/v1/tasks/claim", {"queue": queue, "request_id": str(uuid.uuid4()), "lease_ms": 120000}, token)["task"]
        assert claimed["id"] == task["id"]
        self.api("POST", "/v1/tasks/" + task["id"] + "/renew", {"fence": claimed["fence"], "lease_ms": 120000}, token)
        body = {"fence": claimed["fence"], "result": {"fixture": unique, "value": 42},
                "effects": [{"key": "effect", "destination": "recovery-tool", "payload": {"fixture": unique, "mode": mode}}]}
        if not complete:
            return task, claimed, body
        self.api("POST", "/v1/tasks/" + task["id"] + "/complete", body, token)
        acknowledged = {"id": task["id"], "state": "completed", "result": body["result"], "fence": claimed["fence"]}
        with ledger_connect(self.directory) as db:
            db.execute("INSERT INTO acknowledgments VALUES(?,?,?)", (task["id"], json.dumps(acknowledged), time.time()))
        messages = self.api("GET", "/v1/tasks/" + task["id"] + "/outbox", token=token)["messages"]
        assert len(messages) == 1
        with ledger_connect(self.directory) as db:
            db.execute("INSERT INTO intents VALUES(?,?)", (messages[0]["id"], json.dumps(messages[0])))
        with self.failure_lock:
            self.task_latencies.append(time.monotonic() - started)
        return task, claimed, messages[0]

    def worker(self, token):
        while not self.workload_stop.is_set():
            if self.workload_pause.is_set():
                time.sleep(.05)
                continue
            try:
                self.task(token=token)
            except BaseException as error:
                with self.failure_lock:
                    self.workload_errors.append(repr(error))
                self.workload_stop.set()
                return
            time.sleep(.05)

    def start_workload(self):
        self.workload_started = time.monotonic()
        for token in self.worker_tokens:
            worker = threading.Thread(target=self.worker, args=(token,), daemon=True)
            self.workers.append(worker)
            worker.start()

    def stop_workload(self):
        self.workload_stop.set()
        for worker in self.workers:
            worker.join(timeout=75)
            if worker.is_alive():
                raise AssertionError("workload failed to stop")
        if self.workload_errors:
            raise AssertionError(self.workload_errors)

    def verify_completions(self, node):
        with ledger_connect(self.directory) as db:
            acknowledged = [json.loads(row[0]) for row in db.execute("SELECT body FROM acknowledgments")]
        persisted = json.loads(node.sql("SELECT coalesce(json_agg(json_build_object('id',id,'state',state,'result',result,'fence',fence::text)),'[]') FROM swarmpg.tasks WHERE org_id='" + self.org + "'"))
        validate_acknowledged(acknowledged, persisted)
        return len(acknowledged)

    def wait_delivered(self):
        wait_for(lambda: self.primary.sql("SELECT count(*) FROM swarmpg.outbox_messages WHERE org_id='" + self.org + "' AND state <> 'delivered'") == "0", "all effects acknowledged", 60)

    def verify_effects(self):
        self.wait_delivered()
        with ledger_connect(self.directory) as db:
            intents = {row[0]: json.loads(row[1]) for row in db.execute("SELECT id,body FROM intents")}
            effects = db.execute("SELECT id,key,digest,body FROM effects").fetchall()
            assert {row[0] for row in effects} == set(intents), "missing or extra external effects"
            for identity, key, digest, raw in effects:
                expected = {k: intents[identity][k] for k in ("id", "org_id", "task_id", "key", "destination", "payload", "idempotency_key")}
                assert key == expected["idempotency_key"] and hashlib.sha256(raw.encode()).hexdigest() == digest
                assert json.dumps(expected, sort_keys=True) == json.dumps(json.loads(raw), sort_keys=True), "effect payload differs from committed intent"
            assert db.execute("SELECT count(*) FROM (SELECT key FROM deliveries GROUP BY key HAVING count(DISTINCT digest)>1)").fetchone()[0] == 0
            requests = db.execute("SELECT count(*) FROM deliveries").fetchone()[0]
        return len(effects), requests

    def partition_case(self):
        task, claim, completion = self.task(complete=False)
        self.workload_pause.set()
        # Delay only this owned fixture's completion *after* every production
        # recovery check. Partition during the deferred COMMIT trigger, so the transaction
        # genuinely commits locally without its WAL reaching the replica.
        self.primary.sql("CREATE FUNCTION public.recovery_test_completion_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='" + task["id"] + "'::uuid AND NEW.state='completed' THEN PERFORM pg_sleep(2); END IF; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER recovery_test_completion_delay AFTER UPDATE ON swarmpg.tasks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.recovery_test_completion_delay()")
        result = []
        request = threading.Thread(target=lambda: result.append(self.raw_status("POST", "/v1/tasks/" + task["id"] + "/complete", completion, self.token)), daemon=True)
        request.start()
        pid = wait_for(lambda: self.primary.sql("SELECT pid FROM pg_stat_activity WHERE wait_event='PgSleep' AND lower(query)='commit' LIMIT 1"), "target completion past all guards inside deferred commit trigger", 10)
        self.replication_proxy.partition(True)
        wait_for(lambda: self.primary.sql("SELECT count(*) FROM pg_stat_activity WHERE pid=" + pid + " AND wait_event='SyncRep'") == "1", "target completion in synchronous commit wait", 10)
        self.primary.sql("SELECT pg_cancel_backend(" + pid + ")")
        request.join(timeout=8)
        assert result and result[0][0] == 503, ("cancelled synchronous commit was not HTTP503", result)
        assert json.loads(result[0][1])["error"]["code"] == "commit_unconfirmed", result
        assert self.primary.sql("SELECT state FROM swarmpg.tasks WHERE id='" + task["id"] + "'") == "completed", "fixture never committed locally"
        assert self.standby.sql("SELECT state FROM swarmpg.tasks WHERE id='" + task["id"] + "'") != "completed", "partition did not isolate unreplicated completion"
        with ledger_connect(self.directory) as db:
            assert not any(json.loads(row[0])["task_id"] == task["id"] for row in db.execute("SELECT body FROM effects")), "unreplicated intent reached external receiver"
        replay_code, _ = self.raw_status("POST", "/v1/tasks/" + task["id"] + "/complete", completion, self.token)
        assert replay_code not in range(200, 300), "locally visible completion replay bypassed durability barrier"
        wait_for(lambda: self.primary.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='recovery_standby'") == "0", "replication network partition", 10)
        required = self.primary.sql("SELECT pg_current_wal_lsn()::text")
        actual = self.standby.sql("SELECT pg_last_wal_replay_lsn()::text")
        try:
            require_replayed(required, actual)
        except AssertionError:
            pass
        else:
            raise AssertionError("unsafe standby unexpectedly covers partitioned WAL")
        denied, _ = self.raw_status("POST", "/v1/tasks/" + task["id"] + "/complete", completion, self.token)
        assert denied not in range(200, 300), "partitioned completion was acknowledged"
        self.report["faults"].append({"kind": "replication_partition", "required_lsn": required, "standby_replay_lsn": actual,
                                       "initial_status": result[0][0], "receipt_replay_status": replay_code, "partition_status": denied})
        self.replication_proxy.partition(False)
        wait_for(lambda: self.primary.sql("SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'") == "1", "replication reconnect", 15)
        wait_for(lambda: self.standby.sql("SELECT state FROM swarmpg.tasks WHERE id='" + task["id"] + "'") == "completed", "uncertain completion replayed after healing", 15)
        self.standby.sql("SELECT pg_wal_replay_pause()")
        paused_replay, _ = self.raw_status("POST", "/v1/tasks/" + task["id"] + "/complete", completion, self.token)
        assert paused_replay not in range(200, 300), "receipt replay succeeded before synchronous replica applied commit"
        self.standby.sql("SELECT pg_wal_replay_resume()")
        self.primary.sql("DROP TRIGGER recovery_test_completion_delay ON swarmpg.tasks; DROP FUNCTION public.recovery_test_completion_delay()")
        self.api("POST", "/v1/tasks/" + task["id"] + "/complete", completion, self.token)
        acknowledged = {"id": task["id"], "state": "completed", "result": completion["result"], "fence": claim["fence"]}
        messages = self.api("GET", "/v1/tasks/" + task["id"] + "/outbox", token=self.token)["messages"]
        with ledger_connect(self.directory) as db:
            db.execute("INSERT INTO acknowledgments VALUES(?,?,?)", (task["id"], json.dumps(acknowledged), time.time()))
            db.execute("INSERT INTO intents VALUES(?,?)", (messages[0]["id"], json.dumps(messages[0])))
        self.workload_pause.clear()
        self.record("replication partition rejects success, unsafe promotion and local-only receipt replay")

    def failover_case(self):
        # The receiver commits before the dispatcher can acknowledge it. Fail
        # the primary at that exact external side-effect uncertainty boundary.
        stale_task, stale_claim, _ = self.task(complete=False)
        _, _, held = self.task(mode="hold-before-ack")
        assert self.receiver.blocked_effect.wait(timeout=20), "receiver did not reach durable effect before ack"
        acknowledged_before = self.verify_completions(self.primary)
        required = self.primary.sql("SELECT pg_current_wal_lsn()::text")
        wait_for(lambda: lsn_number(self.standby.sql("SELECT pg_last_wal_replay_lsn()::text")) >= lsn_number(required), "standby covers known acknowledged WAL", 20)
        old_primary = self.primary
        started = time.monotonic()
        old_primary.stop(crash=True)
        # Stopping the actual postmaster and checking its pid file is the owned
        # test's fence. This harness does not claim to implement cloud STONITH.
        assert not (old_primary.directory / "postmaster.pid").exists(), "old primary has not been fenced"
        require_replayed(required, self.standby.sql("SELECT pg_last_wal_replay_lsn()::text"))
        self.standby.command("pg_ctl", "-D", str(self.standby.directory), "-w", "promote")
        self.primary = self.standby
        self.primary.sql("CHECKPOINT")
        # Routing to a new timeline with the old immutable pin must fail closed.
        self.database_proxy.redirect(self.primary.port)
        status, _ = self.raw_status("POST", "/v1/tasks/claim", {"queue": "pin-probe", "request_id": str(uuid.uuid4())}, self.token)
        assert status not in range(200, 300), "unapproved promoted timeline accepted traffic"
        self.standby, self.replication_proxy = self.create_standby(self.primary, "replacement-standby")
        self.activate(self.primary, "failover", 1)
        self.start_gateway(self.primary, allow=True)
        self.receiver.release.set()
        self.task()
        rto = time.monotonic() - started
        assert rto < 60, "eligible failover exceeded 60-second acceptance RTO"
        verified = self.verify_completions(self.primary)
        wait_for(lambda: self.primary.sql("SELECT state FROM swarmpg.outbox_messages WHERE id='" + held["id"] + "'") == "delivered", "uncertain external effect recovered", 30)
        with ledger_connect(self.directory) as db:
            assert db.execute("SELECT count(*) FROM effects WHERE id=?", (held["id"],)).fetchone()[0] == 1
            assert db.execute("SELECT count(*) FROM deliveries WHERE key=?", (held["idempotency_key"],)).fetchone()[0] >= 2
        # Simulate an accidentally returned former primary on an isolated
        # owned endpoint. The newly pinned gateway must reject its old history.
        old_primary.start()
        returned_standby, _ = self.create_standby(old_primary, "returned-old-standby")
        self.database_proxy.redirect(old_primary.port)
        def returned_pin_denied():
            # Administrative guarded writes expose the exact identity error;
            # runtime requests may wrap it as admission_unavailable.
            status, body = self.raw_status("POST", "/v1/orgs", {"name": "must-not-create-on-returned-old-primary"})
            assert status not in range(200, 300), "returned old primary accepted new timeline pins"
            return status if status == 503 and "gateway recovery identity" in json.loads(body)["error"]["message"] else None
        returned_status = wait_for(returned_pin_denied, "returned old primary rejects exact timeline pins", 10)
        returned_standby.stop(crash=True)
        old_primary.stop(crash=True)
        self.database_proxy.redirect(self.primary.port)
        # Stale ownership must remain stale on the promoted database, too.
        self.api("POST", "/v1/tasks/" + stale_task["id"] + "/release", {"fence": stale_claim["fence"]}, self.token)
        fresh = self.api("POST", "/v1/tasks/claim", {"queue": stale_task["queue"], "request_id": str(uuid.uuid4()), "lease_ms": 30000}, self.token)["task"]
        assert int(fresh["fence"]) > int(stale_claim["fence"])
        self.api("POST", "/v1/tasks/" + stale_task["id"] + "/renew", {"fence": stale_claim["fence"], "lease_ms": 1000}, self.token, expected=409, retry=False)
        self.report.update({"failover_rto_seconds": rto, "acknowledged_before_failover": acknowledged_before, "verified_after_failover": verified, "acknowledged_completions_lost": 0})
        self.report["faults"].append({"kind": "primary_immediate_shutdown", "old_primary_fenced": True, "required_lsn": required, "old_pin_status": status, "returned_old_primary_status": returned_status, "rto_seconds": rto})
        self.record("fenced primary failure retains acknowledgments and deduplicates external effect with lost receipt", rto_seconds=rto)

    def pitr_case(self):
        self.wait_delivered()
        # Preserve a pending intent in the snapshot to require explicit outbox
        # reconciliation, and create a distinct effect after the restore point
        # whose entire database history will be absent after PITR.
        self.stop_child(self.dispatcher)
        _, _, pending = self.task()
        restore_name = "recovery_acceptance_" + uuid.uuid4().hex
        target_lsn = self.primary.sql("SELECT pg_create_restore_point('" + restore_name + "')::text")
        target_acks = self.verify_completions(self.primary)
        with ledger_connect(self.directory) as db:
            target_acknowledged = [json.loads(row[0]) for row in db.execute("SELECT body FROM acknowledgments")]
        after_task, _, after = self.task()
        self.start_dispatcher()
        self.wait_delivered()
        with ledger_connect(self.directory) as db:
            assert db.execute("SELECT count(*) FROM effects WHERE id=?", (after["id"],)).fetchone()[0] == 1
        self.primary.sql("SELECT pg_switch_wal()")
        wait_for(lambda: self.primary.sql("SELECT count(*) FROM pg_stat_archiver WHERE last_archived_wal IS NOT NULL") == "1", "WAL archived", 15)
        # Ensure the segment containing the target and subsequent intent is in
        # the archive, not merely an earlier segment's successful archive stat.
        target_file = self.primary.sql("SELECT pg_walfile_name('" + target_lsn + "'::pg_lsn)")
        wait_for(lambda: (self.archive / target_file).exists(), "restore-point WAL segment archived", 20)
        self.stop_child(self.dispatcher)
        self.stop_child(self.gateway)
        self.primary.stop(crash=True)
        self.standby.stop(crash=True)
        restored = self.node("pitr-restored")
        shutil.copytree(self.backup_directory, restored.directory)
        for filename in ("standby.signal", "recovery.signal", "postmaster.pid"):
            (restored.directory / filename).unlink(missing_ok=True)
        restored.configure()
        with (restored.directory / "postgresql.auto.conf").open("a") as config:
            config.write("\nsynchronous_standby_names = ''\nprimary_conninfo = ''\n")
            config.write("restore_command = " + restored.pg_quote(shlex.quote(str(self.restore_command)) + " '%f' '%p'") + "\n")
            config.write("recovery_target_name = '" + restore_name + "'\nrecovery_target_timeline = 'latest'\nrecovery_target_action = 'promote'\n")
        (restored.directory / "recovery.signal").touch()
        restored.start()
        wait_for(lambda: restored.sql("SELECT NOT pg_is_in_recovery()") == "t", "PITR reaches named restore point", 30)
        restored.sql("CHECKPOINT")
        assert restored.sql("SELECT count(*) FROM swarmpg.outbox_messages WHERE id='" + after["id"] + "'") == "0", "PITR did not rewind the post-target intent"
        assert restored.sql("SELECT count(*) FROM swarmpg.outbox_messages WHERE id='" + pending["id"] + "'") == "1"
        restored_tasks = json.loads(restored.sql("SELECT coalesce(json_agg(json_build_object('id',id,'state',state,'result',result,'fence',fence::text)),'[]') FROM swarmpg.tasks WHERE org_id='" + self.org + "'"))
        validate_acknowledged(target_acknowledged, restored_tasks)
        self.start_gateway(restored)
        old_status, _ = self.raw_status("POST", "/v1/tasks/claim", {"queue": "restored-pin-probe", "request_id": str(uuid.uuid4())}, self.token)
        assert old_status not in range(200, 300), "PITR reused old approved lineage"
        self.primary = restored
        self.standby, self.replication_proxy = self.create_standby(restored, "pitr-standby")
        self.activate(restored, "restore", 2)
        old_admin_key = self.admin_key
        self.admin_key = uuid.uuid4().hex + uuid.uuid4().hex
        self.start_gateway(restored)
        quarantined, _ = self.raw_status("POST", "/v1/tasks/claim", {"queue": "quarantine-probe", "request_id": str(uuid.uuid4())}, self.token)
        assert quarantined not in range(200, 300), "restored generation accepted traffic before reconciliation"
        old_admin_status, _ = self.raw_status("POST", "/v1/admin/outbox/claim", {"org_id": self.org, "destination": "recovery-tool", "worker_id": str(uuid.uuid4()), "request_id": str(uuid.uuid4())}, admin_key=old_admin_key)
        assert old_admin_status == 401, "pre-PITR dispatcher credential remained valid"
        self.start_dispatcher()
        with ledger_connect(self.directory) as db:
            before_requests = db.execute("SELECT count(*) FROM deliveries").fetchone()[0]
        time.sleep(1)
        with ledger_connect(self.directory) as db:
            assert db.execute("SELECT count(*) FROM deliveries").fetchone()[0] == before_requests, "quarantined restored database dispatched external effects"
        self.stop_child(self.dispatcher)
        # Evidence is outside PostgreSQL: target-excluded effect remains in the
        # receiver ledger. Explicitly record it; never silently regenerate it.
        self.report["pitr"] = {"target_name": restore_name, "target_lsn": target_lsn,
                               "acknowledged_before_target": target_acks, "known_rewound_external_effect": after["id"],
                               "old_pin_status": old_status, "quarantine_status": quarantined,
                               "old_admin_credential_status": old_admin_status,
                               "external_effect_reconciliation": "retained in independent receiver ledger; old credentials revoked before operator allows new work"}
        pending_ids = json.loads(restored.sql("SELECT coalesce(json_agg(id),'[]') FROM swarmpg.outbox_messages WHERE state <> 'delivered'"))
        assert pending_ids, "PITR must retain an unresolved intent to test resume rejection"
        try:
            restored.sql("BEGIN; SET LOCAL synchronous_commit=remote_apply; SELECT swarmpg.recovery_resume('" + self.policy_system + "'," + str(self.policy_timeline) + ",2,'deliberately premature resume'); COMMIT")
        except RuntimeError as error:
            assert "every restored undelivered outbox message must be reconciled" in str(error), str(error)
        else:
            raise AssertionError("restore resumed before pending intents were reconciled")
        for identity in pending_ids:
            with ledger_connect(self.directory) as db:
                effect = db.execute("SELECT key FROM effects WHERE id=?", (identity,)).fetchone()
            assert effect, "operator cannot mark unexecuted effect as delivered"
            restored.sql("BEGIN; SET LOCAL synchronous_commit=remote_apply; SELECT swarmpg.recovery_resolve_outbox('" + self.org + "','" + identity + "','delivered','durable receiver evidence.sqlite confirms exactly one effect'); COMMIT")
        restored.sql("BEGIN; SET LOCAL synchronous_commit=remote_apply; SELECT swarmpg.recovery_resume('" + self.policy_system + "'," + str(self.policy_timeline) + ",2,'receiver ledger reconciled; post-target missing effects recorded and old task retries forbidden'); COMMIT")
        old_token = self.token
        denied, _ = self.raw_status("POST", "/v1/tasks", {"queue": after_task["queue"], "key": after_task["task_key"], "payload": after_task["payload"]}, old_token)
        assert denied == 401, "pre-PITR worker credentials could recreate an already executed effect's lost intent"
        self.token = uuid.uuid4().hex + uuid.uuid4().hex
        digest = hashlib.sha256(self.token.encode()).hexdigest()
        restored.sql("BEGIN; SET LOCAL synchronous_commit=remote_apply; UPDATE swarmpg.agents SET token_hash=decode('" + digest + "','hex'),disabled=false WHERE id='" + self.agent + "'; COMMIT")
        self.report["pitr"]["old_credential_replay_status"] = denied
        self.start_dispatcher()
        self.task()
        self.wait_delivered()
        self.record("physical PITR restores named point and quarantines writes and external effects until explicit reconciliation")

    def execute(self):
        self.setup()
        self.start_workload()
        time.sleep(min(5, self.duration / 8))
        self.partition_case()
        self.failover_case()
        while time.monotonic() - self.workload_started < self.duration:
            if self.workload_errors:
                raise AssertionError(self.workload_errors)
            time.sleep(.2)
        self.stop_workload()
        self.report["sustained_workload_seconds"] = time.monotonic() - self.workload_started
        self.report["verified_completions"] = self.verify_completions(self.primary)
        self.report["verified_effects"], self.report["receiver_requests"] = self.verify_effects()
        ordered = sorted(self.task_latencies)
        self.report["task_cycle_latency_seconds"] = {"p50": ordered[int((len(ordered)-1)*.50)], "p95": ordered[int((len(ordered)-1)*.95)], "p99": ordered[int((len(ordered)-1)*.99)], "max": ordered[-1]}
        self.report["verified_tasks_per_second"] = self.report["verified_completions"] / self.report["sustained_workload_seconds"]
        self.record("sustained mixed task, lease and outbox workload matches independent durable success ledger")
        self.pitr_case()
        self.report["passed"] = True

    def run(self):
        try:
            self.execute()
        except BaseException as error:
            self.report["errors"].append(repr(error))
            raise
        finally:
            self.workload_stop.set()
            for child in reversed(self.children):
                self.stop_child(child)
            for worker in self.workers:
                worker.join(timeout=1)
            if self.receiver:
                self.receiver.close()
            for node in reversed(self.nodes):
                try:
                    node.stop(crash=True)
                except BaseException as error:
                    self.report["errors"].append("cleanup: " + repr(error))
                    self.report["passed"] = False
            for proxy in self.proxies:
                proxy.close()
            for log in self.logs:
                log.close()
            self.report["elapsed_seconds"] = time.monotonic() - self.started
            (self.directory / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")
            print(json.dumps({key: self.report.get(key) for key in ("passed", "verified_completions", "verified_effects", "failover_rto_seconds", "errors")}), flush=True)
            print("recovery evidence retained in " + str(self.directory), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=float(os.environ.get("RECOVERY_DURATION", "300")), help="sustained task workload duration, seconds")
    parser.add_argument("--directory", default=os.environ.get("RECOVERY_REPORT_DIR"), help="empty directory for owned services and retained evidence")
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error("duration must be positive")
    directory = args.directory or tempfile.mkdtemp(prefix="swarmpg-recovery-")
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    Harness(args.duration, directory).run()
