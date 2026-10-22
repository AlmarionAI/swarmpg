#!/usr/bin/env python3
"""Check cumulative write budgets across suspended/interleaved protocol portals.

Run against a disposable, trust-authenticated kernel cluster using DATABASE_URL
(postgres:// or postgresql://). PGHOST, PGPORT, PGUSER and PGDATABASE override
URL values. This minimal protocol client does not support TLS or password auth.
Uses only the Python standard library; scripts/e2e.sh runs it with KERNEL=1.
"""

import os
import socket
import struct
from urllib.parse import parse_qs, unquote, urlsplit


def connection_settings(env=None):
    """Resolve the disposable test endpoint without exposing credentials in errors."""
    if env is None:
        env = os.environ
    settings = {
        "host": "localhost",
        "port": "5432",
        "user": env.get("USER", "postgres"),
        "database": "postgres",
    }
    uri = env.get("DATABASE_URL", "")
    if uri:
        parsed = urlsplit(uri)
        if parsed.scheme not in ("postgres", "postgresql"):
            raise ValueError("DATABASE_URL must be a postgres:// or postgresql:// URL")
        if parsed.hostname:
            settings["host"] = unquote(parsed.hostname)
        if parsed.port is not None:
            settings["port"] = str(parsed.port)
        if parsed.username:
            settings["user"] = unquote(parsed.username)
        if parsed.path and parsed.path != "/":
            settings["database"] = unquote(parsed.path[1:])
        query = parse_qs(parsed.query)
        for option, setting in (("host", "host"), ("port", "port"),
                                ("user", "user"), ("dbname", "database")):
            if option in query:
                settings[setting] = query[option][-1]
        if query.get("sslmode", ["disable"])[-1] not in ("disable", "allow", "prefer"):
            raise ValueError("protocol regression requires a disposable non-TLS cluster")
    for variable, setting in (("PGHOST", "host"), ("PGPORT", "port"),
                              ("PGUSER", "user"), ("PGDATABASE", "database")):
        if env.get(variable):
            settings[setting] = env[variable]
    settings["port"] = int(settings["port"])
    if not 1 <= settings["port"] <= 65535:
        raise ValueError("protocol regression port must be between 1 and 65535")
    return settings


def cstring(value):
    return value.encode() + b"\0"


class Connection:
    def __init__(self):
        settings = connection_settings()
        host, port = settings["host"], settings["port"]
        if host.startswith("/"):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(f"{host}/.s.PGSQL.{port}")
        else:
            self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.settimeout(10)
        user, database = settings["user"], settings["database"]
        body = struct.pack("!I", 196608)
        body += b"user\0" + cstring(user) + b"database\0" + cstring(database) + b"\0"
        self.sock.sendall(struct.pack("!I", len(body) + 4) + body)
        self.receive_until(b"Z")

    def send(self, kind, body=b""):
        self.sock.sendall(kind + struct.pack("!I", len(body) + 4) + body)

    def read(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise RuntimeError("server disconnected")
            data += chunk
        return data

    def receive_until(self, terminal):
        messages = []
        while True:
            kind = self.read(1)
            body = self.read(struct.unpack("!I", self.read(4))[0] - 4)
            if kind == b"R" and body != struct.pack("!I", 0):
                raise RuntimeError("test requires a disposable trust-authenticated cluster")
            messages.append((kind, body))
            if kind in terminal:
                return messages

    def query(self, sql):
        self.send(b"Q", cstring(sql))
        result = self.receive_until(b"Z")
        errors = [body for kind, body in result if kind == b"E"]
        assert not errors, errors
        return [body for kind, body in result if kind == b"D"]

    def bind(self, portal):
        self.send(b"B", cstring(portal) + b"budget_stmt\0" + struct.pack("!HHH", 0, 0, 0))

    def execute(self, portal):
        self.send(b"E", cstring(portal) + struct.pack("!I", 1))
        self.send(b"H")
        return self.receive_until(b"sCE")


def main():
    connection = Connection()
    try:
        connection.query("""
            CREATE TEMP TABLE agent_budget_protocol (id int);
            CREATE TEMP TABLE agent_budget_protocol_history (id int);
            CREATE FUNCTION pg_temp.agent_budget_record() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              INSERT INTO agent_budget_protocol_history VALUES (NEW.id);
              RETURN NEW;
            END $$;
            CREATE TRIGGER agent_budget_record AFTER INSERT ON agent_budget_protocol
              FOR EACH ROW EXECUTE FUNCTION pg_temp.agent_budget_record();
            CREATE FUNCTION pg_temp.agent_budget_put(i int) RETURNS int LANGUAGE plpgsql AS $$
            BEGIN
              INSERT INTO agent_budget_protocol VALUES (i);
              RETURN i;
            END $$;
            SET agent_max_rows = 4;
        """)
        sql = "SELECT pg_temp.agent_budget_put(g) FROM generate_series(1, 3) g"
        connection.send(b"P", b"budget_stmt\0" + cstring(sql) + struct.pack("!H", 0))
        connection.bind("budget_a")
        connection.bind("budget_b")
        for portal in ("budget_a", "budget_b", "budget_a", "budget_b"):
            messages = connection.execute(portal)
            assert messages[-1][0] == b"s", messages
            assert sum(kind == b"D" for kind, _ in messages) == 1, messages
        messages = connection.execute("budget_a")
        assert messages[-1][0] == b"E", messages
        fields = messages[-1][1].split(b"\0")
        assert b"C53400" in fields, fields
        connection.send(b"S")
        connection.receive_until(b"Z")
        # Both portals were in one implicit transaction: all writes must roll back.
        rows = connection.query("SELECT count(*) FROM agent_budget_protocol UNION ALL "
                                "SELECT count(*) FROM agent_budget_protocol_history")
        zero = struct.pack("!HI", 1, 1) + b"0"
        assert rows == [zero, zero], rows
        # A subsequent statement has a fresh budget and still permits normal writes.
        connection.query("SELECT pg_temp.agent_budget_put(10)")
        rows = connection.query("SELECT count(*) FROM agent_budget_protocol UNION ALL "
                                "SELECT count(*) FROM agent_budget_protocol_history")
        one = struct.pack("!HI", 1, 1) + b"1"
        assert rows == [one, one], rows
        print("PASS: suspended/interleaved portals, SQLSTATE 53400, atomic rollback, fresh next statement")
    finally:
        connection.send(b"X")
        connection.sock.close()


if __name__ == "__main__":
    main()
