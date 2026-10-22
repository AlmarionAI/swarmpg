-- SwarmPG core schema. Stock Postgres 14+; pgvector optional.
--
-- Isolation model (what the database enforces, not the prompt and not the gateway):
--   * Every agent query runs as ONE shared low-privilege login role, agent_runtime.
--     The gateway logs in directly AS agent_runtime (it is the session_user), so an
--     agent cannot SET ROLE / RESET ROLE to anything more privileged.
--   * Identity is a transaction-local setting (app.agent_id / app.org_id) the gateway
--     establishes with SET LOCAL at the start of every transaction. On the SwarmPG
--     kernel this is the sealed `agent_principal` GUC (immutable once set); on stock Postgres
--     the gateway's statement guard is the backstop. See DESIGN.md §7.
--   * Agent-visible relations are tenant-scoped shared tables with FORCE ROW LEVEL
--     SECURITY keyed on app.agent_id / app.org_id. This is DESIGN.md §1.2 "tier 1",
--     the 99% case. It is the only agent data surface that stock Postgres can isolate
--     correctly under connection pooling.
--   * Agents are deliberately given NO DDL and NO ability to own database objects.
--     A shared DDL scratch schema cannot be isolated per-agent on stock Postgres with
--     a shared role (object ownership and signalling are role-scoped, and SET ROLE to a
--     per-agent role reintroduces an escalation path), so "private DDL" is DESIGN.md
--     "tier 2": deferred to the in-core space/catalog work, not offered here.

DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'pgvector not available, memory.embedding will be omitted';
END $$;

-- The single low-privilege login role every agent query runs as.
DO $$ BEGIN
  CREATE ROLE agent_runtime LOGIN;
EXCEPTION WHEN duplicate_object THEN
  ALTER ROLE agent_runtime LOGIN;
END $$;

CREATE SCHEMA IF NOT EXISTS swarmpg;
GRANT USAGE ON SCHEMA swarmpg TO agent_runtime;

-- Current principal, read from the per-transaction settings the gateway sets.
-- IMMUTABLE-ish STABLE; cast errors if the setting is not a uuid, which is a
-- feature: a non-uuid identity cannot satisfy any policy.
-- On the SwarmPG kernel both read only the sealed agent_principal (agent and
-- org established together), so a bare SET app.agent_id / app.org_id from an
-- agent changes nothing.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_settings WHERE name = 'agent_principal') THEN
    CREATE OR REPLACE FUNCTION swarmpg.current_agent() RETURNS uuid
    LANGUAGE sql STABLE AS $f$ SELECT pg_catalog.current_agent()::uuid $f$;
    CREATE OR REPLACE FUNCTION swarmpg.current_org() RETURNS uuid
    LANGUAGE sql STABLE AS $f$ SELECT pg_catalog.current_org()::uuid $f$;
  ELSE
    CREATE OR REPLACE FUNCTION swarmpg.current_agent() RETURNS uuid
    LANGUAGE sql STABLE AS $f$
      SELECT NULLIF(current_setting('app.agent_id', true), '')::uuid
    $f$;
    CREATE OR REPLACE FUNCTION swarmpg.current_org() RETURNS uuid
    LANGUAGE sql STABLE AS $f$
      SELECT NULLIF(current_setting('app.org_id', true), '')::uuid
    $f$;
  END IF;
END $$;

-- Control-plane tables (gateway/admin only, never granted to agent_runtime).
CREATE TABLE IF NOT EXISTS swarmpg.orgs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS swarmpg.agents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id uuid NOT NULL REFERENCES swarmpg.orgs(id),
  name text NOT NULL,
  token_hash bytea NOT NULL UNIQUE,          -- sha256(token); plaintext never stored
  shard smallint NOT NULL DEFAULT 0,
  -- budgets (CHECK-constrained so a degenerate agent cannot be minted)
  rps_limit int NOT NULL DEFAULT 20 CHECK (rps_limit >= 0),
  max_rows int NOT NULL DEFAULT 1000 CHECK (max_rows >= 0),
  max_result_bytes bigint NOT NULL DEFAULT 4194304 CHECK (max_result_bytes >= 0),
  max_concurrency int NOT NULL DEFAULT 4 CHECK (max_concurrency >= 0),
  max_cost double precision NOT NULL DEFAULT 1000000 CHECK (max_cost >= 0),  -- planner cost budget (kernel only)

  statement_timeout_ms int NOT NULL DEFAULT 5000 CHECK (statement_timeout_ms >= 0),
  read_only boolean NOT NULL DEFAULT false,
  disabled boolean NOT NULL DEFAULT false,   -- kill switch
  token_expires_at timestamptz,              -- NULL = no expiry
  created_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz
);
CREATE INDEX IF NOT EXISTS agents_org_idx ON swarmpg.agents(org_id);
-- Idempotent migration for clusters created before these columns existed.
DO $$ BEGIN
  ALTER TABLE swarmpg.agents ADD COLUMN IF NOT EXISTS max_result_bytes bigint NOT NULL DEFAULT 4194304;
  ALTER TABLE swarmpg.agents ADD COLUMN IF NOT EXISTS max_concurrency int NOT NULL DEFAULT 4;
  ALTER TABLE swarmpg.agents ADD COLUMN IF NOT EXISTS token_expires_at timestamptz;
  ALTER TABLE swarmpg.agents ADD COLUMN IF NOT EXISTS max_cost double precision NOT NULL DEFAULT 1000000;

EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Server-side audit of what was actually executed. Written by the gateway via a
-- SECURITY DEFINER function; agent_runtime has no direct DML on it, so an agent
-- cannot read, forge or erase it. This is the real audit trail (cf. swarmpg.events,
-- which is an agent-authored journal).
CREATE TABLE IF NOT EXISTS swarmpg.audit (
  id bigserial PRIMARY KEY,
  agent_id uuid,
  org_id uuid,
  at timestamptz NOT NULL DEFAULT now(),
  sql_hash bytea,          -- sha256 of the statement text
  sql_sample text,         -- first bytes, for debugging
  params_count int,
  rows_returned int,
  bytes_returned bigint,
  affected bigint,
  duration_ms double precision,
  sqlstate text,           -- NULL on success
  error text
);
CREATE INDEX IF NOT EXISTS audit_agent_idx ON swarmpg.audit(agent_id, id);
-- Stable gateway receipt IDs make asynchronous batch retries idempotent,
-- including a lost commit acknowledgement and replay after gateway restart.
ALTER TABLE swarmpg.audit ADD COLUMN IF NOT EXISTS receipt_id uuid;
CREATE UNIQUE INDEX IF NOT EXISTS audit_receipt_idx ON swarmpg.audit(receipt_id);

CREATE OR REPLACE FUNCTION swarmpg.record_audit(
  p_agent uuid, p_org uuid, p_sql text, p_params int,
  p_rows int, p_bytes bigint, p_affected bigint, p_ms double precision,
  p_sqlstate text, p_error text) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = swarmpg, pg_catalog AS $$
  INSERT INTO swarmpg.audit(agent_id, org_id, sql_hash, sql_sample, params_count,
                            rows_returned, bytes_returned, affected, duration_ms, sqlstate, error)
  VALUES (p_agent, p_org, sha256(convert_to(coalesce(p_sql,''), 'UTF8')),
          left(coalesce(p_sql,''), 200), p_params, p_rows, p_bytes, p_affected, p_ms, p_sqlstate, p_error)
$$;
-- Only the gateway's admin connection writes audit records. An agent able to
-- call this could file records in another agent's name.
REVOKE ALL ON FUNCTION swarmpg.record_audit(uuid,uuid,text,int,int,bigint,bigint,double precision,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION swarmpg.record_audit(uuid,uuid,text,int,int,bigint,bigint,double precision,text,text) FROM agent_runtime;

CREATE OR REPLACE FUNCTION swarmpg.record_audit_receipt(
  p_receipt uuid, p_agent uuid, p_org uuid, p_at timestamptz,
  p_hash text, p_sample text, p_params int, p_rows int, p_bytes bigint,
  p_affected bigint, p_ms double precision, p_sqlstate text, p_error text) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = swarmpg, pg_catalog AS $$
  INSERT INTO swarmpg.audit(receipt_id, agent_id, org_id, at, sql_hash, sql_sample, params_count,
                            rows_returned, bytes_returned, affected, duration_ms, sqlstate, error)
  VALUES (p_receipt, p_agent, p_org, p_at, decode(p_hash, 'hex'), left(p_sample, 200),
          p_params, p_rows, p_bytes, p_affected, p_ms, p_sqlstate, left(p_error, 1000))
  ON CONFLICT (receipt_id) DO NOTHING
$$;
REVOKE ALL ON FUNCTION swarmpg.record_audit_receipt(uuid,uuid,uuid,timestamptz,text,text,int,int,bigint,bigint,double precision,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION swarmpg.record_audit_receipt(uuid,uuid,uuid,timestamptz,text,text,int,int,bigint,bigint,double precision,text,text) FROM agent_runtime;

-- Idempotency: a client retry after a timeout must not duplicate a write. The
-- gateway claims (agent_id, idempotency_key) before the write and replays the
-- stored response on a repeat. TTL-swept by the control plane.
CREATE TABLE IF NOT EXISTS swarmpg.idempotency (
  agent_id uuid NOT NULL,
  key text NOT NULL,
  response jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (agent_id, key)
);
ALTER TABLE swarmpg.idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.idempotency FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS idem_isolation ON swarmpg.idempotency;
CREATE POLICY idem_isolation ON swarmpg.idempotency
  USING (agent_id = swarmpg.current_agent())
  WITH CHECK (agent_id = swarmpg.current_agent());
GRANT SELECT, INSERT ON swarmpg.idempotency TO agent_runtime;

-- ---------------------------------------------------------------------------
-- Tier-1 agent data: tenant-scoped shared relations, FORCE RLS on app.agent_id.
-- ---------------------------------------------------------------------------

-- Per-agent key/value memory (optionally org-shared, optionally vector).
CREATE TABLE IF NOT EXISTS swarmpg.memory (
  agent_id uuid NOT NULL,
  org_id uuid NOT NULL,
  key text NOT NULL,
  value jsonb NOT NULL,
  shared boolean NOT NULL DEFAULT false,     -- readable by the whole org
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (agent_id, key)
);
DO $$ BEGIN
  ALTER TABLE swarmpg.memory ADD COLUMN embedding vector(1536);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS memory_org_shared_idx ON swarmpg.memory(org_id) WHERE shared;

-- Memory versions and history. Every change to a memory row bumps its version
-- and appends the new state to swarmpg.memory_history (a delete appends a
-- marker). Triggers do this, so writes made with raw SQL are versioned too, and
-- no agent can choose a version or edit history. On top of that:
--   memory_put() / memory_delete()  optimistic compare-and-set writes
--   memory_history() / memory_as_of() / memory_at_version()  reads of the past
--   memory_undo()  restores an earlier state as a new version (so undo can be undone)
-- An expected_version names the live version; 0 means "the key must not exist".
-- Conflicts raise SQLSTATE SW409, missing keys or versions SW404.
ALTER TABLE swarmpg.memory ADD COLUMN IF NOT EXISTS version bigint NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS swarmpg.memory_history (
  agent_id uuid NOT NULL,
  org_id uuid NOT NULL,
  key text NOT NULL,
  version bigint NOT NULL,
  value jsonb,                               -- NULL for a delete
  shared boolean NOT NULL DEFAULT false,
  deleted boolean NOT NULL DEFAULT false,
  changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (agent_id, key, version)
);
-- Rows written before versioning existed get their version-1 entry once.
INSERT INTO swarmpg.memory_history(agent_id, org_id, key, version, value, shared, changed_at)
SELECT m.agent_id, m.org_id, m.key, m.version, m.value, m.shared, m.updated_at
FROM swarmpg.memory m
WHERE NOT EXISTS (SELECT 1 FROM swarmpg.memory_history h WHERE h.agent_id = m.agent_id AND h.key = m.key)
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION swarmpg.memory_version() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = swarmpg, pg_catalog AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.agent_id <> OLD.agent_id OR NEW.org_id <> OLD.org_id OR NEW.key <> OLD.key THEN
      RAISE EXCEPTION 'a memory key cannot be renamed or moved; write a new key instead'
        USING ERRCODE = 'check_violation';
    END IF;
    NEW.version := OLD.version + 1;
  ELSE
    -- after a delete, numbering continues past the delete marker
    NEW.version := coalesce((SELECT max(h.version) FROM memory_history h
                             WHERE h.agent_id = NEW.agent_id AND h.key = NEW.key), 0) + 1;
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION swarmpg.memory_record() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = swarmpg, pg_catalog AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    INSERT INTO memory_history(agent_id, org_id, key, version, value, shared, deleted)
    VALUES (OLD.agent_id, OLD.org_id, OLD.key, OLD.version + 1, NULL, OLD.shared, true);
    RETURN OLD;
  END IF;
  INSERT INTO memory_history(agent_id, org_id, key, version, value, shared)
  VALUES (NEW.agent_id, NEW.org_id, NEW.key, NEW.version, NEW.value, NEW.shared);
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS memory_version_trg ON swarmpg.memory;
CREATE TRIGGER memory_version_trg BEFORE INSERT OR UPDATE ON swarmpg.memory
  FOR EACH ROW EXECUTE FUNCTION swarmpg.memory_version();
DROP TRIGGER IF EXISTS memory_record_trg ON swarmpg.memory;
CREATE TRIGGER memory_record_trg AFTER INSERT OR UPDATE OR DELETE ON swarmpg.memory
  FOR EACH ROW EXECUTE FUNCTION swarmpg.memory_record();

-- The functions below run with the caller's rights, so row-level security
-- confines every one of them to the caller's own keys.
CREATE OR REPLACE FUNCTION swarmpg.memory_conflict(p_key text, p_expected bigint) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE cur bigint;
BEGIN
  SELECT m.version INTO cur FROM swarmpg.memory m
  WHERE m.agent_id = swarmpg.current_agent() AND m.key = p_key;
  RAISE EXCEPTION 'version conflict on memory key "%"', p_key
    USING ERRCODE = 'SW409',
          DETAIL = format('current_version=%s expected_version=%s', coalesce(cur, 0), p_expected),
          HINT = 'Read the key again and retry with its current version; 0 means the key must not exist.';
END $$;

CREATE OR REPLACE FUNCTION swarmpg.memory_put(p_key text, p_value jsonb, p_shared boolean DEFAULT false,
                                              p_expected_version bigint DEFAULT NULL)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v bigint;
BEGIN
  IF p_expected_version IS NULL THEN
    INSERT INTO swarmpg.memory(agent_id, org_id, key, value, shared)
    VALUES (swarmpg.current_agent(), swarmpg.current_org(), p_key, p_value, p_shared)
    ON CONFLICT (agent_id, key) DO UPDATE SET value = EXCLUDED.value, shared = EXCLUDED.shared
    RETURNING version INTO v;
  ELSIF p_expected_version = 0 THEN
    INSERT INTO swarmpg.memory(agent_id, org_id, key, value, shared)
    VALUES (swarmpg.current_agent(), swarmpg.current_org(), p_key, p_value, p_shared)
    ON CONFLICT (agent_id, key) DO NOTHING
    RETURNING version INTO v;
  ELSE
    -- a concurrent writer that got there first leaves no row matching the version
    UPDATE swarmpg.memory SET value = p_value, shared = p_shared
    WHERE agent_id = swarmpg.current_agent() AND key = p_key AND version = p_expected_version
    RETURNING version INTO v;
  END IF;
  IF v IS NULL THEN
    PERFORM swarmpg.memory_conflict(p_key, p_expected_version);
  END IF;
  RETURN v;
END $$;

-- Returns the version of the delete marker.
CREATE OR REPLACE FUNCTION swarmpg.memory_delete(p_key text, p_expected_version bigint DEFAULT NULL)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v bigint;
BEGIN
  DELETE FROM swarmpg.memory
  WHERE agent_id = swarmpg.current_agent() AND key = p_key
    AND (p_expected_version IS NULL OR version = p_expected_version)
  RETURNING version + 1 INTO v;
  IF v IS NULL AND p_expected_version IS NULL THEN
    RAISE EXCEPTION 'memory key "%" not found', p_key USING ERRCODE = 'SW404';
  END IF;
  IF v IS NULL THEN
    PERFORM swarmpg.memory_conflict(p_key, p_expected_version);
  END IF;
  RETURN v;
END $$;

CREATE OR REPLACE FUNCTION swarmpg.memory_history(p_key text, p_limit int DEFAULT 50)
RETURNS TABLE (version bigint, value jsonb, shared boolean, deleted boolean, changed_at timestamptz)
LANGUAGE sql STABLE AS $$
  SELECT h.version, h.value, h.shared, h.deleted, h.changed_at FROM swarmpg.memory_history h
  WHERE h.agent_id = swarmpg.current_agent() AND h.key = p_key
  ORDER BY h.version DESC LIMIT greatest(1, least(coalesce(p_limit, 50), 1000))
$$;

CREATE OR REPLACE FUNCTION swarmpg.memory_as_of(p_key text, p_at timestamptz)
RETURNS TABLE (version bigint, value jsonb, shared boolean, deleted boolean, changed_at timestamptz)
LANGUAGE sql STABLE AS $$
  SELECT h.version, h.value, h.shared, h.deleted, h.changed_at FROM swarmpg.memory_history h
  WHERE h.agent_id = swarmpg.current_agent() AND h.key = p_key AND h.changed_at <= p_at
  ORDER BY h.version DESC LIMIT 1
$$;

CREATE OR REPLACE FUNCTION swarmpg.memory_at_version(p_key text, p_version bigint)
RETURNS TABLE (version bigint, value jsonb, shared boolean, deleted boolean, changed_at timestamptz)
LANGUAGE sql STABLE AS $$
  SELECT h.version, h.value, h.shared, h.deleted, h.changed_at FROM swarmpg.memory_history h
  WHERE h.agent_id = swarmpg.current_agent() AND h.key = p_key AND h.version = p_version
$$;

-- Restore the state before the latest change, or the state at p_to_version, as
-- a new version. Restoring a "did not exist yet" or deleted state deletes the key.
CREATE OR REPLACE FUNCTION swarmpg.memory_undo(p_key text, p_to_version bigint DEFAULT NULL,
                                               p_expected_version bigint DEFAULT NULL)
RETURNS TABLE (version bigint, restored_version bigint, deleted boolean)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
  me uuid := swarmpg.current_agent();
  live bigint;
  latest bigint;
  target swarmpg.memory_history%ROWTYPE;
BEGIN
  SELECT m.version INTO live FROM swarmpg.memory m WHERE m.agent_id = me AND m.key = p_key FOR UPDATE;
  IF p_expected_version IS NOT NULL AND coalesce(live, 0) <> p_expected_version THEN
    PERFORM swarmpg.memory_conflict(p_key, p_expected_version);
  END IF;
  SELECT max(h.version) INTO latest FROM swarmpg.memory_history h WHERE h.agent_id = me AND h.key = p_key;
  IF latest IS NULL THEN
    RAISE EXCEPTION 'memory key "%" has no history', p_key USING ERRCODE = 'SW404';
  END IF;
  IF p_to_version IS NULL THEN
    SELECT h.* INTO target FROM swarmpg.memory_history h
    WHERE h.agent_id = me AND h.key = p_key AND h.version < latest
    ORDER BY h.version DESC LIMIT 1;
  ELSE
    SELECT h.* INTO target FROM swarmpg.memory_history h
    WHERE h.agent_id = me AND h.key = p_key AND h.version = p_to_version;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'memory key "%" has no version %', p_key, p_to_version USING ERRCODE = 'SW404';
    END IF;
  END IF;

  IF target.version IS NULL OR target.deleted THEN
    IF live IS NULL THEN
      RETURN QUERY SELECT latest, target.version, true;   -- already absent
      RETURN;
    END IF;
    DELETE FROM swarmpg.memory m WHERE m.agent_id = me AND m.key = p_key;
    RETURN QUERY SELECT live + 1, target.version, true;
    RETURN;
  END IF;
  INSERT INTO swarmpg.memory(agent_id, org_id, key, value, shared)
  VALUES (me, swarmpg.current_org(), p_key, target.value, target.shared)
  ON CONFLICT (agent_id, key) DO UPDATE SET value = EXCLUDED.value, shared = EXCLUDED.shared;
  RETURN QUERY SELECT m.version, target.version, false FROM swarmpg.memory m
               WHERE m.agent_id = me AND m.key = p_key;
END $$;

-- General-purpose per-agent document store: the supported substitute for
-- agent-created tables. Agents get arbitrary JSON in named collections without
-- owning any database object, so there is nothing to leak across the pool.
CREATE TABLE IF NOT EXISTS swarmpg.documents (
  agent_id uuid NOT NULL,
  org_id uuid NOT NULL,
  collection text NOT NULL,
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  body jsonb NOT NULL DEFAULT '{}',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (agent_id, collection, id)
);
CREATE INDEX IF NOT EXISTS documents_collection_idx ON swarmpg.documents(agent_id, collection);

-- Append-only, hash-chained event journal: the agent's own record of its work.
-- (Tamper-evident against later edits, but agent-authored; the system's own
--  record of executed statements is swarmpg.audit above.)
CREATE TABLE IF NOT EXISTS swarmpg.events (
  id bigserial PRIMARY KEY,
  agent_id uuid NOT NULL,
  org_id uuid NOT NULL,
  seq bigint NOT NULL,                 -- per-agent monotonic position in the chain
  kind text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  prev_hash bytea,
  hash bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (agent_id, seq)
);
CREATE INDEX IF NOT EXISTS events_agent_idx ON swarmpg.events(agent_id, seq);
DO $$ BEGIN
  ALTER TABLE swarmpg.events ADD COLUMN IF NOT EXISTS seq bigint;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Per-agent chain head. Serializes concurrent appends (SELECT ... FOR UPDATE)
-- so the hash chain is a line, never a fork, under concurrency.
CREATE TABLE IF NOT EXISTS swarmpg.event_heads (
  agent_id uuid PRIMARY KEY,
  last_hash bytea,
  seq bigint NOT NULL DEFAULT 0
);

-- SECURITY DEFINER so the chain head (swarmpg.event_heads, which agents have no
-- privilege on) can be read/updated under a row lock on the agent's behalf.
-- search_path is pinned so the definer context cannot be hijacked.
CREATE OR REPLACE FUNCTION swarmpg.events_chain() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = swarmpg, pg_catalog AS $$
DECLARE prev bytea; nextseq bigint;
BEGIN
  -- Take (and create) the per-agent head row under a row lock. Concurrent
  -- inserts for the same agent serialize here, so prev_hash/seq never fork.
  INSERT INTO swarmpg.event_heads(agent_id, last_hash, seq)
    VALUES (NEW.agent_id, NULL, 0)
    ON CONFLICT (agent_id) DO NOTHING;
  SELECT last_hash, seq INTO prev, nextseq
    FROM swarmpg.event_heads WHERE agent_id = NEW.agent_id FOR UPDATE;
  nextseq := nextseq + 1;

  NEW.seq := nextseq;
  NEW.prev_hash := prev;
  -- Hash binds the agent, the position and the content so the chain is
  -- independently verifiable and order-stable. jsonb::text is canonical
  -- (normalized key order/whitespace); we avoid timestamp text formatting,
  -- which is DateStyle/TimeZone dependent.
  NEW.hash := sha256(
    coalesce(prev, '\x'::bytea) ||
    convert_to(NEW.agent_id::text || ':' || nextseq::text || ':' ||
               NEW.kind || ':' || NEW.payload::text, 'UTF8'));

  UPDATE swarmpg.event_heads
     SET last_hash = NEW.hash, seq = nextseq
   WHERE agent_id = NEW.agent_id;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS events_chain_trg ON swarmpg.events;
CREATE TRIGGER events_chain_trg BEFORE INSERT ON swarmpg.events
  FOR EACH ROW EXECUTE FUNCTION swarmpg.events_chain();

CREATE OR REPLACE FUNCTION swarmpg.events_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'swarmpg.events is append-only'; END $$;
DROP TRIGGER IF EXISTS events_immutable_trg ON swarmpg.events;
CREATE TRIGGER events_immutable_trg BEFORE UPDATE OR DELETE OR TRUNCATE ON swarmpg.events
  FOR EACH STATEMENT EXECUTE FUNCTION swarmpg.events_immutable();
-- Also keep a row-level guard for UPDATE/DELETE (TRUNCATE is statement-level only).
DROP TRIGGER IF EXISTS events_immutable_row_trg ON swarmpg.events;
CREATE TRIGGER events_immutable_row_trg BEFORE UPDATE OR DELETE ON swarmpg.events
  FOR EACH ROW EXECUTE FUNCTION swarmpg.events_immutable();

-- ---------------------------------------------------------------------------
-- Row-level security. Separate per-command policies so that org-shared rows are
-- READ-only to peers: a peer can see a shared memory row but cannot UPDATE or
-- DELETE it (only the owner can).
-- ---------------------------------------------------------------------------
ALTER TABLE swarmpg.memory ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.memory FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS memory_isolation ON swarmpg.memory;   -- legacy combined policy
DROP POLICY IF EXISTS memory_select ON swarmpg.memory;
DROP POLICY IF EXISTS memory_insert ON swarmpg.memory;
DROP POLICY IF EXISTS memory_update ON swarmpg.memory;
DROP POLICY IF EXISTS memory_delete ON swarmpg.memory;
CREATE POLICY memory_select ON swarmpg.memory FOR SELECT
  USING (agent_id = swarmpg.current_agent()
         OR (shared AND org_id = swarmpg.current_org()));
CREATE POLICY memory_insert ON swarmpg.memory FOR INSERT
  WITH CHECK (agent_id = swarmpg.current_agent() AND org_id = swarmpg.current_org());
CREATE POLICY memory_update ON swarmpg.memory FOR UPDATE
  USING (agent_id = swarmpg.current_agent())
  WITH CHECK (agent_id = swarmpg.current_agent() AND org_id = swarmpg.current_org());
CREATE POLICY memory_delete ON swarmpg.memory FOR DELETE
  USING (agent_id = swarmpg.current_agent());

ALTER TABLE swarmpg.memory_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.memory_history FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS memory_history_select ON swarmpg.memory_history;
CREATE POLICY memory_history_select ON swarmpg.memory_history FOR SELECT
  USING (agent_id = swarmpg.current_agent());

ALTER TABLE swarmpg.documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.documents FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS documents_isolation ON swarmpg.documents;
CREATE POLICY documents_isolation ON swarmpg.documents
  USING (agent_id = swarmpg.current_agent())
  WITH CHECK (agent_id = swarmpg.current_agent() AND org_id = swarmpg.current_org());

ALTER TABLE swarmpg.events ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS events_isolation ON swarmpg.events;
DROP POLICY IF EXISTS events_select ON swarmpg.events;
DROP POLICY IF EXISTS events_insert ON swarmpg.events;
CREATE POLICY events_select ON swarmpg.events FOR SELECT
  USING (agent_id = swarmpg.current_agent());
CREATE POLICY events_insert ON swarmpg.events FOR INSERT
  WITH CHECK (agent_id = swarmpg.current_agent() AND org_id = swarmpg.current_org());

GRANT SELECT, INSERT, UPDATE, DELETE ON swarmpg.memory TO agent_runtime;
GRANT SELECT ON swarmpg.memory_history TO agent_runtime;   -- written only by the memory triggers
GRANT SELECT, INSERT, UPDATE, DELETE ON swarmpg.documents TO agent_runtime;
GRANT SELECT, INSERT ON swarmpg.events TO agent_runtime;
-- USAGE is enough for the id default; SELECT would reveal the fleet-wide event count.
GRANT USAGE ON SEQUENCE swarmpg.events_id_seq TO agent_runtime;
REVOKE SELECT ON SEQUENCE swarmpg.events_id_seq FROM agent_runtime;
-- agent_runtime gets NO privileges on orgs, agents, audit, event_heads.
REVOKE ALL ON swarmpg.event_heads FROM PUBLIC;

-- Defaults applied to every agent session regardless of gateway bugs.
ALTER ROLE agent_runtime SET search_path = swarmpg;
ALTER ROLE agent_runtime SET row_security = on;         -- cannot be a table owner, but be explicit
-- On the SwarmPG kernel: refuse to run any plan until a principal is sealed,
-- and cap every agent statement. The gateway lowers the budgets to each agent's
-- own per transaction; neither it nor the agent can raise them past these
-- ceilings. agent_max_rows also caps the rows one statement may modify.
DO $$ BEGIN
  EXECUTE 'ALTER ROLE agent_runtime SET agent_require_principal = on';
  EXECUTE 'ALTER ROLE agent_runtime SET agent_max_rows = 100001';
  EXECUTE 'ALTER ROLE agent_runtime SET agent_max_cost = 1e9';
EXCEPTION WHEN undefined_object OR syntax_error THEN
  RAISE NOTICE 'SwarmPG kernel settings not available (stock Postgres); relying on gateway + RLS';
END $$;

-- ---------------------------------------------------------------------------
-- Hardening: agents run untrusted LLM-written SQL under a shared role, so every
-- cross-tenant and fleet-wide lever stock Postgres exposes to a plain role must
-- be revoked. (On the SwarmPG kernel the sealed principal + tenant-scoped access
-- path replace most of this; on stock Postgres these REVOKEs are the boundary.)
-- ---------------------------------------------------------------------------

-- NOTE: these REVOKE ... FROM PUBLIC statements are deliberately from PUBLIC,
-- not from agent_runtime. A plain role inherits EXECUTE on these functions via
-- the default PUBLIC grant, so REVOKE ... FROM agent_runtime leaves the PUBLIC
-- grant intact and the function still callable (verified). On a dedicated agent
-- cluster that is the right posture; on a shared cluster, re-grant to specific
-- non-agent roles after this file runs.

-- No way to re-establish identity via the only SQL route to it.
REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) FROM PUBLIC;
-- No anonymous code / arbitrary function bodies for agents. (These two are
-- granted to PUBLIC by default; keep them available to the admin owner so
-- operator tooling and the DO blocks below keep working. agent_runtime excluded.)
REVOKE USAGE ON LANGUAGE plpgsql FROM PUBLIC;
REVOKE USAGE ON LANGUAGE sql FROM PUBLIC;
DO $$
BEGIN
  EXECUTE format('GRANT USAGE ON LANGUAGE plpgsql TO %I', current_user);
  EXECUTE format('GRANT USAGE ON LANGUAGE sql TO %I', current_user);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- No cross-agent denial of service through shared-role signalling: one agent
-- must not be able to cancel or terminate another agent's backend or the pool.
DO $$
DECLARE fn text;
BEGIN
  FOR fn IN
    SELECT 'pg_catalog.' || p.proname || '(' ||
           pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pg_catalog'
      AND p.proname IN ('pg_terminate_backend','pg_cancel_backend')
  LOOP
    EXECUTE 'REVOKE EXECUTE ON FUNCTION ' || fn || ' FROM PUBLIC';
  END LOOP;
END $$;

-- No session-wide advisory-lock contention or lock-table exhaustion across the
-- shared pool. Revoke the whole family.
DO $$
DECLARE fn text;
BEGIN
  FOR fn IN
    SELECT 'pg_catalog.' || p.proname || '(' ||
           pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pg_catalog'
      AND p.proname LIKE 'pg_%advisory%'
  LOOP
    EXECUTE 'REVOKE EXECUTE ON FUNCTION ' || fn || ' FROM PUBLIC';
  END LOOP;
END $$;

-- No shared, unbounded large objects across the pool.
DO $$
DECLARE fn text;
BEGIN
  FOR fn IN
    SELECT 'pg_catalog.' || p.proname || '(' ||
           pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pg_catalog'
      AND (p.proname LIKE 'lo_%' OR p.proname IN ('loread','lowrite'))
  LOOP
    BEGIN EXECUTE 'REVOKE EXECUTE ON FUNCTION ' || fn || ' FROM PUBLIC';
    EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
END $$;

-- Hide other agents' in-flight SQL text. All agents share agent_runtime, and
-- Postgres shows same-role query text in pg_stat_activity, so the view and its
-- backing functions must be closed to the role entirely (on the kernel too: the
-- sealed principal does not change pg_stat_activity visibility).
DO $$ BEGIN
  EXECUTE 'REVOKE pg_read_all_stats FROM agent_runtime';
EXCEPTION WHEN OTHERS THEN NULL; END $$;
REVOKE SELECT ON pg_catalog.pg_stat_activity FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_activity TO pg_monitor;

-- The same goes for every statistics function. The pg_stat_* views call them,
-- and they expose fleet-wide counts (how many agents, rows written, audit
-- volume) to any role. Monitoring keeps working through pg_monitor.
DO $$
DECLARE fn regprocedure;
BEGIN
  FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p
            WHERE p.pronamespace = 'pg_catalog'::regnamespace AND p.proname LIKE 'pg\_stat\_get\_%'
  LOOP
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC', fn);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO pg_monitor', fn);
  END LOOP;
END $$;

-- No cross-agent messaging: pg_notify() would let one agent signal another
-- through the shared database (the gateway also refuses LISTEN and NOTIFY).
REVOKE EXECUTE ON FUNCTION pg_catalog.pg_notify(text, text) FROM PUBLIC;

-- Block ALL DDL and other object creation from agents (defense in depth; agents
-- have no DDL need now that scratch is gone). Keyed on session_user because the
-- gateway logs in directly as agent_runtime.
CREATE OR REPLACE FUNCTION swarmpg.block_agent_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF session_user = 'agent_runtime' THEN
    RAISE EXCEPTION 'swarmpg: % is not permitted for agents (agents have no DDL)', tg_tag
      USING ERRCODE = 'insufficient_privilege';
  END IF;
END $$;
DROP EVENT TRIGGER IF EXISTS swarmpg_block_ddl;
CREATE EVENT TRIGGER swarmpg_block_ddl ON ddl_command_start
  EXECUTE FUNCTION swarmpg.block_agent_ddl();
-- ddl_command_start does not fire for every utility (e.g. GRANT, SECURITY LABEL
-- in some versions); block those classes too.
DROP EVENT TRIGGER IF EXISTS swarmpg_block_ddl_rewrite;
CREATE EVENT TRIGGER swarmpg_block_ddl_rewrite ON table_rewrite
  EXECUTE FUNCTION swarmpg.block_agent_ddl();

-- ---------------------------------------------------------------------------
-- Durable org-scoped task coordination. Agent DML is confined to the functions
-- below: RLS protects reads, while state transitions use explicit authorization
-- and row locks. Keep this additive migration atomic when upgrading a live DB.
-- ---------------------------------------------------------------------------
BEGIN;

CREATE TABLE IF NOT EXISTS swarmpg.tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id uuid NOT NULL REFERENCES swarmpg.orgs(id),
  created_by uuid NOT NULL,
  queue text NOT NULL CHECK (length(queue) BETWEEN 1 AND 128),
  task_key text NOT NULL CHECK (length(task_key) BETWEEN 1 AND 256),
  payload jsonb NOT NULL CHECK (octet_length(payload::text) <= 65536),
  dependencies uuid[] NOT NULL DEFAULT '{}',
  inputs jsonb NOT NULL DEFAULT '[]',
  state text NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'claimed', 'completed', 'cancelled')),
  owner_agent_id uuid,
  fence bigint NOT NULL DEFAULT 0 CHECK (fence >= 0),
  lease_until timestamptz,
  attempts bigint NOT NULL DEFAULT 0,
  result jsonb CHECK (octet_length(result::text) <= 65536),
  revision bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  UNIQUE (org_id, queue, task_key),
  UNIQUE (org_id, id),
  CHECK (state <> 'claimed' OR (owner_agent_id IS NOT NULL AND lease_until IS NOT NULL)),
  CHECK ((state = 'completed') = (completed_at IS NOT NULL)),
  CHECK ((state = 'completed') = (result IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS tasks_claim_idx ON swarmpg.tasks(org_id, queue, created_at, id)
  WHERE state IN ('ready', 'claimed');

CREATE TABLE IF NOT EXISTS swarmpg.task_history (
  task_id uuid NOT NULL,
  org_id uuid NOT NULL,
  revision bigint NOT NULL,
  event text NOT NULL,
  actor_agent_id uuid,
  fence bigint NOT NULL,
  state text NOT NULL,
  owner_agent_id uuid,
  lease_until timestamptz,
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  detail jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (task_id, revision),
  FOREIGN KEY (org_id, task_id) REFERENCES swarmpg.tasks(org_id, id) ON DELETE CASCADE
);

-- A claim retry must never quietly claim a second task. The receipt, task
-- transition and history commit together, including an empty queue response.
CREATE TABLE IF NOT EXISTS swarmpg.task_claim_requests (
  org_id uuid NOT NULL REFERENCES swarmpg.orgs(id),
  agent_id uuid NOT NULL,
  request_id uuid NOT NULL,
  queue text NOT NULL,
  lease_ms integer NOT NULL,
  response jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id, agent_id, request_id)
);

ALTER TABLE swarmpg.tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.tasks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS task_read ON swarmpg.tasks;
CREATE POLICY task_read ON swarmpg.tasks FOR SELECT
  USING (org_id = swarmpg.current_org());
ALTER TABLE swarmpg.task_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.task_history FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS task_history_read ON swarmpg.task_history;
CREATE POLICY task_history_read ON swarmpg.task_history FOR SELECT
  USING (org_id = swarmpg.current_org());
ALTER TABLE swarmpg.task_claim_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.task_claim_requests FORCE ROW LEVEL SECURITY;
REVOKE ALL ON swarmpg.tasks, swarmpg.task_history, swarmpg.task_claim_requests FROM PUBLIC, agent_runtime;
GRANT SELECT ON swarmpg.tasks, swarmpg.task_history TO agent_runtime;

-- These helpers are private to the schema owner. In particular callers cannot
-- supply another agent identity to a privileged transition function.
CREATE OR REPLACE FUNCTION swarmpg._task_authorize(p_write boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE a swarmpg.agents%ROWTYPE;
BEGIN
  SELECT * INTO a FROM swarmpg.agents
  WHERE id = swarmpg.current_agent() AND org_id = swarmpg.current_org();
  IF NOT FOUND OR a.disabled OR (a.token_expires_at IS NOT NULL AND a.token_expires_at <= clock_timestamp()) THEN
    RAISE EXCEPTION 'task operation requires an enabled agent in the current organization' USING ERRCODE = '42501';
  END IF;
  IF p_write AND a.read_only THEN
    RAISE EXCEPTION 'read-only agents cannot change tasks' USING ERRCODE = '42501';
  END IF;
END $$;

-- Take this barrier only after task/input locks, not at request entry. An
-- administrator can therefore revoke an agent while it waits for work. Once
-- admitted here, the registry row remains share-locked through commit, ordering
-- the transition before a subsequent revocation. Expiry is still checked using
-- database wall time, including again at the final response boundary.
CREATE OR REPLACE FUNCTION swarmpg._task_authorize_locked(p_write boolean) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
BEGIN
  PERFORM 1 FROM swarmpg.agents
  WHERE id = swarmpg.current_agent() AND org_id = swarmpg.current_org() FOR SHARE;
  PERFORM swarmpg._task_authorize(p_write);
END $$;

-- Bound task receipts in SQL as well as in typed REST/MCP handlers. A raw SQL
-- call through /query must not commit a transition whose receipt would require
-- truncation. jsonb text includes insignificant spaces, making this conservative;
-- also account for escaping performed by Go's default JSON encoder.
CREATE OR REPLACE FUNCTION swarmpg._task_response(p_response jsonb, p_write boolean DEFAULT false) RETURNS jsonb
LANGUAGE plpgsql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE row_limit integer; byte_limit bigint; body text := p_response::text; encoded_bytes bigint;
BEGIN
  IF p_write THEN
    PERFORM swarmpg._task_authorize_locked(true);
  ELSE
    -- Read-only transactions cannot take row locks, and have no transition
    -- that needs serialization against an administrator's revocation.
    PERFORM swarmpg._task_authorize(false);
  END IF;
  SELECT max_rows, max_result_bytes INTO row_limit, byte_limit FROM swarmpg.agents
  WHERE id = swarmpg.current_agent() AND org_id = swarmpg.current_org();
  encoded_bytes := octet_length(body)
    + 5::bigint * (length(body) - length(translate(body, '<>&', '')))
    + 3::bigint * (length(body) - length(translate(body, chr(8232) || chr(8233), '')));
  IF row_limit < 1 OR (byte_limit > 0 AND encoded_bytes > byte_limit) THEN
    RAISE EXCEPTION 'complete task response exceeds the agent result budget; operation was not committed'
      USING ERRCODE = 'PT413', HINT = 'Raise the result budget before retrying; task receipts are never truncated.';
  END IF;
  RETURN p_response;
END $$;

CREATE OR REPLACE FUNCTION swarmpg._task_json(t swarmpg.tasks) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog, swarmpg, pg_temp AS $$
  SELECT to_jsonb(t) || jsonb_build_object('fence', t.fence::text)
$$;

CREATE OR REPLACE FUNCTION swarmpg._task_event(t swarmpg.tasks, p_event text,
                                               p_actor uuid, p_detail jsonb DEFAULT '{}') RETURNS void
LANGUAGE sql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
  INSERT INTO swarmpg.task_history(task_id, org_id, revision, event, actor_agent_id,
                                  fence, state, owner_agent_id, lease_until, detail)
  VALUES (t.id, t.org_id, t.revision, p_event, p_actor, t.fence, t.state,
          t.owner_agent_id, t.lease_until, p_detail)
$$;

CREATE OR REPLACE FUNCTION swarmpg._task_lease(p_lease_ms integer) RETURNS void
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog, swarmpg, pg_temp AS $$
BEGIN
  IF p_lease_ms IS NULL OR p_lease_ms < 100 OR p_lease_ms > 300000 THEN
    RAISE EXCEPTION 'lease_ms must be between 100 and 300000' USING ERRCODE = 'PT400';
  END IF;
END $$;

-- Normalize the input set so ordering and UUID spelling do not affect enqueue
-- replay. Versions are integral positive JSON numbers, not floating-point IDs.
CREATE OR REPLACE FUNCTION swarmpg._task_inputs(p_inputs jsonb) RETURNS jsonb
LANGUAGE plpgsql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE item jsonb; producer uuid; input_key text; input_version bigint; normalized jsonb := '[]';
BEGIN
  IF p_inputs IS NULL OR jsonb_typeof(p_inputs) <> 'array' OR jsonb_array_length(p_inputs) > 64 THEN
    RAISE EXCEPTION 'inputs must be an array of at most 64 producer/key/version references' USING ERRCODE = 'PT400';
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(p_inputs) LOOP
    IF jsonb_typeof(item) <> 'object' OR NOT (item ?& ARRAY['agent_id','key','version'])
       OR item - ARRAY['agent_id','key','version'] <> '{}'::jsonb
       OR jsonb_typeof(item->'agent_id') <> 'string' OR jsonb_typeof(item->'key') <> 'string'
       OR jsonb_typeof(item->'version') <> 'number'
       OR (item->>'version') !~ '^[1-9][0-9]*$' THEN
      RAISE EXCEPTION 'each input requires exactly agent_id, key and a positive integer version' USING ERRCODE = 'PT400';
    END IF;
    BEGIN
      producer := (item->>'agent_id')::uuid;
      input_version := (item->>'version')::bigint;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
      RAISE EXCEPTION 'invalid input agent_id or version' USING ERRCODE = 'PT400';
    END;
    input_key := item->>'key';
    IF length(input_key) NOT BETWEEN 1 AND 256 THEN
      RAISE EXCEPTION 'input key must contain 1 to 256 characters' USING ERRCODE = 'PT400';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(normalized) x
               WHERE x->>'agent_id' = producer::text AND x->>'key' = input_key) THEN
      RAISE EXCEPTION 'an input producer/key may appear only once' USING ERRCODE = 'PT400';
    END IF;
    normalized := normalized || jsonb_build_array(jsonb_build_object('agent_id', producer,
                                                    'key', input_key, 'version', input_version));
  END LOOP;
  RETURN coalesce((SELECT jsonb_agg(x ORDER BY x->>'agent_id', x->>'key')
                   FROM jsonb_array_elements(normalized) x), '[]'::jsonb);
END $$;

-- FOR SHARE holds every declared live input stable until completion commits.
-- The same lock order in every task reduces lock-order inversions. Visibility
-- is rechecked after waiting for a concurrent publisher, not inferred from an
-- earlier unlocked read. A revoked share is an input change too.
CREATE OR REPLACE FUNCTION swarmpg._task_check_inputs(p_inputs jsonb, p_enqueuing boolean DEFAULT false) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE item jsonb; current_version bigint;
BEGIN
  FOR item IN SELECT value FROM jsonb_array_elements(p_inputs)
              ORDER BY value->>'agent_id', value->>'key' LOOP
    SELECT m.version INTO current_version FROM swarmpg.memory m
    WHERE m.agent_id = (item->>'agent_id')::uuid AND m.key = item->>'key'
      AND m.org_id = swarmpg.current_org()
      AND (m.agent_id = swarmpg.current_agent() OR m.shared)
    FOR SHARE;
    IF NOT FOUND AND p_enqueuing THEN
      RAISE EXCEPTION 'task input not found' USING ERRCODE = 'PT404';
    END IF;
    IF NOT FOUND OR current_version <> (item->>'version')::bigint THEN
      RAISE EXCEPTION 'a declared task input is unavailable or changed' USING ERRCODE = 'PT412',
        HINT = 'Read the selected producers again and enqueue a new task with current input versions.';
    END IF;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION swarmpg._task_require_claim(t swarmpg.tasks, p_fence bigint) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, swarmpg, pg_temp AS $$
BEGIN
  IF p_fence IS NULL OR p_fence < 1 THEN
    RAISE EXCEPTION 'fence must be a positive integer' USING ERRCODE = 'PT400';
  END IF;
  IF t.state <> 'claimed' OR t.owner_agent_id IS DISTINCT FROM swarmpg.current_agent() OR t.fence <> p_fence THEN
    RAISE EXCEPTION 'task claim is no longer owned by this agent and fence' USING ERRCODE = 'PT410';
  END IF;
  IF t.lease_until <= clock_timestamp() THEN
    RAISE EXCEPTION 'task lease has expired' USING ERRCODE = 'PT411';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_enqueue(p_queue text, p_key text, p_payload jsonb,
                        p_dependencies uuid[] DEFAULT '{}', p_inputs jsonb DEFAULT '[]') RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE; deps uuid[]; input_set jsonb; dependency uuid; inserted boolean;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  IF p_queue IS NULL OR length(p_queue) NOT BETWEEN 1 AND 128
     OR p_key IS NULL OR length(p_key) NOT BETWEEN 1 AND 256
     OR p_payload IS NULL OR octet_length(p_payload::text) > 65536 THEN
    RAISE EXCEPTION 'queue/key required (128/256 characters); payload must be at most 64 KiB' USING ERRCODE = 'PT400';
  END IF;
  IF p_dependencies IS NULL OR cardinality(p_dependencies) > 64
     OR coalesce(array_ndims(p_dependencies), 1) <> 1 OR array_position(p_dependencies, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'dependencies must contain at most 64 distinct non-null task IDs' USING ERRCODE = 'PT400';
  END IF;
  SELECT coalesce(array_agg(DISTINCT x ORDER BY x), '{}'::uuid[]) INTO deps FROM unnest(p_dependencies) x;
  IF cardinality(deps) <> cardinality(p_dependencies) THEN
    RAISE EXCEPTION 'duplicate task dependency' USING ERRCODE = 'PT400';
  END IF;
  input_set := swarmpg._task_inputs(p_inputs);

  INSERT INTO swarmpg.tasks(org_id, created_by, queue, task_key, payload, dependencies, inputs)
  VALUES (swarmpg.current_org(), swarmpg.current_agent(), p_queue, p_key, p_payload, deps, input_set)
  ON CONFLICT (org_id, queue, task_key) DO NOTHING RETURNING * INTO t;
  inserted := FOUND;
  IF NOT inserted THEN
    SELECT * INTO t FROM swarmpg.tasks
    WHERE org_id = swarmpg.current_org() AND queue = p_queue AND task_key = p_key FOR SHARE;
    IF t.created_by <> swarmpg.current_agent() OR t.payload <> p_payload
       OR t.dependencies <> deps OR t.inputs <> input_set THEN
      RAISE EXCEPTION 'task key already identifies a different enqueue request' USING ERRCODE = 'PT409';
    END IF;
    RETURN swarmpg._task_response(jsonb_build_object('task', swarmpg._task_json(t), 'replayed', true), true);
  END IF;
  -- Dependencies can only name pre-existing tasks: no mutation can later add a
  -- back edge, and every dependency belongs to the same organization.
  FOREACH dependency IN ARRAY deps LOOP
    PERFORM 1 FROM swarmpg.tasks WHERE id = dependency AND org_id = swarmpg.current_org() AND id <> t.id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'task dependency not found in this organization' USING ERRCODE = 'PT404';
    END IF;
  END LOOP;
  PERFORM swarmpg._task_check_inputs(input_set, true);
  PERFORM swarmpg._task_authorize_locked(true);
  PERFORM swarmpg._task_event(t, 'enqueued', swarmpg.current_agent());
  RETURN swarmpg._task_response(jsonb_build_object('task', swarmpg._task_json(t), 'replayed', false), true);
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_get(p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_authorize(false);
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org();
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  RETURN swarmpg._task_response(swarmpg._task_json(t));
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_claim(p_queue text, p_request_id uuid,
                                               p_lease_ms integer DEFAULT 30000) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE; receipt swarmpg.task_claim_requests%ROWTYPE; v_response jsonb;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  PERFORM swarmpg._task_lease(p_lease_ms);
  IF p_queue IS NULL OR length(p_queue) NOT BETWEEN 1 AND 128 OR p_request_id IS NULL THEN
    RAISE EXCEPTION 'queue and claim request_id are required' USING ERRCODE = 'PT400';
  END IF;
  INSERT INTO swarmpg.task_claim_requests(org_id, agent_id, request_id, queue, lease_ms)
  VALUES (swarmpg.current_org(), swarmpg.current_agent(), p_request_id, p_queue, p_lease_ms)
  ON CONFLICT (org_id, agent_id, request_id) DO NOTHING;
  SELECT * INTO receipt FROM swarmpg.task_claim_requests
  WHERE org_id = swarmpg.current_org() AND agent_id = swarmpg.current_agent() AND request_id = p_request_id FOR UPDATE;
  IF receipt.queue <> p_queue OR receipt.lease_ms <> p_lease_ms THEN
    RAISE EXCEPTION 'claim request_id already identifies different parameters' USING ERRCODE = 'PT409';
  END IF;
  IF receipt.response IS NOT NULL THEN
    RETURN swarmpg._task_response(receipt.response || jsonb_build_object('replayed', true), true);
  END IF;
  SELECT candidate.* INTO t FROM swarmpg.tasks candidate
  WHERE candidate.org_id = swarmpg.current_org() AND candidate.queue = p_queue
    AND (candidate.state = 'ready' OR (candidate.state = 'claimed' AND candidate.lease_until <= clock_timestamp()))
    AND NOT EXISTS (
      SELECT 1 FROM unnest(candidate.dependencies) dep
      LEFT JOIN swarmpg.tasks parent ON parent.id = dep AND parent.org_id = candidate.org_id
      WHERE parent.state IS DISTINCT FROM 'completed')
  ORDER BY candidate.created_at, candidate.id LIMIT 1 FOR UPDATE OF candidate SKIP LOCKED;
  IF FOUND THEN
    PERFORM swarmpg._task_authorize_locked(true);
    UPDATE swarmpg.tasks SET state = 'claimed', owner_agent_id = swarmpg.current_agent(),
      fence = fence + 1, lease_until = clock_timestamp() + p_lease_ms * interval '1 millisecond',
      attempts = attempts + 1, revision = revision + 1, updated_at = clock_timestamp()
    WHERE id = t.id RETURNING * INTO t;
    PERFORM swarmpg._task_event(t, 'claimed', swarmpg.current_agent(), jsonb_build_object('request_id', p_request_id));
    v_response := jsonb_build_object('task', swarmpg._task_json(t), 'replayed', false);
  ELSE
    v_response := jsonb_build_object('task', NULL, 'replayed', false);
  END IF;
  UPDATE swarmpg.task_claim_requests SET response = v_response
  WHERE org_id = swarmpg.current_org() AND agent_id = swarmpg.current_agent() AND request_id = p_request_id;
  RETURN swarmpg._task_response(v_response, true);
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_renew(p_id uuid, p_fence bigint,
                                               p_lease_ms integer DEFAULT 30000) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  PERFORM swarmpg._task_lease(p_lease_ms);
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org() FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  PERFORM swarmpg._task_authorize_locked(true);
  PERFORM swarmpg._task_require_claim(t, p_fence);
  UPDATE swarmpg.tasks SET lease_until = greatest(lease_until, clock_timestamp() + p_lease_ms * interval '1 millisecond'),
    revision = revision + 1, updated_at = clock_timestamp() WHERE id = t.id RETURNING * INTO t;
  PERFORM swarmpg._task_event(t, 'renewed', swarmpg.current_agent());
  RETURN swarmpg._task_response(swarmpg._task_json(t), true);
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_release(p_id uuid, p_fence bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org() FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  PERFORM swarmpg._task_authorize_locked(true);
  PERFORM swarmpg._task_require_claim(t, p_fence);
  UPDATE swarmpg.tasks SET state = 'ready', owner_agent_id = NULL, lease_until = NULL,
    revision = revision + 1, updated_at = clock_timestamp() WHERE id = t.id RETURNING * INTO t;
  PERFORM swarmpg._task_event(t, 'released', swarmpg.current_agent());
  RETURN swarmpg._task_response(swarmpg._task_json(t), true);
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_cancel(p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org() FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  PERFORM swarmpg._task_authorize_locked(true);
  IF t.created_by <> swarmpg.current_agent() THEN
    RAISE EXCEPTION 'only the task creator may cancel it' USING ERRCODE = '42501';
  END IF;
  IF t.state = 'completed' THEN
    RAISE EXCEPTION 'a completed task is immutable' USING ERRCODE = 'PT409';
  END IF;
  IF t.state = 'cancelled' THEN RETURN swarmpg._task_response(swarmpg._task_json(t), true); END IF;
  UPDATE swarmpg.tasks SET state = 'cancelled', owner_agent_id = NULL, lease_until = NULL,
    fence = fence + 1, revision = revision + 1, updated_at = clock_timestamp()
  WHERE id = t.id RETURNING * INTO t;
  PERFORM swarmpg._task_event(t, 'cancelled', swarmpg.current_agent());
  RETURN swarmpg._task_response(swarmpg._task_json(t), true);
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_history(p_id uuid, p_limit integer DEFAULT 50) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
BEGIN
  PERFORM swarmpg._task_authorize(false);
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'history limit must be between 1 and 1000' USING ERRCODE = 'PT400';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org()) THEN
    RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404';
  END IF;
  RETURN swarmpg._task_response(jsonb_build_object('events', coalesce((SELECT jsonb_agg(to_jsonb(h) || jsonb_build_object('fence', h.fence::text)
                                                               ORDER BY h.revision DESC)
    FROM (SELECT * FROM swarmpg.task_history WHERE task_id = p_id AND org_id = swarmpg.current_org()
          ORDER BY revision DESC LIMIT p_limit) h), '[]'::jsonb)));
END $$;

-- Administrative recovery is explicitly separate from agent authorization.
-- It never rewrites completed results or refreshes declared input snapshots.
CREATE OR REPLACE FUNCTION swarmpg.task_recover(p_id uuid, p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, swarmpg, pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE;
BEGIN
  IF p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 1000 THEN
    RAISE EXCEPTION 'recovery requires a reason of 1 to 1000 characters' USING ERRCODE = 'PT400';
  END IF;
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  IF t.state IN ('completed', 'cancelled') THEN
    RAISE EXCEPTION 'a terminal task cannot be recovered' USING ERRCODE = 'PT409';
  END IF;
  UPDATE swarmpg.tasks SET state = 'ready', owner_agent_id = NULL, lease_until = NULL,
    fence = fence + 1, revision = revision + 1, updated_at = clock_timestamp()
  WHERE id = t.id RETURNING * INTO t;
  PERFORM swarmpg._task_event(t, 'recovered', NULL, jsonb_build_object('reason', p_reason));
  RETURN swarmpg._task_json(t);
END $$;

-- Outbox intents commit with task completion. Delivery state never changes the
-- canonical completion receipt; retries compare the immutable effect set.
ALTER TABLE swarmpg.tasks ADD COLUMN IF NOT EXISTS completion_effects jsonb NOT NULL DEFAULT '[]';

CREATE TABLE IF NOT EXISTS swarmpg.outbox_messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id uuid NOT NULL,
  task_id uuid NOT NULL,
  key text NOT NULL,
  destination text NOT NULL,
  payload jsonb NOT NULL,
  idempotency_key text GENERATED ALWAYS AS ('swarmpg:' || org_id::text || ':' || id::text) STORED,
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','delivering','delivered','failed')),
  worker_id uuid,
  fence bigint NOT NULL DEFAULT 0 CHECK (fence >= 0),
  lease_until timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  total_attempts bigint NOT NULL DEFAULT 0 CHECK (total_attempts >= attempts),
  max_attempts integer NOT NULL DEFAULT 10 CHECK (max_attempts > 0),
  available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_error text,
  delivery_response jsonb,
  revision bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  delivered_at timestamptz,
  UNIQUE (task_id,key),
  UNIQUE (org_id,id),
  FOREIGN KEY (org_id,task_id) REFERENCES swarmpg.tasks(org_id,id) ON DELETE CASCADE,
  CHECK (state <> 'delivering' OR (worker_id IS NOT NULL AND lease_until IS NOT NULL)),
  CHECK ((state = 'delivered') = (delivered_at IS NOT NULL)),
  CHECK ((state = 'delivered') = (delivery_response IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS outbox_claim_idx ON swarmpg.outbox_messages(org_id,destination,available_at,created_at,id)
  WHERE state IN ('pending','delivering');
CREATE TABLE IF NOT EXISTS swarmpg.outbox_history (
  message_id uuid NOT NULL,
  org_id uuid NOT NULL,
  revision bigint NOT NULL,
  event text NOT NULL,
  worker_id uuid,
  fence bigint NOT NULL,
  state text NOT NULL,
  attempts integer NOT NULL,
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  detail jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (message_id,revision),
  FOREIGN KEY (org_id,message_id) REFERENCES swarmpg.outbox_messages(org_id,id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS swarmpg.outbox_claim_requests (
  org_id uuid NOT NULL REFERENCES swarmpg.orgs(id) ON DELETE CASCADE,
  worker_id uuid NOT NULL,
  request_id uuid NOT NULL,
  destination text NOT NULL,
  lease_ms integer NOT NULL,
  response jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id,worker_id,request_id)
);
ALTER TABLE swarmpg.outbox_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.outbox_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_read ON swarmpg.outbox_messages;
CREATE POLICY outbox_read ON swarmpg.outbox_messages FOR SELECT USING (org_id = swarmpg.current_org());
ALTER TABLE swarmpg.outbox_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.outbox_history FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_history_read ON swarmpg.outbox_history;
CREATE POLICY outbox_history_read ON swarmpg.outbox_history FOR SELECT USING (org_id = swarmpg.current_org());
ALTER TABLE swarmpg.outbox_claim_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE swarmpg.outbox_claim_requests FORCE ROW LEVEL SECURITY;
REVOKE ALL ON swarmpg.outbox_messages,swarmpg.outbox_history,swarmpg.outbox_claim_requests FROM PUBLIC,agent_runtime;
GRANT SELECT ON swarmpg.outbox_messages,swarmpg.outbox_history TO agent_runtime;

CREATE OR REPLACE FUNCTION swarmpg._task_effects(p_effects jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE e jsonb; normalized jsonb;
BEGIN
  IF p_effects IS NULL OR jsonb_typeof(p_effects) <> 'array' THEN
    RAISE EXCEPTION 'effects must be a JSON array' USING ERRCODE = 'PT400';
  END IF;
  IF jsonb_array_length(p_effects) > 32 THEN
    RAISE EXCEPTION 'at most 32 effects are allowed' USING ERRCODE = 'PT400';
  END IF;
  FOR e IN SELECT value FROM jsonb_array_elements(p_effects) LOOP
    IF jsonb_typeof(e) <> 'object' THEN
      RAISE EXCEPTION 'each effect must be an object' USING ERRCODE = 'PT400';
    END IF;
    IF NOT (e ?& ARRAY['key','destination','payload']) OR (e - ARRAY['key','destination','payload']) <> '{}'::jsonb
       OR jsonb_typeof(e->'key') <> 'string' OR jsonb_typeof(e->'destination') <> 'string'
       OR (e->>'key') COLLATE "C" !~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
       OR (e->>'destination') COLLATE "C" !~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
       OR octet_length((e->'payload')::text) > 65536 THEN
      RAISE EXCEPTION 'invalid effect fields or payload exceeds 64 KiB' USING ERRCODE = 'PT400';
    END IF;
  END LOOP;
  IF (SELECT count(*) <> count(DISTINCT value->>'key') FROM jsonb_array_elements(p_effects)) THEN
    RAISE EXCEPTION 'effect keys must be unique within a task' USING ERRCODE = 'PT400';
  END IF;
  SELECT coalesce(jsonb_agg(value ORDER BY (value->>'key') COLLATE "C"),'[]') INTO normalized
  FROM jsonb_array_elements(p_effects);
  IF octet_length(normalized::text) > 262144 THEN
    RAISE EXCEPTION 'normalized effects exceed 256 KiB' USING ERRCODE = 'PT400';
  END IF;
  RETURN normalized;
END $$;

CREATE OR REPLACE FUNCTION swarmpg._outbox_json(m swarmpg.outbox_messages) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog,swarmpg,pg_temp AS $$
  SELECT to_jsonb(m) || jsonb_build_object('fence',m.fence::text)
$$;
CREATE OR REPLACE FUNCTION swarmpg._outbox_event(m swarmpg.outbox_messages,p_event text,
  p_worker uuid,p_detail jsonb DEFAULT '{}') RETURNS void
LANGUAGE sql SET search_path = pg_catalog,swarmpg,pg_temp AS $$
  INSERT INTO swarmpg.outbox_history(message_id,org_id,revision,event,worker_id,fence,state,attempts,detail)
  VALUES (m.id,m.org_id,m.revision,p_event,p_worker,m.fence,m.state,m.attempts,p_detail)
$$;
CREATE OR REPLACE FUNCTION swarmpg._outbox_require_claim(m swarmpg.outbox_messages,p_worker uuid,p_fence bigint) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog,swarmpg,pg_temp AS $$
BEGIN
  IF p_worker IS NULL OR p_fence IS NULL OR p_fence < 1 THEN
    RAISE EXCEPTION 'worker_id and positive fence are required' USING ERRCODE = 'PT400';
  END IF;
  IF m.state <> 'delivering' OR m.worker_id IS DISTINCT FROM p_worker OR m.fence <> p_fence THEN
    RAISE EXCEPTION 'outbox claim is no longer current' USING ERRCODE = 'PT410';
  END IF;
  IF m.lease_until <= clock_timestamp() THEN
    RAISE EXCEPTION 'outbox lease has expired' USING ERRCODE = 'PT411';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION swarmpg.task_complete(p_id uuid,p_fence bigint,p_result jsonb,p_effects jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE t swarmpg.tasks%ROWTYPE; effects jsonb; e jsonb; m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_authorize(true);
  IF p_result IS NULL OR octet_length(p_result::text) > 65536 THEN
    RAISE EXCEPTION 'result must be a JSON value of at most 64 KiB' USING ERRCODE = 'PT400';
  END IF;
  IF p_fence IS NULL OR p_fence < 1 THEN
    RAISE EXCEPTION 'fence must be a positive integer' USING ERRCODE = 'PT400';
  END IF;
  effects := swarmpg._task_effects(p_effects);
  SELECT * INTO t FROM swarmpg.tasks WHERE id = p_id AND org_id = swarmpg.current_org() FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404'; END IF;
  IF t.state = 'completed' AND t.owner_agent_id = swarmpg.current_agent() AND t.fence = p_fence THEN
    IF t.result IS DISTINCT FROM p_result OR t.completion_effects IS DISTINCT FROM effects THEN
      RAISE EXCEPTION 'completed task already contains a different result or effect set' USING ERRCODE = 'PT409';
    END IF;
    RETURN swarmpg._task_response(jsonb_build_object('task',swarmpg._task_json(t),'replayed',true),true);
  END IF;
  PERFORM swarmpg._task_require_claim(t,p_fence);
  PERFORM swarmpg._task_check_inputs(t.inputs);
  PERFORM swarmpg._task_authorize_locked(true);
  PERFORM swarmpg._task_require_claim(t,p_fence);
  UPDATE swarmpg.tasks SET state='completed',result=p_result,completion_effects=effects,lease_until=NULL,
    completed_at=clock_timestamp(),updated_at=clock_timestamp(),revision=revision+1
  WHERE id=t.id RETURNING * INTO t;
  PERFORM swarmpg._task_event(t,'completed',swarmpg.current_agent());
  FOR e IN SELECT value FROM jsonb_array_elements(effects) LOOP
    INSERT INTO swarmpg.outbox_messages(org_id,task_id,key,destination,payload)
    VALUES (t.org_id,t.id,e->>'key',e->>'destination',e->'payload') RETURNING * INTO m;
    PERFORM swarmpg._outbox_event(m,'enqueued',NULL);
  END LOOP;
  RETURN swarmpg._task_response(jsonb_build_object('task',swarmpg._task_json(t),'replayed',false),true);
END $$;

-- No default on the four-argument overload: old SQL callers unambiguously
-- complete with an empty effect set, including their exact retry comparison.
CREATE OR REPLACE FUNCTION swarmpg.task_complete(p_id uuid,p_fence bigint,p_result jsonb) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
  SELECT swarmpg.task_complete(p_id,p_fence,p_result,'[]'::jsonb)
$$;
CREATE OR REPLACE FUNCTION swarmpg.task_outbox(p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
BEGIN
  PERFORM swarmpg._task_authorize(false);
  IF NOT EXISTS (SELECT 1 FROM swarmpg.tasks WHERE id=p_id AND org_id=swarmpg.current_org()) THEN
    RAISE EXCEPTION 'task not found' USING ERRCODE = 'PT404';
  END IF;
  RETURN swarmpg._task_response(jsonb_build_object('messages',coalesce((
    SELECT jsonb_agg(swarmpg._outbox_json(m) ORDER BY m.key COLLATE "C") FROM swarmpg.outbox_messages m
    WHERE m.task_id=p_id AND m.org_id=swarmpg.current_org()),'[]'::jsonb)));
END $$;

-- Dispatcher authority comes only from function ACLs (schema owner/admin).
-- A destination is a configured routing name, never an agent-supplied URL.
CREATE OR REPLACE FUNCTION swarmpg.outbox_claim(p_org uuid,p_destination text,p_worker uuid,p_request uuid,
  p_lease_ms integer DEFAULT 30000) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE; r swarmpg.outbox_claim_requests%ROWTYPE; receipt jsonb;
BEGIN
  PERFORM swarmpg._task_lease(p_lease_ms);
  IF p_org IS NULL OR p_worker IS NULL OR p_request IS NULL OR p_destination IS NULL
     OR p_destination COLLATE "C" !~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' THEN
    RAISE EXCEPTION 'organization, worker, request and valid destination are required' USING ERRCODE = 'PT400';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM swarmpg.orgs WHERE id=p_org) THEN
    RAISE EXCEPTION 'organization not found' USING ERRCODE = 'PT404';
  END IF;
  INSERT INTO swarmpg.outbox_claim_requests(org_id,worker_id,request_id,destination,lease_ms)
  VALUES (p_org,p_worker,p_request,p_destination,p_lease_ms) ON CONFLICT DO NOTHING;
  SELECT * INTO r FROM swarmpg.outbox_claim_requests
  WHERE org_id=p_org AND worker_id=p_worker AND request_id=p_request FOR UPDATE;
  IF r.destination <> p_destination OR r.lease_ms <> p_lease_ms THEN
    RAISE EXCEPTION 'claim request identifies different parameters' USING ERRCODE = 'PT409';
  END IF;
  IF r.response IS NOT NULL THEN RETURN r.response || jsonb_build_object('replayed',true); END IF;
  LOOP
    SELECT * INTO m FROM swarmpg.outbox_messages candidate
    WHERE candidate.org_id=p_org AND candidate.destination=p_destination
      AND ((candidate.state='pending' AND candidate.available_at <= clock_timestamp())
        OR (candidate.state='delivering' AND candidate.lease_until <= clock_timestamp()))
    ORDER BY candidate.available_at,candidate.created_at,candidate.id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      receipt := jsonb_build_object('message',NULL,'replayed',false);
      EXIT;
    END IF;
    -- Reclaim an expired final attempt as a durable failure, then continue to
    -- useful work. Such messages cannot remain stuck delivering indefinitely.
    IF m.attempts >= m.max_attempts THEN
      UPDATE swarmpg.outbox_messages SET state='failed',worker_id=NULL,lease_until=NULL,
        last_error='delivery attempts exhausted',revision=revision+1,updated_at=clock_timestamp()
      WHERE id=m.id RETURNING * INTO m;
      PERFORM swarmpg._outbox_event(m,'failed',NULL,jsonb_build_object('reason','delivery attempts exhausted'));
      CONTINUE;
    END IF;
    UPDATE swarmpg.outbox_messages SET state='delivering',worker_id=p_worker,fence=fence+1,
      lease_until=clock_timestamp()+p_lease_ms*interval '1 millisecond',attempts=attempts+1,
      total_attempts=total_attempts+1,revision=revision+1,updated_at=clock_timestamp()
    WHERE id=m.id RETURNING * INTO m;
    PERFORM swarmpg._outbox_event(m,'claimed',p_worker,jsonb_build_object('request_id',p_request));
    receipt := jsonb_build_object('message',swarmpg._outbox_json(m),'replayed',false);
    EXIT;
  END LOOP;
  UPDATE swarmpg.outbox_claim_requests SET response=receipt
  WHERE org_id=p_org AND worker_id=p_worker AND request_id=p_request;
  RETURN receipt;
END $$;

CREATE OR REPLACE FUNCTION swarmpg.outbox_get(p_org uuid,p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  SELECT * INTO m FROM swarmpg.outbox_messages WHERE org_id=p_org AND id=p_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'outbox message not found' USING ERRCODE = 'PT404'; END IF;
  RETURN swarmpg._outbox_json(m);
END $$;
CREATE OR REPLACE FUNCTION swarmpg.outbox_renew(p_org uuid,p_id uuid,p_worker uuid,p_fence bigint,p_lease_ms integer) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  PERFORM swarmpg._task_lease(p_lease_ms);
  SELECT * INTO m FROM swarmpg.outbox_messages WHERE org_id=p_org AND id=p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'outbox message not found' USING ERRCODE = 'PT404'; END IF;
  PERFORM swarmpg._outbox_require_claim(m,p_worker,p_fence);
  UPDATE swarmpg.outbox_messages SET lease_until=greatest(lease_until,clock_timestamp()+p_lease_ms*interval '1 millisecond'),
    revision=revision+1,updated_at=clock_timestamp() WHERE id=m.id RETURNING * INTO m;
  PERFORM swarmpg._outbox_event(m,'renewed',p_worker);
  RETURN swarmpg._outbox_json(m);
END $$;
CREATE OR REPLACE FUNCTION swarmpg.outbox_ack(p_org uuid,p_id uuid,p_worker uuid,p_fence bigint,p_response jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  IF p_response IS NULL OR octet_length(p_response::text)>16384 OR p_worker IS NULL OR p_fence IS NULL OR p_fence<1 THEN
    RAISE EXCEPTION 'worker, positive fence and JSON response of at most 16 KiB required' USING ERRCODE = 'PT400';
  END IF;
  SELECT * INTO m FROM swarmpg.outbox_messages WHERE org_id=p_org AND id=p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'outbox message not found' USING ERRCODE = 'PT404'; END IF;
  IF m.state='delivered' AND m.worker_id=p_worker AND m.fence=p_fence THEN
    IF m.delivery_response IS DISTINCT FROM p_response THEN
      RAISE EXCEPTION 'delivered message contains a different response' USING ERRCODE = 'PT409';
    END IF;
    RETURN jsonb_build_object('message',swarmpg._outbox_json(m),'replayed',true);
  END IF;
  PERFORM swarmpg._outbox_require_claim(m,p_worker,p_fence);
  UPDATE swarmpg.outbox_messages SET state='delivered',lease_until=NULL,delivery_response=p_response,
    delivered_at=clock_timestamp(),updated_at=clock_timestamp(),revision=revision+1 WHERE id=m.id RETURNING * INTO m;
  PERFORM swarmpg._outbox_event(m,'delivered',p_worker);
  RETURN jsonb_build_object('message',swarmpg._outbox_json(m),'replayed',false);
END $$;
CREATE OR REPLACE FUNCTION swarmpg.outbox_nack(p_org uuid,p_id uuid,p_worker uuid,p_fence bigint,
  p_error text,p_retry_ms integer,p_permanent boolean) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  IF p_error IS NULL OR length(p_error) NOT BETWEEN 1 AND 1000 OR p_retry_ms IS NULL
     OR p_retry_ms NOT BETWEEN 0 AND 3600000 OR p_permanent IS NULL THEN
    RAISE EXCEPTION 'error, retry_ms 0..3600000 and permanent are required' USING ERRCODE = 'PT400';
  END IF;
  SELECT * INTO m FROM swarmpg.outbox_messages WHERE org_id=p_org AND id=p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'outbox message not found' USING ERRCODE = 'PT404'; END IF;
  PERFORM swarmpg._outbox_require_claim(m,p_worker,p_fence);
  UPDATE swarmpg.outbox_messages SET state=CASE WHEN p_permanent OR attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
    worker_id=NULL,lease_until=NULL,last_error=p_error,available_at=clock_timestamp()+p_retry_ms*interval '1 millisecond',
    revision=revision+1,updated_at=clock_timestamp() WHERE id=m.id RETURNING * INTO m;
  PERFORM swarmpg._outbox_event(m,CASE WHEN m.state='failed' THEN 'failed' ELSE 'nacked' END,p_worker,
    jsonb_build_object('error',p_error,'retry_ms',p_retry_ms,'permanent',p_permanent));
  RETURN swarmpg._outbox_json(m);
END $$;
CREATE OR REPLACE FUNCTION swarmpg.outbox_retry(p_org uuid,p_id uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,swarmpg,pg_temp AS $$
DECLARE m swarmpg.outbox_messages%ROWTYPE;
BEGIN
  IF p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 1000 THEN
    RAISE EXCEPTION 'retry reason must be 1 to 1000 characters' USING ERRCODE = 'PT400';
  END IF;
  SELECT * INTO m FROM swarmpg.outbox_messages WHERE org_id=p_org AND id=p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'outbox message not found' USING ERRCODE = 'PT404'; END IF;
  IF m.state <> 'failed' THEN RAISE EXCEPTION 'only failed messages can be retried' USING ERRCODE = 'PT409'; END IF;
  UPDATE swarmpg.outbox_messages SET state='pending',worker_id=NULL,lease_until=NULL,attempts=0,fence=fence+1,
    available_at=clock_timestamp(),revision=revision+1,updated_at=clock_timestamp() WHERE id=m.id RETURNING * INTO m;
  PERFORM swarmpg._outbox_event(m,'retried',NULL,jsonb_build_object('reason',p_reason));
  RETURN swarmpg._outbox_json(m);
END $$;


-- Revoke default PUBLIC execution for every task function, including private
-- helpers and administrative recovery. Grant only the intended agent surface.
DO $$
DECLARE f regprocedure;
BEGIN
  FOR f IN SELECT p.oid::regprocedure FROM pg_proc p
           WHERE p.pronamespace = 'swarmpg'::regnamespace
             AND (p.proname LIKE 'task\_%' OR p.proname LIKE '\_task\_%'
               OR p.proname LIKE 'outbox\_%' OR p.proname LIKE '\_outbox\_%')
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, agent_runtime', f);
  END LOOP;
END $$;
GRANT EXECUTE ON FUNCTION swarmpg.task_enqueue(text,text,jsonb,uuid[],jsonb),
  swarmpg.task_get(uuid), swarmpg.task_claim(text,uuid,integer), swarmpg.task_renew(uuid,bigint,integer),
  swarmpg.task_release(uuid,bigint), swarmpg.task_complete(uuid,bigint,jsonb), swarmpg.task_cancel(uuid),
  swarmpg.task_history(uuid,integer), swarmpg.task_complete(uuid,bigint,jsonb,jsonb),
  swarmpg.task_outbox(uuid) TO agent_runtime;
COMMIT;
