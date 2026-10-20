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
REVOKE ALL ON FUNCTION swarmpg.record_audit(uuid,uuid,text,int,int,bigint,bigint,double precision,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION swarmpg.record_audit(uuid,uuid,text,int,int,bigint,bigint,double precision,text,text) TO agent_runtime;

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
GRANT SELECT, INSERT, UPDATE, DELETE ON swarmpg.documents TO agent_runtime;
GRANT SELECT, INSERT ON swarmpg.events TO agent_runtime;
GRANT USAGE, SELECT ON SEQUENCE swarmpg.events_id_seq TO agent_runtime;
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
REVOKE EXECUTE ON FUNCTION pg_catalog.pg_stat_get_activity(integer) FROM PUBLIC;
DO $$ BEGIN
  EXECUTE 'REVOKE EXECUTE ON FUNCTION pg_catalog.pg_stat_get_backend_activity(integer) FROM PUBLIC';
EXCEPTION WHEN OTHERS THEN NULL; END $$;

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
