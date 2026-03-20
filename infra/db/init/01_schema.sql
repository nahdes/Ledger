-- =============================================================================
-- Apex Financial Services — Agentic Audit Ledger
-- Schema: core event store tables
-- Runs automatically on first container boot via docker-entrypoint-initdb.d
-- =============================================================================

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- text-search on event_type

-- =============================================================================
-- event_streams
-- One row per aggregate stream.  Carries the current version for OCC.
-- =============================================================================
CREATE TABLE event_streams (
    stream_id           TEXT        PRIMARY KEY,
    aggregate_type      TEXT        NOT NULL,
    current_version     INTEGER     NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at         TIMESTAMPTZ,                        -- NULL = active
    metadata            JSONB       NOT NULL DEFAULT '{}'
);

COMMENT ON TABLE  event_streams IS 'One row per aggregate stream. current_version is the OCC guard.';
COMMENT ON COLUMN event_streams.stream_id       IS 'Natural key: loan-APEX-0001, agent-credit_analysis-sess-cre-XXXX, etc.';
COMMENT ON COLUMN event_streams.aggregate_type  IS 'Derived from stream_id prefix: LoanApplication, AgentSession, …';
COMMENT ON COLUMN event_streams.current_version IS 'Incremented atomically on every append. Used for SELECT FOR UPDATE OCC.';
COMMENT ON COLUMN event_streams.archived_at     IS 'Set when stream reaches a terminal state. Prevents further appends.';

-- ── Index: list active streams by type ────────────────────────────────────────
CREATE INDEX idx_event_streams_type
    ON event_streams (aggregate_type)
    WHERE archived_at IS NULL;

-- =============================================================================
-- events
-- Immutable append-only event log.
-- =============================================================================
CREATE TABLE events (
    event_id            UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    stream_id           TEXT        NOT NULL REFERENCES event_streams (stream_id),
    stream_position     INTEGER     NOT NULL,   -- 1-based position within stream
    global_position     BIGINT      GENERATED ALWAYS AS IDENTITY,  -- global ordering
    event_type          TEXT        NOT NULL,
    event_version       INTEGER     NOT NULL DEFAULT 1,
    payload             JSONB       NOT NULL,
    metadata            JSONB       NOT NULL DEFAULT '{}',
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  events IS 'Immutable event log. Never UPDATE or DELETE.';
COMMENT ON COLUMN events.stream_position IS '1-based within stream. (stream_id, stream_position) is unique.';
COMMENT ON COLUMN events.global_position IS 'Monotonically increasing. Used by ProjectionDaemon to track replay position.';
COMMENT ON COLUMN events.event_version   IS 'Schema version. UpcasterRegistry translates old versions on read.';
COMMENT ON COLUMN events.payload         IS 'Domain event payload. Schema is governed by event_version.';
COMMENT ON COLUMN events.metadata        IS 'Cross-cutting: correlation_id, causation_id, actor, etc.';

-- ── Unique constraint: no duplicate positions within a stream ─────────────────
ALTER TABLE events
    ADD CONSTRAINT uq_events_stream_position
    UNIQUE (stream_id, stream_position);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX idx_events_global_position
    ON events (global_position);

CREATE INDEX idx_events_stream_recorded
    ON events (stream_id, recorded_at);

CREATE INDEX idx_events_type
    ON events (event_type);

-- Partial index: quickly find all agent session starts
CREATE INDEX idx_events_session_started
    ON events (stream_id, recorded_at)
    WHERE event_type = 'AgentSessionStarted';

-- GIN index on payload for ad-hoc JSONB queries (compliance audit, debugging)
CREATE INDEX idx_events_payload_gin
    ON events USING gin (payload);

-- =============================================================================
-- outbox
-- Transactional outbox pattern: written in the SAME transaction as events.
-- A relay process publishes rows to downstream consumers (Redis Streams, Kafka).
-- =============================================================================
CREATE TABLE outbox (
    outbox_id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id            UUID        NOT NULL REFERENCES events (event_id),
    destination         TEXT        NOT NULL,           -- e.g. 'redis-streams', 'kafka-audit'
    payload             JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMPTZ,                    -- NULL = pending relay
    failed_attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error          TEXT
);

COMMENT ON TABLE  outbox IS 'Transactional outbox. Relay publishes to message brokers after commit.';
COMMENT ON COLUMN outbox.published_at    IS 'Set by relay after successful delivery. NULL = undelivered.';
COMMENT ON COLUMN outbox.failed_attempts IS 'Incremented on relay failure. Circuit-breaker at 5 failures.';

CREATE INDEX idx_outbox_pending
    ON outbox (created_at)
    WHERE published_at IS NULL AND failed_attempts < 5;

-- =============================================================================
-- projection_checkpoints
-- Stores the last global_position each ProjectionDaemon successfully processed.
-- On restart the daemon resumes from checkpoint, not from global position 0.
-- =============================================================================
CREATE TABLE projection_checkpoints (
    projection_name     TEXT        PRIMARY KEY,
    last_global_position BIGINT     NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE projection_checkpoints IS 'Resume point for each async projection. Updated after every batch commit.';

-- Seed known projections at position 0
INSERT INTO projection_checkpoints (projection_name, last_global_position) VALUES
    ('ApplicationSummary',      0),
    ('AgentPerformanceLedger',  0),
    ('ComplianceAuditView',     0)
ON CONFLICT (projection_name) DO NOTHING;

-- =============================================================================
-- NOTIFY trigger
-- Sends a lightweight pg_notify on every event insert.
-- ProjectionDaemon LISTENs on 'new_event' to wake up immediately.
-- =============================================================================
CREATE OR REPLACE FUNCTION notify_new_event()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(
        'new_event',
        json_build_object(
            'stream_id',       NEW.stream_id,
            'event_type',      NEW.event_type,
            'global_position', NEW.global_position
        )::text
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_notify_new_event
    AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_new_event();

-- =============================================================================
-- archive_stream helper function
-- Marks a stream archived and refuses further appends at the DB level.
-- =============================================================================
CREATE OR REPLACE FUNCTION archive_stream(p_stream_id TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    UPDATE event_streams
    SET    archived_at = NOW()
    WHERE  stream_id   = p_stream_id
    AND    archived_at IS NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Stream % not found or already archived', p_stream_id;
    END IF;
END;
$$;
