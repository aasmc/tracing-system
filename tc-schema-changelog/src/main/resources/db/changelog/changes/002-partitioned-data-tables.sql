--liquibase formatted sql

-- Partitioned data tables. Partitions are created by tc-maintenance, never by Liquibase.
-- Columns are ordered by alignment (8-byte, uuid, 4-byte, 2-byte, variable length) so rows carry no padding.
-- Indexes are declared on the parent; PostgreSQL creates the matching index on every partition.
-- Storage parameters (fillfactor, autovacuum, toast_tuple_target) cannot be set on a parent; tc-maintenance sets
-- them on each partition it creates.
-- Data tables only ever grow (columns are added, never removed), so these changesets have no rollback.

--changeset tc:002-01-span
-- RANGE(start_time), one partition per hour. The primary key is the only index.
CREATE TABLE tc.span (
    start_time     timestamptz NOT NULL,   -- span start in microseconds (the wire value is cut from nanoseconds)
    span_id        bigint      NOT NULL,
    duration_ns    bigint      NOT NULL,   -- exact: end nanos - start nanos
    resource_id    bigint      NOT NULL,   -- resource.id
    scope_id       bigint      NOT NULL,   -- instrumentation_scope.id
    parent_span_id bigint      NULL,       -- NULL for a root span
    trace_id       uuid        NOT NULL,
    service_id     int         NOT NULL,   -- service.id
    name_id        int         NULL,       -- span_name.id; NULL when the service has too many names (name_overflow)
    kind           smallint    NOT NULL,   -- 0 UNSPECIFIED, 1 INTERNAL, 2 SERVER, 3 CLIENT, 4 PRODUCER, 5 CONSUMER
    status_code    smallint    NOT NULL,   -- 0 UNSET, 1 OK, 2 ERROR
    trace_flags    smallint    NOT NULL,   -- W3C trace flags as an unsigned byte
    flags          smallint    NOT NULL DEFAULT 0,   -- collector bit flags (e.g. attributes were truncated)
    -- STORAGE MAIN: compress large values inline before moving them out of the row, so reading a trace
    -- rarely needs a TOAST lookup per span
    name_overflow  text        STORAGE MAIN COMPRESSION lz4 NULL,
    status_message text        STORAGE MAIN COMPRESSION lz4 NULL,
    trace_state    text        STORAGE MAIN COMPRESSION lz4 NULL,
    dropped_counts jsonb       STORAGE MAIN COMPRESSION lz4 NULL,   -- only when the producer dropped something
    attributes     jsonb       STORAGE MAIN COMPRESSION lz4 NOT NULL DEFAULT '{}'::jsonb,
    events         jsonb       STORAGE MAIN COMPRESSION lz4 NULL,
    links          jsonb       STORAGE MAIN COMPRESSION lz4 NULL,
    CONSTRAINT span_pk             PRIMARY KEY (trace_id, span_id, start_time),
    CONSTRAINT span_span_id_ck     CHECK (span_id <> 0),
    CONSTRAINT span_duration_ck    CHECK (duration_ns >= 0),
    CONSTRAINT span_kind_ck        CHECK (kind BETWEEN 0 AND 5),
    CONSTRAINT span_status_ck      CHECK (status_code BETWEEN 0 AND 2),
    CONSTRAINT span_trace_flags_ck CHECK (trace_flags BETWEEN 0 AND 255),
    CONSTRAINT span_name_ck        CHECK ((name_id IS NULL) <> (name_overflow IS NULL))
) PARTITION BY RANGE (start_time);

--changeset tc:002-02-trace-summary
-- RANGE(trace_day), one partition per day. One row per trace and trace day.
CREATE TABLE tc.trace_summary (
    start_time        timestamptz NOT NULL,   -- earliest span start
    end_time          timestamptz NOT NULL,   -- latest span end
    duration_ns       bigint      NOT NULL,   -- end_time - start_time, microsecond resolution
    root_span_id      bigint      NULL,
    first_ingested_at timestamptz NOT NULL,
    last_ingested_at  timestamptz NOT NULL,
    trace_id          uuid        NOT NULL,
    trace_day         date        NOT NULL,   -- the UTC day the trace is filed under
    root_service_id   int         NULL,
    root_name_id      int         NULL,
    span_count        int         NOT NULL,
    error_count       int         NOT NULL DEFAULT 0,
    fragment_count    int         NOT NULL DEFAULT 1,
    service_ids       int[]       NOT NULL,   -- sorted, distinct
    CONSTRAINT trace_summary_pk        PRIMARY KEY (trace_id, trace_day),
    CONSTRAINT trace_summary_time_ck   CHECK (end_time >= start_time),
    CONSTRAINT trace_summary_counts_ck CHECK (span_count > 0 AND error_count BETWEEN 0 AND span_count)
) PARTITION BY RANGE (trace_day);
CREATE INDEX trace_summary_start_idx          ON tc.trace_summary (start_time DESC);
CREATE INDEX trace_summary_root_svc_start_idx ON tc.trace_summary (root_service_id, start_time DESC);
CREATE INDEX trace_summary_root_svc_dur_idx   ON tc.trace_summary (root_service_id, duration_ns);

--changeset tc:002-03-trace-attribute
-- RANGE(ts), one partition per hour. Search index of the indexed attribute keys only.
CREATE TABLE tc.trace_attribute (
    ts        timestamptz      NOT NULL,   -- span start, or the Kafka timestamp for an enrichment
    value_num double precision NULL,       -- set when the key is indexed as numeric and the value is a number
    trace_id  uuid             NOT NULL,
    key_id    int              NOT NULL,   -- attribute_key.id
    origin    smallint         NOT NULL,   -- 1 SPAN, 2 RESOURCE, 3 ENRICHMENT, 4 DERIVED
    value_str text             COLLATE "C" NOT NULL,   -- canonical text, cut to the configured index length
    CONSTRAINT trace_attribute_origin_ck CHECK (origin BETWEEN 1 AND 4)
) PARTITION BY RANGE (ts);
CREATE INDEX trace_attribute_kv_ts_idx
    ON tc.trace_attribute (key_id, value_str, ts DESC) INCLUDE (trace_id);
CREATE INDEX trace_attribute_num_ts_idx
    ON tc.trace_attribute (key_id, value_num, ts DESC) INCLUDE (trace_id)
    WHERE value_num IS NOT NULL;

--changeset tc:002-04-span-link
-- RANGE(start_time), one partition per day. OpenTelemetry span links: which traces point at which.
CREATE TABLE tc.span_link (
    start_time      timestamptz NOT NULL,   -- start of the linking span
    span_id         bigint      NOT NULL,
    linked_span_id  bigint      NOT NULL,
    trace_id        uuid        NOT NULL,
    linked_trace_id uuid        NOT NULL,
    attributes      jsonb       NULL
) PARTITION BY RANGE (start_time);
CREATE INDEX span_link_linked_trace_idx ON tc.span_link (linked_trace_id);
CREATE INDEX span_link_trace_idx        ON tc.span_link (trace_id);

--changeset tc:002-05-trace-enrichment
-- RANGE(observed_at), one partition per day. Attributes added to traces from enrichment sources; append-only.
CREATE TABLE tc.trace_enrichment (
    id              bigint      GENERATED ALWAYS AS IDENTITY,
    observed_at     timestamptz NOT NULL,   -- Kafka record timestamp: the same on every replay
    kafka_offset    bigint      NOT NULL,
    span_id         bigint      NULL,       -- set when the event names a span
    source_id       uuid        NOT NULL,   -- enrichment_source.id
    trace_id        uuid        NOT NULL,
    source_revision int         NOT NULL,
    key_id          int         NOT NULL,   -- attribute_key.id
    kafka_partition int         NOT NULL,
    value           jsonb       NOT NULL,   -- same typed encoding as span attributes
    -- a replayed Kafka record writes nothing twice
    CONSTRAINT trace_enrichment_record_uk
        UNIQUE (source_id, kafka_partition, kafka_offset, key_id, observed_at)
) PARTITION BY RANGE (observed_at);
CREATE INDEX trace_enrichment_trace_idx ON tc.trace_enrichment (trace_id);

--changeset tc:002-06-trace-correlation
-- RANGE(observed_at), one partition per day. Correlation values seen on traces; read by the correlation resolver.
CREATE TABLE tc.trace_correlation (
    id                      bigint      GENERATED ALWAYS AS IDENTITY,
    observed_at             timestamptz NOT NULL,   -- span start, or the Kafka timestamp for an enrichment
    trace_id                uuid        NOT NULL,
    operation_definition_id uuid        NOT NULL,   -- business_operation_definition.id
    correlation_key_id      uuid        NOT NULL,   -- correlation_key.id
    origin                  smallint    NOT NULL,   -- 1 SPAN, 2 RESOURCE, 3 ENRICHMENT, 4 DERIVED
    value                   text        COLLATE "C" NOT NULL,   -- maximum length is configured
    CONSTRAINT trace_correlation_origin_ck CHECK (origin BETWEEN 1 AND 4)
) PARTITION BY RANGE (observed_at);
CREATE INDEX trace_correlation_definition_value_idx
    ON tc.trace_correlation (operation_definition_id, value) INCLUDE (trace_id);
CREATE INDEX trace_correlation_trace_idx ON tc.trace_correlation (trace_id);
CREATE INDEX trace_correlation_id_idx    ON tc.trace_correlation (id);

--changeset tc:002-07-business-operation
-- RANGE(first_seen), one partition per month.
CREATE TABLE tc.business_operation (
    first_seen              timestamptz NOT NULL,   -- never changes: it is the partition key
    last_seen               timestamptz NOT NULL,
    id                      uuid        NOT NULL DEFAULT uuidv7(),
    merged_into             uuid        NULL,       -- the operation this one was merged into
    operation_definition_id uuid        NOT NULL,   -- business_operation_definition.id
    trace_count             int         NOT NULL DEFAULT 0,
    display_value           text        NOT NULL,
    CONSTRAINT business_operation_pk       PRIMARY KEY (id, first_seen),
    CONSTRAINT business_operation_time_ck  CHECK (last_seen >= first_seen),
    CONSTRAINT business_operation_merge_ck CHECK (merged_into IS DISTINCT FROM id),
    CONSTRAINT business_operation_count_ck CHECK (trace_count >= 0)
) PARTITION BY RANGE (first_seen);
CREATE INDEX business_operation_type_first_seen_idx
    ON tc.business_operation (operation_definition_id, first_seen DESC);

--changeset tc:002-08-trace-digest
-- RANGE(trace_day), one partition per day. One compact row per trace, written once when its full data is digested.
CREATE TABLE tc.trace_digest (
    start_time            timestamptz NOT NULL,   -- trace_summary.start_time
    duration_ns           bigint      NOT NULL,
    compacted_at          timestamptz NOT NULL DEFAULT now(),
    trace_id              uuid        NOT NULL,
    business_operation_id uuid        NULL,       -- the operation at the end of the merge chain
    trace_day             date        NOT NULL,   -- trace_summary.trace_day
    root_service_id       int         NULL,       -- service.id
    root_name_id          int         NULL,       -- span_name.id
    span_count            int         NOT NULL,
    error_count           int         NOT NULL,
    outcome               smallint    NOT NULL,   -- 1 OK (no error span), 2 ERROR
    service_ids           int[]       NOT NULL,
    -- retained keys -> sorted distinct values after the digest transform, e.g.
    -- {"user_id":["9f86d081…"],"client_id":["bookstore-web"]}
    attributes            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT trace_digest_pk         PRIMARY KEY (trace_id, trace_day),
    CONSTRAINT trace_digest_outcome_ck CHECK (outcome IN (1, 2)),
    CONSTRAINT trace_digest_counts_ck  CHECK (span_count > 0 AND error_count BETWEEN 0 AND span_count),
    CONSTRAINT trace_digest_attrs_ck   CHECK (jsonb_typeof(attributes) = 'object')
) PARTITION BY RANGE (trace_day);
CREATE INDEX trace_digest_start_idx          ON tc.trace_digest (start_time DESC);
CREATE INDEX trace_digest_root_svc_start_idx ON tc.trace_digest (root_service_id, start_time DESC);
CREATE INDEX trace_digest_operation_idx      ON tc.trace_digest (business_operation_id)
    WHERE business_operation_id IS NOT NULL;
CREATE INDEX trace_digest_attributes_gin     ON tc.trace_digest USING gin (attributes jsonb_path_ops);

--changeset tc:002-09-business-operation-digest
-- RANGE(op_day), one partition per day. One compact row per finished business operation that was not merged away.
CREATE TABLE tc.business_operation_digest (
    first_seen              timestamptz NOT NULL,
    last_seen               timestamptz NOT NULL,
    compacted_at            timestamptz NOT NULL DEFAULT now(),
    operation_id            uuid        NOT NULL,   -- business_operation.id
    operation_definition_id uuid        NOT NULL,   -- business_operation_definition.id
    op_day                  date        NOT NULL,   -- UTC day of first_seen
    trace_count             int         NOT NULL,
    outcome                 smallint    NOT NULL,   -- 0 UNKNOWN, 1 SUCCESS, 2 FAILED, 3 INCOMPLETE
    display_value           text        NOT NULL,
    outcome_detail          text        NULL,       -- the value that decided the outcome, cut to the configured length
    -- the member traces ordered by start, e.g.
    -- [{"stepLabel":"authentication","traceId":"4bf9…","startTime":"2026-11-21T14:03:12.123456Z",
    --   "durationMs":412,"outcome":"OK"}]
    steps                   jsonb       NOT NULL,
    attributes              jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- union of the members' retained attributes
    CONSTRAINT business_operation_digest_pk         PRIMARY KEY (operation_id, op_day),
    CONSTRAINT business_operation_digest_day_ck     CHECK (op_day = (first_seen AT TIME ZONE 'UTC')::date),
    CONSTRAINT business_operation_digest_time_ck    CHECK (last_seen >= first_seen),
    CONSTRAINT business_operation_digest_outcome_ck CHECK (outcome BETWEEN 0 AND 3),
    CONSTRAINT business_operation_digest_steps_ck   CHECK (jsonb_typeof(steps) = 'array'),
    CONSTRAINT business_operation_digest_attrs_ck   CHECK (jsonb_typeof(attributes) = 'object')
) PARTITION BY RANGE (op_day);
CREATE INDEX business_operation_digest_type_first_seen_idx
    ON tc.business_operation_digest (operation_definition_id, first_seen DESC);
CREATE INDEX business_operation_digest_type_value_idx
    ON tc.business_operation_digest (operation_definition_id, display_value);
CREATE INDEX business_operation_digest_type_outcome_idx
    ON tc.business_operation_digest (operation_definition_id, outcome, first_seen DESC);
CREATE INDEX business_operation_digest_attributes_gin
    ON tc.business_operation_digest USING gin (attributes jsonb_path_ops);

--changeset tc:002-20-operation-root splitStatements:false
-- The operation at the end of the merge chain that starts at p_id (p_id itself when it was never merged, or
-- when it is not stored). The resolver keeps chains at most one step long; the depth limit only stops a
-- corrupt cycle. first_seen is unknown here, so the primary key of every partition is probed.
CREATE FUNCTION tc.operation_root(p_id uuid) RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    WITH RECURSIVE chain(id, merged_into, depth) AS (
        SELECT bo.id, bo.merged_into, 1
        FROM tc.business_operation bo WHERE bo.id = p_id
        UNION ALL
        SELECT bo.id, bo.merged_into, c.depth + 1
        FROM chain c JOIN tc.business_operation bo ON bo.id = c.merged_into
        WHERE c.depth < 8
    )
    SELECT COALESCE((SELECT id FROM chain WHERE merged_into IS NULL ORDER BY depth LIMIT 1), p_id)
$$;
