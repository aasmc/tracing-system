--liquibase formatted sql

--changeset tc:001-01-kafka-connections-and-enrichment-sources
CREATE TABLE tc.kafka_connection (
    id                     uuid        PRIMARY KEY DEFAULT uuidv7(),
    name                   text        NOT NULL,
    bootstrap_servers      text        NOT NULL,
    security_protocol      text        NOT NULL DEFAULT 'SASL_SSL',
    sasl_mechanism         text        NULL,
    credentials_secret_ref text        NULL,    -- reference to a Vault / Kubernetes secret, never the secret itself
    truststore_secret_ref  text        NULL,
    client_properties      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT kafka_connection_name_uk     UNIQUE (name),
    CONSTRAINT kafka_connection_protocol_ck CHECK (security_protocol IN
        ('PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL')),
    CONSTRAINT kafka_connection_sasl_ck     CHECK (
        (security_protocol LIKE 'SASL%') = (sasl_mechanism IS NOT NULL)),
    CONSTRAINT kafka_connection_mech_ck     CHECK (sasl_mechanism IN
        ('PLAIN','SCRAM-SHA-256','SCRAM-SHA-512','OAUTHBEARER','GSSAPI')),
    CONSTRAINT kafka_connection_props_ck    CHECK (jsonb_typeof(client_properties) = 'object'),
    -- secrets belong in the secret store: they must not be smuggled in through client properties
    -- (removing the secret keys must leave the properties unchanged; the ?| operator would clash with JDBC's ?)
    CONSTRAINT kafka_connection_nosecret_ck CHECK (client_properties - ARRAY[
        'sasl.jaas.config','ssl.keystore.password','ssl.key.password',
        'ssl.truststore.password','ssl.keystore.key','ssl.keystore.certificate.chain'] = client_properties)
);

CREATE TABLE tc.enrichment_source (
    id               uuid        PRIMARY KEY DEFAULT uuidv7(),
    name             text        NOT NULL,
    connection_id    uuid        NOT NULL REFERENCES tc.kafka_connection (id),
    topic            text        NOT NULL,
    status           text        NOT NULL DEFAULT 'DRAFT',
    current_revision int         NULL,
    created_by       text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_source_name_uk   UNIQUE (name),
    -- the name is part of the consumer group id "tc-enricher.<name>"; its maximum length is configured
    CONSTRAINT enrichment_source_name_ck   CHECK (name ~ '^[a-z0-9][a-z0-9._-]{2,}$'),
    -- Kafka's own topic-name rule
    CONSTRAINT enrichment_source_topic_ck  CHECK (topic ~ '^[A-Za-z0-9._-]{1,249}$'),
    CONSTRAINT enrichment_source_status_ck CHECK (status IN ('DRAFT','ACTIVE','PAUSED','DELETED')),
    CONSTRAINT enrichment_source_rev_ck    CHECK (status IN ('DRAFT','DELETED')
                                                  OR current_revision IS NOT NULL)
);
CREATE INDEX enrichment_source_connection_idx ON tc.enrichment_source (connection_id);

CREATE TABLE tc.enrichment_source_revision (
    source_id  uuid        NOT NULL REFERENCES tc.enrichment_source (id),
    revision   int         NOT NULL,
    definition jsonb       NOT NULL,
    created_by text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_source_revision_pk PRIMARY KEY (source_id, revision),
    CONSTRAINT enrichment_source_revision_ck CHECK (revision > 0),
    CONSTRAINT enrichment_source_definition_ck CHECK (
        jsonb_typeof(definition) = 'object'
        AND definition -> 'traceContext' IS NOT NULL
        AND jsonb_typeof(definition -> 'fieldMappings') = 'array')
);
-- The current revision must be a revision of the same source. Deferred, so that a new revision and the
-- switch to it can be written in either order within one transaction.
ALTER TABLE tc.enrichment_source
    ADD CONSTRAINT enrichment_source_current_rev_fk
    FOREIGN KEY (id, current_revision)
    REFERENCES tc.enrichment_source_revision (source_id, revision)
    DEFERRABLE INITIALLY DEFERRED;
--rollback ALTER TABLE tc.enrichment_source DROP CONSTRAINT enrichment_source_current_rev_fk;
--rollback DROP TABLE tc.enrichment_source_revision;
--rollback DROP TABLE tc.enrichment_source;
--rollback DROP TABLE tc.kafka_connection;

--changeset tc:001-02-attribute-keys
CREATE TABLE tc.attribute_key (
    id         int         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key        text        COLLATE "C" NOT NULL,
    value_type text        NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT attribute_key_key_uk  UNIQUE (key),
    CONSTRAINT attribute_key_len_ck  CHECK (octet_length(key) >= 1),  -- maximum length is configured
    CONSTRAINT attribute_key_type_ck CHECK (value_type IN
        ('STRING','BOOLEAN','LONG','DOUBLE',
         'STRING_ARRAY','BOOLEAN_ARRAY','LONG_ARRAY','DOUBLE_ARRAY','MIXED'))
);

CREATE TABLE tc.indexed_attribute (
    key_id             int         PRIMARY KEY REFERENCES tc.attribute_key (id),
    origin_scope       text        NOT NULL DEFAULT 'ANY',
    numeric            boolean     NOT NULL DEFAULT false,
    status             text        NOT NULL DEFAULT 'BACKFILLING',
    indexed_since      timestamptz NOT NULL DEFAULT now(),
    backfill_cursor    timestamptz NULL,   -- last hour already backfilled; a new leader resumes from here
    retained_in_digest boolean     NOT NULL DEFAULT false,    -- the value is kept in trace_digest after full data is gone
    digest_transform   text        NOT NULL DEFAULT 'NONE',   -- applied to the value when it is kept in the digest
    created_by         text        NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT indexed_attribute_scope_ck     CHECK (origin_scope IN ('ANY','SPAN','RESOURCE','ENRICHMENT')),
    CONSTRAINT indexed_attribute_status_ck    CHECK (status IN ('BACKFILLING','ACTIVE','DISABLED')),
    CONSTRAINT indexed_attribute_transform_ck CHECK (digest_transform IN ('NONE','SHA256','MASK'))
);
--rollback DROP TABLE tc.indexed_attribute;
--rollback DROP TABLE tc.attribute_key;

--changeset tc:001-03-business-operation-definitions
CREATE TABLE tc.business_operation_definition (
    id           uuid     PRIMARY KEY DEFAULT uuidv7(),
    name         text     NOT NULL,
    description  text     NULL,
    merge_window interval NOT NULL DEFAULT '1 day',
    status       text     NOT NULL DEFAULT 'ACTIVE',
    -- how a finished operation's outcome is decided; every field is optional, e.g.
    -- {"outcomeKey":"business.outcome","successValues":["SUCCESS"],"failureValues":["FAILED"],
    --  "finalStepLabel":"token-exchange"}
    outcome_rule jsonb    NULL,
    CONSTRAINT business_operation_definition_name_uk   UNIQUE (name),
    CONSTRAINT business_operation_definition_window_ck CHECK (merge_window > interval '0'),  -- bounds are configured
    CONSTRAINT business_operation_definition_status_ck CHECK (status IN ('ACTIVE','DISABLED')),
    -- a missing rule or field makes its comparison NULL, which a CHECK accepts; a field of the wrong type fails it
    CONSTRAINT business_operation_definition_outcome_rule_ck CHECK (
        jsonb_typeof(outcome_rule) = 'object'
        AND jsonb_typeof(outcome_rule -> 'outcomeKey')     = 'string'
        AND jsonb_typeof(outcome_rule -> 'successValues')  = 'array'
        AND jsonb_typeof(outcome_rule -> 'failureValues')  = 'array'
        AND jsonb_typeof(outcome_rule -> 'finalStepLabel') = 'string')
);

CREATE TABLE tc.correlation_key (
    id                      uuid PRIMARY KEY DEFAULT uuidv7(),
    operation_definition_id uuid NOT NULL REFERENCES tc.business_operation_definition (id),
    key_id                  int  NOT NULL REFERENCES tc.attribute_key (id),
    role                    text NOT NULL,
    step_label              text NULL,
    CONSTRAINT correlation_key_role_ck    CHECK (role IN ('PRIMARY','ALIAS')),
    CONSTRAINT correlation_key_type_key_uk UNIQUE (operation_definition_id, key_id)
);
CREATE INDEX correlation_key_key_idx ON tc.correlation_key (key_id);
--rollback DROP TABLE tc.correlation_key;
--rollback DROP TABLE tc.business_operation_definition;

--changeset tc:001-04-retention-and-compaction-configuration
-- How long each data table keeps its rows. FULL tables hold the full trace data; DIGEST tables hold the compact
-- per-trace and per-operation rows that remain afterwards, and their retention is the total age (full + digest).
CREATE TABLE tc.retention_policy (
    table_name            text     PRIMARY KEY,
    tier                  text     NOT NULL,
    retention             interval NOT NULL,
    partition_granularity text     NOT NULL,
    precreate             interval NOT NULL DEFAULT '3 days',
    CONSTRAINT retention_policy_table_ck CHECK (table_name IN
        ('span','trace_attribute','span_link','trace_summary',
         'trace_enrichment','trace_correlation','business_operation',
         'trace_digest','business_operation_digest')),
    CONSTRAINT retention_policy_tier_ck  CHECK (tier IN ('FULL','DIGEST')),
    CONSTRAINT retention_policy_gran_ck  CHECK (partition_granularity IN ('HOUR','DAY','MONTH')),
    -- full data must outlive the day it is digested on
    CONSTRAINT retention_policy_ret_ck   CHECK (retention >= interval '2 days'),
    -- partitions must exist at least for the next hour; how far ahead beyond that is configured
    CONSTRAINT retention_policy_pre_ck   CHECK (precreate >= interval '1 hour')
);

-- Traces that are never digested, unless they belong to a business operation.
CREATE TABLE tc.compaction_exclusion (
    id                uuid        PRIMARY KEY DEFAULT uuidv7(),
    service_name      text        COLLATE "C" NULL,       -- root service; NULL matches every service
    root_name_pattern text        COLLATE "C" NOT NULL,   -- SQL LIKE pattern on the root span name
    description       text        NULL,
    created_by        text        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT compaction_exclusion_uk         UNIQUE NULLS NOT DISTINCT (service_name, root_name_pattern),
    CONSTRAINT compaction_exclusion_pattern_ck CHECK (octet_length(root_name_pattern) >= 1)
);

-- One row whose revision every configuration change increments, so services notice a change with one query.
CREATE TABLE tc.config_revision (
    id         smallint    PRIMARY KEY,
    revision   bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT config_revision_singleton_ck CHECK (id = 1)
);
--rollback DROP TABLE tc.config_revision;
--rollback DROP TABLE tc.compaction_exclusion;
--rollback DROP TABLE tc.retention_policy;

--changeset tc:001-05-services-span-names-resources-scopes
CREATE TABLE tc.service (
    id         int         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        COLLATE "C" NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT service_name_uk UNIQUE (name),
    CONSTRAINT service_name_ck CHECK (octet_length(name) >= 1)
);

CREATE TABLE tc.span_name (
    id         int  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_id int  NOT NULL REFERENCES tc.service (id),
    name       text COLLATE "C" NOT NULL,
    CONSTRAINT span_name_service_name_uk UNIQUE (service_id, name),
    CONSTRAINT span_name_len_ck CHECK (octet_length(name) >= 1)
);

-- id: xxHash64 of the canonical resource (schema URL and attributes sorted by key) as a signed 64-bit number
CREATE TABLE tc.resource (
    id         bigint      PRIMARY KEY,
    service_id int         NOT NULL REFERENCES tc.service (id),
    schema_url text        NULL,
    attributes jsonb       NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 90);   -- free space in each page lets last_seen updates stay HOT
CREATE INDEX resource_service_idx ON tc.resource (service_id);

-- id: xxHash64 of the canonical scope (name, version, schema URL, sorted attributes)
CREATE TABLE tc.instrumentation_scope (
    id         bigint PRIMARY KEY,
    name       text   NOT NULL,
    version    text   NULL,
    schema_url text   NULL,
    attributes jsonb  NULL
);
--rollback DROP TABLE tc.instrumentation_scope;
--rollback DROP TABLE tc.resource;
--rollback DROP TABLE tc.span_name;
--rollback DROP TABLE tc.service;

--changeset tc:001-06-correlation-value
-- The business operation each correlation value currently belongs to. Written by the correlation resolver only.
CREATE TABLE tc.correlation_value (
    operation_first_seen    timestamptz NOT NULL,   -- business_operation.first_seen: finds the operation's partition
    updated_at              timestamptz NOT NULL DEFAULT now(),
    operation_definition_id uuid        NOT NULL REFERENCES tc.business_operation_definition (id),
    operation_id            uuid        NOT NULL,   -- business_operation.id
    value                   text        COLLATE "C" NOT NULL,
    CONSTRAINT correlation_value_pk PRIMARY KEY (operation_definition_id, value)
) WITH (fillfactor = 85);
CREATE INDEX correlation_value_operation_idx ON tc.correlation_value (operation_id);
--rollback DROP TABLE tc.correlation_value;

--changeset tc:001-07-operational-state
-- How far the correlation resolver has read tc.trace_correlation (by id).
CREATE TABLE tc.correlation_resolver_state (
    id                smallint    PRIMARY KEY,
    last_processed_id bigint      NOT NULL DEFAULT 0,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- ShedLock's standard table. The lock provider uses the database clock, not the clocks of the pods.
CREATE TABLE tc.shedlock (
    name       varchar(64)  NOT NULL,
    lock_until timestamp    NOT NULL,
    locked_at  timestamp    NOT NULL,
    locked_by  varchar(255) NOT NULL,
    PRIMARY KEY (name)
);
-- Digesting progress of one trace day; a new leader resumes from last_chunk_end.
CREATE TABLE tc.compaction_state (
    trace_day           date        PRIMARY KEY,
    status              text        NOT NULL DEFAULT 'PENDING',
    last_chunk_end      timestamptz NULL,   -- trace_day + 1 day once every chunk is digested
    traces_digested     bigint      NOT NULL DEFAULT 0,
    traces_excluded     bigint      NOT NULL DEFAULT 0,
    operations_digested bigint      NOT NULL DEFAULT 0,
    started_at          timestamptz NULL,
    finished_at         timestamptz NULL,
    error               text        NULL,
    CONSTRAINT compaction_state_status_ck CHECK (status IN ('PENDING','RUNNING','DONE','FAILED')),
    CONSTRAINT compaction_state_done_ck   CHECK (status <> 'DONE' OR finished_at IS NOT NULL)
);
--rollback DROP TABLE tc.compaction_state;
--rollback DROP TABLE tc.shedlock;
--rollback DROP TABLE tc.correlation_resolver_state;

--changeset tc:001-20-try-float8 splitStatements:false
-- A text as float8, or NULL when it is not a number (an invalid value must not fail a whole backfill chunk).
CREATE FUNCTION tc.try_float8(p text) RETURNS float8
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    RETURN p::float8;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN NULL;
END $$;
--rollback DROP FUNCTION tc.try_float8(text);
