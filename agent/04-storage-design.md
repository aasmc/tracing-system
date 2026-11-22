# 04 — Storage Design (PostgreSQL 18)

**Related documents**

- [01-technology-overview.md](01-technology-overview.md) covers Tempo, the OpenTelemetry Collector, Jaeger, ClickHouse-based backends and Promscale.
- [02-high-level-design.md](02-high-level-design.md) has the C4 context and container views, ADRs and the input contract.
- [03-low-level-design.md](03-low-level-design.md) covers services, packages, Kafka consumers and error handling.
- [04-storage-design.md](04-storage-design.md) is this document.
- [05-domain-model.md](05-domain-model.md) covers domain records, value objects and invariants.
- [06-use-cases.md](06-use-cases.md) has use cases UC-01 to UC-23.
- [07-sequence-diagrams.md](07-sequence-diagrams.md) has the sequence diagrams.
- [08-er-diagrams.md](08-er-diagrams.md) has the ER diagrams and the partitioning and retention summary.

This document specifies the PostgreSQL storage of the Trace Collector System (TCS). It covers:
- the complete DDL for schema `tc`;
- how data from the starter's `TraceMessage` (schemaVersion 1) is encoded;
- the SQL for the write and read paths;
- partition and vacuum management;
- two-tier retention (ADR-13): compaction of the FULL tier into the DIGEST tier (§6.8, §7.7–§7.9, §8.7);
- capacity planning for profiles S, M and L;
- server configuration, HA and backup, and security;
- the storage alternatives.

Table, column, service and use-case names are the canonical ones from the design brief. Any additions are listed in §15.

Numbers marked **ASSUMPTION** are model inputs. They must be validated by the benchmark plan in §11.

---

## 1. Why PostgreSQL 18

TCS has a hard constraint: PostgreSQL is the primary store, and other engines are alternatives only (§14). Within PostgreSQL we target **18.x, with 18.7 the current minor as of 2026-11-21**. It is supported until November 2030.

**PostgreSQL 19.0** was released on 2026-11-19, two days before this document. We will adopt 19 after its first minor releases, through `pg_upgrade`. PG 18 keeps planner statistics across `pg_upgrade`, which shortens the post-upgrade `ANALYZE` window on a multi-terabyte, roughly 1,000-partition database.

PG 18 and 17 features this design relies on:

| Feature | Version | Use in TCS |
|---|---|---|
| Asynchronous I/O subsystem (`io_method = worker \| io_uring \| sync`, `io_workers`) | 18 | Faster sequential scans, bitmap heap scans and vacuum. These matter for brute-force search (UC-13), backfill (UC-07) and vacuuming of hourly partitions. |
| `uuidv7()` | 18 | Time-ordered `business_operation.id`. B-tree inserts stay right-leaning, and the id leaks no information beyond the creation time. |
| B-tree skip scan | 18 | `trace_summary (root_service_id, duration_ns)` can serve a duration-only search with no service predicate. The planner skips across the few hundred distinct `root_service_id` values. |
| `RETURNING WITH (OLD AS o, NEW AS n)` | 18 | Optional. It lets the summary upsert report pre-update values for metrics. It is not required for correctness. |
| `transaction_timeout` | 17 | Caps how long an ingest or enrichment transaction can stay open. The correlation resolver's watermark logic (§6.7) depends on this cap. |
| Identity columns on partitioned tables | 17 | `trace_enrichment.id` and `trace_correlation.id`. |
| Data checksums enabled by `initdb` by default | 18 | Detects silent corruption on NVMe. Keep them on. |
| `DETACH PARTITION … CONCURRENTLY` | 14 | Retention drops without an ACCESS EXCLUSIVE lock on the parent (§8). |
| Insert-triggered autovacuum (`autovacuum_vacuum_insert_*`) | 13 | Sets visibility-map bits and freezes append-only partitions early (§8.5). |
| `default_toast_compression = lz4`, per-column `COMPRESSION` and `STORAGE` in `CREATE TABLE` | 14 / 16 | Inline lz4 compression of span attributes (§4.3, §9). |

---

## 2. Storage principles

1. **Append-only data, immutable rows.**
   - `span`, `trace_attribute`, `span_link`, `trace_enrichment` and `trace_correlation` are insert-only.
   - The only mutable data rows are `trace_summary`, which is upserted per fragment, plus `business_operation` and `correlation_value`, which the resolver maintains.
   - Enrichment never mutates spans (ADR-07).
2. **Retention means dropping partitions, never DELETE.** Every large table is RANGE-partitioned by time. Expired partitions are detached concurrently and dropped. Nothing is ever deleted row by row from a data table. The one exception is the unpartitioned `correlation_value` map, which is small.
3. **No DEFAULT partitions.**
   - A default partition would silently absorb rows with bad timestamps.
   - It would make every `ATTACH` scan it.
   - It makes `DETACH … CONCURRENTLY` impossible, because PostgreSQL forbids that operation when a default partition exists.
   - Rows outside the pre-created range are rejected by the application before insert: older than retention means dropped plus a metric; later than now + 1 day goes to the DLQ with reason `FUTURE_TIMESTAMP`.
4. **Dictionaries and deduplication.**
   - Low-cardinality strings are dictionary-encoded: `service`, `span_name`, `attribute_key`.
   - Resources and instrumentation scopes repeat in every span on the wire. They are stored once, keyed by xxHash64 of their canonical form (ADR-06).
   - A span carries `resource_id` and `scope_id`, 16 bytes in total, instead of roughly 700 bytes of repeated resource JSON.
5. **Binary identifiers.**
   - `trace_id` is stored as `uuid` (16 B fixed, compared with `memcmp`).
   - `span_id`, `parent_span_id` and `linked_span_id` are stored as `bigint` (8 B).
   - Hex text would take 33 and 17 bytes, compare more slowly, and inflate every index (ADR-10).
6. **Typed jsonb attributes.** Attribute maps are stored as jsonb in a type-preserving encoding (§5). Only *registered* keys are copied into the narrow inverted index table `trace_attribute` (ADR-05). There is no GIN index on `span.attributes`.
7. **Logical, not enforced, relationships between data tables.** Foreign keys are enforced only between the small, unpartitioned configuration and dictionary tables. Partitioned data tables reference each other logically (see [08-er-diagrams.md](08-er-diagrams.md)), because:
   - an enforced FK would add an index probe per inserted row;
   - it would also prevent independent per-table retention drops.


8. **Idempotent writes.** Kafka is the write-ahead log (ADR-02). Every insert is replay-safe:
   - `ON CONFLICT DO NOTHING` on the natural key;
   - derived rows are written only for rows that were actually inserted;
   - counters are incremented only from `RETURNING` sets.
9. **All timestamps in UTC.** The database runs with `timezone = 'UTC'`. Partition bounds, `trace_day` and partition names are all UTC.
10. **Two-tier retention (ADR-13).**
    - **FULL tier:** `span`, `trace_attribute`, `span_link`, `trace_summary`, `trace_enrichment`, `trace_correlation`, `correlation_value` and `business_operation` keep full detail for `fullDataDays` = n (default 7, minimum 2).
    - **DIGEST tier:** `trace_digest` (one row per trace) and `business_operation_digest` (one row per business operation) keep a compact digest for another `digestDays` = m (default 90). A digest partition is dropped when its day is older than n + m (default 97 days).
    - The tier of every table and its retention live in `retention_policy.tier` / `retention_policy.retention`.
    - `DigestCompactor` in tc-maintenance builds the digest from `trace_summary`, `trace_attribute` (retained keys only) and the correlation tables, **never from `span`** (§6.8).
    - **Never drop uncompacted data:** a FULL-tier partition is dropped only after every trace day it can contribute to is compacted (drop gate, §8.7).
    - The digest is write-once, so a GIN index (`jsonb_path_ops`) on the digest `attributes` is acceptable there. Principle 6 (no GIN on live spans, ADR-05) still holds for the FULL tier.
    - Only keys flagged `indexed_attribute.retain_in_digest` survive into the digest, optionally transformed (`digest_transform` SHA256 or MASK). Flagging a key later is not retroactive.
11. **Limits are configuration, not schema.** `CHECK` constraints enforce only rules that never change: non-empty values, enum values, time order, non-negative counters. Sizes, counts, lengths and time windows (name and key lengths, attribute value size, index value size, correlation value length, merge-window bounds) are configured limits enforced by the application (`IngestLimits`, `AttributeLimits`, `EnrichmentLimits`, `CorrelationLimits`), so an operator can raise them without a migration. The application validates configured limits at startup against physical ceilings, e.g. a B-tree index entry is at most about 2,700 bytes, so `maxIndexValueBytes` must stay below it.

---

## 3. Workload characterisation

| Dimension | Characteristic | Consequence |
|---|---|---|
| Write volume | Baseline M: 20,000 spans/s average, 60,000 peak. 1.728 B spans/day. About 4 fragments per trace (ASSUMPTION). | Batch writes per Kafka poll (COPY plus set-based SQL), never row-by-row JDBC. NVMe storage. WAL is a first-class sizing input. |
| Write pattern | Inserts with random `trace_id` keys. Time-correlated `start_time`. Small update churn on `trace_summary` (about 3 updates per trace). | Heap appends are cheap. **B-tree inserts on random uuid keys touch random leaf pages.** The leaf pages of the *currently written* index must stay in RAM, which is why the hot tables use hourly partitions. |
| Read volume | Low QPS: UI users and API clients, tens of requests per second at peak. Each request is latency-sensitive. | Queries go to a hot-standby replica (ADR-11). The primary is dedicated to ingest. |
| Read patterns | (a) point lookup by `trace_id` (UC-11); (b) time-bounded search over registered attributes (UC-12); (c) bounded brute-force scans (UC-13); (d) graph lookups by correlation value or link (UC-14, UC-20). | Every query carries a time bound that enables partition pruning. Indexes are designed for (a), (b) and (d). (c) is a bounded sequential scan. |
| Data life | Two tiers (ADR-13). FULL tier (spans, summaries, attributes, enrichments, correlations, operations): n = 7 days. DIGEST tier (`trace_digest`, `business_operation_digest`): until n + m = 97 days. | Partition drop per table, driven by `retention_policy` (`tier`). FULL-tier drops wait for compaction (drop gate, §8.7). |
| Consistency | At-least-once delivery from Kafka plus idempotent writes, which gives effectively-once storage. | Natural-key uniqueness wherever the source supplies a natural key. Duplicates are tolerated (and removed with `DISTINCT` at read time) only in `trace_attribute`. |

**Why hourly partitions for `span` and `trace_attribute`.**

A single-day `span` partition at baseline M would have 1.728 B primary-key entries. That is about 110 GB of B-tree at roughly 64 B per entry (§9.2). Random `trace_id` keys spread inserts uniformly over the whole index, so:
- with 32 GB `shared_buffers`, most inserts would need a random read of a leaf page first;
- at 60,000 spans/s that means tens of thousands of random reads per second, which is the classic "index outgrew RAM" ingest cliff.

With hourly partitions, the index currently being written covers 72 M spans on average and 216 M in a peak hour. That is about 4.6 GB on average and 14 GB at peak, and it stays resident.

The planner handles the resulting partition count well, provided every query prunes to a few partitions. Counts at the default retention (n = 7, m = 90):
- `span`: 168 hourly partitions, plus about 24 held by the compaction drop gate (§8.7), plus 72 pre-created;
- `trace_attribute`: the same again;
- `trace_digest` and `business_operation_digest`: up to 97 daily partitions each;
- about 800 partitions overall.

Daily tables receive roughly 20 times fewer rows (`trace_summary`) or have far smaller volumes, so daily granularity is enough for them.

---

## 4. Complete DDL (Liquibase, schema `tc`)

DDL is owned by `tc-schema-migrator`, which runs Liquibase 5.0.4 (the version managed by Spring Boot 4.1.3) through Spring Boot's Liquibase auto-configuration. The changelogs are **Liquibase formatted SQL**, so the DDL below is plain PostgreSQL SQL with Liquibase comment headers (§4.0).

**Role setup.**
- Roles are cluster-level objects. They are created by infrastructure-as-code (§13), not by Liquibase. The same infrastructure-as-code creates the empty schema `tc` (owner `tc_owner`), because Liquibase creates its tracking tables `databasechangelog` and `databasechangeloglock` in `tc` **before** it runs the first changeset.
- Liquibase connects as `tc_migrator`. The migrator's Hikari pool runs `SET ROLE tc_owner` on every new connection (`spring.datasource.hikari.connection-init-sql`), so every object, including the tracking tables, is owned by the `NOLOGIN` role `tc_owner`.

**Migration layout.**

Master changelog `db/changelog/db.changelog-master.yaml` includes, in this order:

| Changelog file (`db/changelog/changes/`) | Content |
|---|---|
| `001-control-plane-and-dictionaries.sql` | Schema, configuration tables, dictionaries, operational tables, `correlation_value` |
| `002-partitioned-data-tables.sql` | Partitioned parents and parent-level indexes |
| `003-seed.sql` | Seed rows: retention defaults, singleton rows, built-in indexed attributes |
| `004-grants.sql` | Privileges for the application roles (changeset `runOnChange:true`, so edited grants are re-applied) |
| `005-two-tier-retention.sql` | ADR-13: new columns on `indexed_attribute`, `business_operation_definition`, `retention_policy`; the DIGEST-tier tables `trace_digest`, `business_operation_digest`; `compaction_exclusion`, `compaction_state`; retention re-seed; grants (§4.7) |

**Partitions are not created by Liquibase.**
- `tc-maintenance` (PartitionManager) creates partitions on start and every 10 minutes (§8).
- `tc-ingester` reports *not ready* until partitions exist for the window [now − 1 h, now + 1 day].
- If an insert targets a missing partition, PostgreSQL raises SQLSTATE 23514, "no partition of relation … found for row". This is treated as a *transient* error with back-off, not as poison.

### 4.0 Changelog conventions

```yaml
# db/changelog/db.changelog-master.yaml
databaseChangeLog:
  - include: { file: changes/001-control-plane-and-dictionaries.sql, relativeToChangelogFile: true }
  - include: { file: changes/002-partitioned-data-tables.sql,       relativeToChangelogFile: true }
  - include: { file: changes/003-seed.sql,                          relativeToChangelogFile: true }
  - include: { file: changes/004-grants.sql,                        relativeToChangelogFile: true }
  - include: { file: changes/005-two-tier-retention.sql,            relativeToChangelogFile: true }
```

Every file starts with `--liquibase formatted sql`. Each logical unit is one changeset with author `tc` and an id prefixed by the file number:

```sql
--liquibase formatted sql

--changeset tc:001-01-config-tables
SET LOCAL lock_timeout = '5s';
CREATE TABLE tc.kafka_connection ( ... );
--rollback DROP TABLE tc.kafka_connection;

--changeset tc:001-20-try-float8 splitStatements:false
CREATE FUNCTION tc.try_float8(p text) RETURNS float8 ... $$ ... $$;
```

Rules:
- **Applied changesets are immutable.** Liquibase stores an MD5 checksum per changeset in `databasechangelog` and fails validation if an applied changeset changes. Fixes go into a new changeset. The only exception is `004-grants.sql`, marked `runOnChange:true`.
- **Functions and `DO` blocks** (`$$ … $$` bodies with inner semicolons) use `splitStatements:false`.
- **Non-transactional statements** (`CREATE INDEX CONCURRENTLY`, `DETACH PARTITION … CONCURRENTLY`) go into their own changeset with `runInTransaction:false` (§4.6).
- **Changelog parameters** `${fullDataDays}` and `${digestDays}` are substituted by Liquibase from `spring.liquibase.parameters.*` (§4.7). No other `${…}` sequences may appear in the SQL.
- **Rollback** comments are written for DDL changesets of configuration tables. Data tables are expand-only and are never rolled back by Liquibase.
- **Schema version check.** Each release declares the id of its last required changeset (for example `005-40-grants-digest`). The services' `SchemaVersionGuard` checks that this id is present in `tc.databasechangelog` (03 §7.2). No database tags are needed.

### 4.1 001 — configuration, dictionaries, operational tables

```sql
--liquibase formatted sql
-- 001-control-plane-and-dictionaries.sql (changeset headers omitted below for readability; see §4.0)
CREATE SCHEMA IF NOT EXISTS tc AUTHORIZATION tc_owner;

------------------------------------------------------------------------
-- Configuration (control plane). Small, unpartitioned, FKs ENFORCED.
------------------------------------------------------------------------
CREATE TABLE tc.kafka_connection (
    id                     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                   text        NOT NULL,
    bootstrap_servers      text        NOT NULL,
    security_protocol      text        NOT NULL DEFAULT 'SASL_SSL',
    sasl_mechanism         text        NULL,
    credentials_secret_ref text        NULL,    -- Vault / k8s secret reference, never the secret
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
    -- defence in depth: secrets must not be smuggled in via client properties
    CONSTRAINT kafka_connection_nosecret_ck CHECK (NOT (client_properties ?| ARRAY[
        'sasl.jaas.config','ssl.keystore.password','ssl.key.password',
        'ssl.truststore.password','ssl.keystore.key','ssl.keystore.certificate.chain']))
);

CREATE TABLE tc.enrichment_source (
    id               bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name             text        NOT NULL,
    connection_id    bigint      NOT NULL REFERENCES tc.kafka_connection (id),
    topic            text        NOT NULL,
    status           text        NOT NULL DEFAULT 'DRAFT',
    current_revision int         NULL,
    created_by       text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_source_name_uk   UNIQUE (name),
    -- name becomes part of the consumer group id "tc-enricher.<sourceName>"
    CONSTRAINT enrichment_source_name_ck   CHECK (name ~ '^[a-z0-9][a-z0-9._-]{2,}$'),  -- max length: configured (EnrichmentLimits)
    CONSTRAINT enrichment_source_topic_ck  CHECK (topic ~ '^[A-Za-z0-9._-]{1,249}$'),
    CONSTRAINT enrichment_source_status_ck CHECK (status IN ('DRAFT','ACTIVE','PAUSED','DELETED')),
    CONSTRAINT enrichment_source_rev_ck    CHECK (status IN ('DRAFT','DELETED')
                                                  OR current_revision IS NOT NULL)
);
CREATE INDEX enrichment_source_connection_idx ON tc.enrichment_source (connection_id);

CREATE TABLE tc.enrichment_source_revision (
    source_id  bigint      NOT NULL REFERENCES tc.enrichment_source (id),
    revision   int         NOT NULL,
    definition jsonb       NOT NULL,
    created_by text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_source_revision_pk PRIMARY KEY (source_id, revision),
    CONSTRAINT enrichment_source_revision_ck CHECK (revision > 0),
    CONSTRAINT enrichment_source_definition_ck CHECK (
        jsonb_typeof(definition) = 'object'
        AND definition ? 'traceContext'
        AND jsonb_typeof(definition -> 'fieldMappings') = 'array')
);
-- current_revision must point at an existing revision of the same source.
-- DEFERRABLE so that "insert revision N + bump current_revision" works in any order in one tx.
ALTER TABLE tc.enrichment_source
    ADD CONSTRAINT enrichment_source_current_rev_fk
    FOREIGN KEY (id, current_revision)
    REFERENCES tc.enrichment_source_revision (source_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

------------------------------------------------------------------------
-- Dictionaries
------------------------------------------------------------------------
CREATE TABLE tc.attribute_key (
    id         int         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key        text        COLLATE "C" NOT NULL,
    value_type text        NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT attribute_key_key_uk  UNIQUE (key),
    CONSTRAINT attribute_key_len_ck  CHECK (octet_length(key) >= 1),  -- max length: configured (AttributeLimits)
    CONSTRAINT attribute_key_type_ck CHECK (value_type IN
        ('STRING','BOOLEAN','LONG','DOUBLE',
         'STRING_ARRAY','BOOLEAN_ARRAY','LONG_ARRAY','DOUBLE_ARRAY','MIXED'))
);

CREATE TABLE tc.indexed_attribute (
    key_id        int         PRIMARY KEY REFERENCES tc.attribute_key (id),
    origin_scope  text        NOT NULL DEFAULT 'ANY',
    numeric       boolean     NOT NULL DEFAULT false,
    status        text        NOT NULL DEFAULT 'BACKFILLING',
    indexed_since timestamptz NOT NULL DEFAULT now(),
    backfill_cursor timestamptz NULL,      -- AttributeBackfill progress (hour already processed); resume after leader change
    created_by    text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT indexed_attribute_scope_ck  CHECK (origin_scope IN ('ANY','SPAN','RESOURCE','ENRICHMENT')),
    CONSTRAINT indexed_attribute_status_ck CHECK (status IN ('BACKFILLING','ACTIVE','DISABLED'))
);

CREATE TABLE tc.business_operation_definition (
    id           int      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text     NOT NULL,
    description  text     NULL,
    merge_window interval NOT NULL DEFAULT '1 day',
    status       text     NOT NULL DEFAULT 'ACTIVE',
    CONSTRAINT business_operation_definition_name_uk   UNIQUE (name),
    CONSTRAINT business_operation_definition_window_ck CHECK (merge_window > interval '0'),  -- bounds: configured (CorrelationLimits)
    CONSTRAINT business_operation_definition_status_ck CHECK (status IN ('ACTIVE','DISABLED'))
);

CREATE TABLE tc.correlation_key (
    id                  int  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_definition_id int  NOT NULL REFERENCES tc.business_operation_definition (id),
    key_id              int  NOT NULL REFERENCES tc.attribute_key (id),
    role                text NOT NULL,
    step_label          text NULL,
    CONSTRAINT correlation_key_role_ck    CHECK (role IN ('PRIMARY','ALIAS')),
    CONSTRAINT correlation_key_type_key_uk UNIQUE (operation_definition_id, key_id)
);
CREATE INDEX correlation_key_key_idx ON tc.correlation_key (key_id);

CREATE TABLE tc.retention_policy (
    table_name            text     PRIMARY KEY,
    retention             interval NOT NULL,
    partition_granularity text     NOT NULL,
    precreate             interval NOT NULL DEFAULT '3 days',
    CONSTRAINT retention_policy_table_ck CHECK (table_name IN
        ('span','trace_attribute','span_link','trace_summary',
         'trace_enrichment','trace_correlation','business_operation')),
    CONSTRAINT retention_policy_gran_ck  CHECK (partition_granularity IN ('HOUR','DAY','MONTH')),
    CONSTRAINT retention_policy_ret_ck   CHECK (retention >= interval '1 day'),
    CONSTRAINT retention_policy_pre_ck   CHECK (precreate BETWEEN interval '1 hour' AND interval '31 days')
);

CREATE TABLE tc.config_revision (
    id         smallint    PRIMARY KEY,
    revision   bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT config_revision_singleton_ck CHECK (id = 1)
);

CREATE TABLE tc.service (
    id         int         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        COLLATE "C" NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT service_name_uk UNIQUE (name),
    CONSTRAINT service_name_ck CHECK (octet_length(name) >= 1)  -- max length: configured attribute value size
);

CREATE TABLE tc.span_name (
    id         int  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_id int  NOT NULL REFERENCES tc.service (id),
    name       text COLLATE "C" NOT NULL,
    CONSTRAINT span_name_service_name_uk UNIQUE (service_id, name),
    CONSTRAINT span_name_len_ck CHECK (octet_length(name) >= 1)  -- dictionary threshold: configured
);

-- id = xxHash64 (seed 0) of canonical resource: schemaUrl + sorted (key,type,value) triples,
-- reinterpreted as signed 64-bit (Java long).
CREATE TABLE tc.resource (
    id         bigint      PRIMARY KEY,
    service_id int         NOT NULL REFERENCES tc.service (id),
    schema_url text        NULL,
    attributes jsonb       NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 90);              -- room for HOT updates of last_seen
CREATE INDEX resource_service_idx ON tc.resource (service_id);

CREATE TABLE tc.instrumentation_scope (
    id         bigint PRIMARY KEY,     -- xxHash64 of name|version|schemaUrl|sorted attributes
    name       text   NOT NULL,
    version    text   NULL,
    schema_url text   NULL,
    attributes jsonb  NULL
);

------------------------------------------------------------------------
-- Correlation map (unpartitioned; maintained by the resolver only)
------------------------------------------------------------------------
CREATE TABLE tc.correlation_value (
    operation_definition_id  int         NOT NULL REFERENCES tc.business_operation_definition (id),
    value                text        COLLATE "C" NOT NULL,
    operation_id         uuid        NOT NULL,          -- logical ref -> business_operation(id, first_seen)
    operation_first_seen timestamptz NOT NULL,          -- partition key of the referenced op (pruning)
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT correlation_value_pk PRIMARY KEY (operation_definition_id, value)
) WITH (fillfactor = 85);
CREATE INDEX correlation_value_operation_idx ON tc.correlation_value (operation_id);

------------------------------------------------------------------------
-- Operational
------------------------------------------------------------------------
CREATE TABLE tc.correlation_resolver_state (
    id                smallint    PRIMARY KEY,
    last_processed_id bigint      NOT NULL DEFAULT 0,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- Standard ShedLock DDL (JdbcTemplateLockProvider). Use .usingDbTime() so that
-- lock_until/locked_at are computed by the DB clock, not by pod clocks.
CREATE TABLE tc.shedlock (
    name       varchar(64)  NOT NULL,
    lock_until timestamp    NOT NULL,
    locked_at  timestamp    NOT NULL,
    locked_by  varchar(255) NOT NULL,
    PRIMARY KEY (name)
);

------------------------------------------------------------------------
-- Helper functions
------------------------------------------------------------------------
-- Used by AttributeBackfill (03 §6) to fill trace_attribute.value_num for numeric keys:
-- returns NULL instead of raising when the text is not a valid float8.
CREATE FUNCTION tc.try_float8(p text) RETURNS float8
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    RETURN p::float8;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN NULL;
END $$;
```

### 4.2 002 — partitioned data tables

Physical column order is chosen for alignment ("column tetris"). The order is:
1. 8-byte-aligned columns;
2. `uuid` (char alignment, 16 B);
3. 4-byte columns, then 2-byte columns;
4. variable-length columns last.

This removes padding. The logical model in [05-domain-model.md](05-domain-model.md) is unaffected.

Indexes are declared on the **partitioned parent**. PostgreSQL creates or attaches the matching index on every partition.

Storage parameters such as `fillfactor`, autovacuum settings and `toast_tuple_target` **cannot be set on a partitioned parent**, which has no storage. `UNLOGGED` is not supported for partitioned tables either. PartitionManager therefore sets these parameters on every partition when it creates it (§8.2).

```sql
--liquibase formatted sql
-- 002-partitioned-data-tables.sql (changeset headers omitted below for readability; see §4.0)

------------------------------------------------------------------------
-- span: RANGE(start_time), HOURLY. Only index = PK.
------------------------------------------------------------------------
CREATE TABLE tc.span (
    start_time     timestamptz NOT NULL,   -- span start, µs (nanos truncated)
    span_id        bigint      NOT NULL,
    duration_ns    bigint      NOT NULL,   -- exact: endEpochNanos - startEpochNanos
    resource_id    bigint      NOT NULL,   -- logical -> resource.id
    scope_id       bigint      NOT NULL,   -- logical -> instrumentation_scope.id
    parent_span_id bigint      NULL,       -- NULL for root ("0000000000000000" on the wire)
    trace_id       uuid        NOT NULL,
    service_id     int         NOT NULL,   -- logical -> service.id
    name_id        int         NULL,       -- logical -> span_name.id; NULL on overflow
    kind           smallint    NOT NULL,   -- 0 UNSPECIFIED,1 INTERNAL,2 SERVER,3 CLIENT,4 PRODUCER,5 CONSUMER
    status_code    smallint    NOT NULL,   -- 0 UNSET, 1 OK, 2 ERROR
    trace_flags    smallint    NOT NULL,   -- W3C flags as unsigned byte
    flags          smallint    NOT NULL DEFAULT 0,  -- TCS bit flags, see §5.5
    name_overflow  text        STORAGE MAIN COMPRESSION lz4 NULL,
    status_message text        STORAGE MAIN COMPRESSION lz4 NULL,
    trace_state    text        STORAGE MAIN COMPRESSION lz4 NULL,
    dropped_counts jsonb       STORAGE MAIN COMPRESSION lz4 NULL,  -- only if any count > 0
    attributes     jsonb       STORAGE MAIN COMPRESSION lz4 NOT NULL DEFAULT '{}'::jsonb,
    events         jsonb       STORAGE MAIN COMPRESSION lz4 NULL,
    links          jsonb       STORAGE MAIN COMPRESSION lz4 NULL,
    CONSTRAINT span_pk            PRIMARY KEY (trace_id, span_id, start_time),
    CONSTRAINT span_span_id_ck    CHECK (span_id <> 0),
    CONSTRAINT span_duration_ck   CHECK (duration_ns >= 0),
    CONSTRAINT span_kind_ck       CHECK (kind BETWEEN 0 AND 5),
    CONSTRAINT span_status_ck     CHECK (status_code BETWEEN 0 AND 2),
    CONSTRAINT span_trace_flags_ck CHECK (trace_flags BETWEEN 0 AND 255),
    CONSTRAINT span_name_ck       CHECK ((name_id IS NULL) <> (name_overflow IS NULL))
) PARTITION BY RANGE (start_time);

------------------------------------------------------------------------
-- trace_summary: RANGE(trace_day), DAILY. One row per trace per trace_day.
------------------------------------------------------------------------
CREATE TABLE tc.trace_summary (
    start_time        timestamptz NOT NULL,   -- min(span.start_time)
    end_time          timestamptz NOT NULL,   -- max(span.start_time + duration)
    duration_ns       bigint      NOT NULL,   -- (end_time - start_time), µs resolution
    root_span_id      bigint      NULL,
    first_ingested_at timestamptz NOT NULL,
    last_ingested_at  timestamptz NOT NULL,
    trace_id          uuid        NOT NULL,
    trace_day         date        NOT NULL,   -- UTC day chosen by TraceDayResolver
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
CREATE INDEX trace_summary_start_idx            ON tc.trace_summary (start_time DESC);
CREATE INDEX trace_summary_root_svc_start_idx   ON tc.trace_summary (root_service_id, start_time DESC);
CREATE INDEX trace_summary_root_svc_dur_idx     ON tc.trace_summary (root_service_id, duration_ns);

------------------------------------------------------------------------
-- trace_attribute: RANGE(ts), HOURLY. Inverted index of REGISTERED keys only.
------------------------------------------------------------------------
CREATE TABLE tc.trace_attribute (
    ts        timestamptz      NOT NULL,     -- span start (SPAN/RESOURCE/DERIVED) or observed_at (ENRICHMENT)
    value_num double precision NULL,         -- set when indexed_attribute.numeric and value is numeric
    trace_id  uuid             NOT NULL,
    key_id    int              NOT NULL,     -- logical -> attribute_key.id
    origin    smallint         NOT NULL,     -- 1 SPAN, 2 RESOURCE, 3 ENRICHMENT, 4 DERIVED
    value_str text             COLLATE "C" NOT NULL,   -- canonical text, cut to IngestLimits.maxIndexValueBytes
    CONSTRAINT trace_attribute_origin_ck CHECK (origin BETWEEN 1 AND 4)
) PARTITION BY RANGE (ts);
CREATE INDEX trace_attribute_kv_ts_idx
    ON tc.trace_attribute (key_id, value_str, ts DESC) INCLUDE (trace_id);
CREATE INDEX trace_attribute_num_ts_idx
    ON tc.trace_attribute (key_id, value_num, ts DESC) INCLUDE (trace_id)
    WHERE value_num IS NOT NULL;

------------------------------------------------------------------------
-- span_link: RANGE(start_time), DAILY. Native OTel links, trace-to-trace graph.
------------------------------------------------------------------------
CREATE TABLE tc.span_link (
    start_time      timestamptz NOT NULL,     -- start of the LINKING span
    span_id         bigint      NOT NULL,
    linked_span_id  bigint      NOT NULL,
    trace_id        uuid        NOT NULL,
    linked_trace_id uuid        NOT NULL,
    attributes      jsonb       NULL
) PARTITION BY RANGE (start_time);
CREATE INDEX span_link_linked_trace_idx ON tc.span_link (linked_trace_id);
CREATE INDEX span_link_trace_idx        ON tc.span_link (trace_id);

------------------------------------------------------------------------
-- trace_enrichment: RANGE(observed_at), DAILY. Append-only additions (ADR-07).
------------------------------------------------------------------------
CREATE TABLE tc.trace_enrichment (
    id              bigint      GENERATED ALWAYS AS IDENTITY,
    observed_at     timestamptz NOT NULL,   -- Kafka record timestamp (deterministic on replay)
    source_id       bigint      NOT NULL,   -- logical -> enrichment_source.id
    kafka_offset    bigint      NOT NULL,
    span_id         bigint      NULL,       -- set when source scope = SPAN
    trace_id        uuid        NOT NULL,
    source_revision int         NOT NULL,
    key_id          int         NOT NULL,   -- logical -> attribute_key.id
    kafka_partition int         NOT NULL,
    value           jsonb       NOT NULL,   -- same typed encoding as span attributes (§5)
    CONSTRAINT trace_enrichment_record_uk
        UNIQUE (source_id, kafka_partition, kafka_offset, key_id, observed_at)
) PARTITION BY RANGE (observed_at);
CREATE INDEX trace_enrichment_trace_idx ON tc.trace_enrichment (trace_id);

------------------------------------------------------------------------
-- trace_correlation: RANGE(observed_at), DAILY. Input of the resolver.
------------------------------------------------------------------------
CREATE TABLE tc.trace_correlation (
    id                  bigint      GENERATED ALWAYS AS IDENTITY,
    observed_at         timestamptz NOT NULL,  -- span start (origin 1/2) or Kafka ts (origin 3)
    trace_id            uuid        NOT NULL,
    operation_definition_id int         NOT NULL,  -- logical -> business_operation_definition.id
    correlation_key_id  int         NOT NULL,  -- logical -> correlation_key.id
    origin              smallint    NOT NULL,
    value               text        COLLATE "C" NOT NULL,
    CONSTRAINT trace_correlation_origin_ck CHECK (origin BETWEEN 1 AND 4)  -- value length: configured (CorrelationLimits)
) PARTITION BY RANGE (observed_at);
CREATE INDEX trace_correlation_definition_value_idx
    ON tc.trace_correlation (operation_definition_id, value) INCLUDE (trace_id);
CREATE INDEX trace_correlation_trace_idx ON tc.trace_correlation (trace_id);
CREATE INDEX trace_correlation_id_idx    ON tc.trace_correlation (id);

------------------------------------------------------------------------
-- business_operation: RANGE(first_seen), MONTHLY.
------------------------------------------------------------------------
CREATE TABLE tc.business_operation (
    id                  uuid        NOT NULL DEFAULT uuidv7(),
    first_seen          timestamptz NOT NULL,   -- immutable (partition key)
    last_seen           timestamptz NOT NULL,
    merged_into         uuid        NULL,       -- survivor id after a merge (UC-10)
    operation_definition_id int         NOT NULL,   -- logical -> business_operation_definition.id
    trace_count         int         NOT NULL DEFAULT 0,
    display_value       text        NOT NULL,
    CONSTRAINT business_operation_pk       PRIMARY KEY (id, first_seen),
    CONSTRAINT business_operation_time_ck  CHECK (last_seen >= first_seen),
    CONSTRAINT business_operation_merge_ck CHECK (merged_into IS DISTINCT FROM id),
    CONSTRAINT business_operation_count_ck CHECK (trace_count >= 0)
) PARTITION BY RANGE (first_seen);
CREATE INDEX business_operation_type_first_seen_idx
    ON tc.business_operation (operation_definition_id, first_seen DESC);
```

Notes on the DDL:

- **PK and UNIQUE on partitioned tables must include the partition key.** That is why the span PK is `(trace_id, span_id, start_time)` and the enrichment idempotency key includes `observed_at`.
  - As a consequence, two copies of a span with the same id but *different* start times are not detected as duplicates.
  - Starter replays always carry identical timestamps, so this is acceptable.
- **`trace_enrichment` and `trace_correlation` have no primary key.** Their `id` exists for ordering and watermarks. A PK on `(id, observed_at)` would only add another index.
- **`COLLATE "C"`** is used on `trace_attribute.value_str`, `trace_correlation.value`, `correlation_value.value` and the dictionary names. It has three effects:
  - B-tree comparisons become byte-wise, which is faster than ICU or libc collation;
  - `LIKE 'abc%'` and range predicates can use the plain B-tree, which autocomplete (UC-15) and the `PREFIX` operator need;
  - ordering is deterministic and independent of the OS locale.
- **`STORAGE MAIN COMPRESSION lz4` on every variable-length column of `span`.**
  - With the default `toast_tuple_target` of about 2 KB, typical span rows (about 480 B) are stored uncompressed exactly as before.
  - Rows between 2 KB and about 8 KB are compressed *inline* rather than moved out of line. That avoids a TOAST index lookup per span on trace reads.
  - The setting also enables the optional inline-compression mode of §4.3.
- The B-tree tuple size limit is roughly one third of a page, about 2.7 KB. Index keys must therefore be bounded:
  - `value_str` and correlation `value` are capped at 512 bytes;
  - longer values are truncated by the ingester on a UTF-8 boundary;
  - the matching rule is described in §7.2.

### 4.3 Note: the brief's `toast_tuple_target = 128` must be combined with `STORAGE MAIN`

The brief suggests `toast_tuple_target = 128` on `span` to force lz4 compression of `attributes`. Taken alone this is harmful.

The TOAST algorithm works in passes. While the tuple exceeds the target, it:
1. compresses `EXTENDED` columns;
2. then **moves `EXTENDED` columns out of line** while the tuple is still above the target.

With a 128-byte target, almost every span (compressed size about 200 to 350 B) would have its `attributes`, and even short `status_message` values, pushed into the TOAST relation. Every trace read would then need one TOAST index lookup per span.

With **`STORAGE MAIN`** the algorithm behaves differently:
- it first compresses `MAIN` columns against the 128-byte target;
- it moves them out of line only if the row still exceeds the much larger main-tuple target of about 8 KB.

So the combination `STORAGE MAIN` (set in the DDL above) plus `toast_tuple_target = 128` (set per partition by PartitionManager, behind a feature flag) gives *always-compress-inline*. The capacity impact (about −26% on `span`) is quantified in §9.3. It stays an ASSUMPTION until the benchmark confirms it.

When creating partitions with `CREATE TABLE … (LIKE tc.span …)`, use `INCLUDING STORAGE INCLUDING COMPRESSION` so that the column settings are copied. Verify them with `\d+` on a fresh partition in the migration integration test.

### 4.4 003 — seed data

```sql
--liquibase formatted sql
-- 003-seed.sql (changeset headers omitted below for readability; see §4.0)
INSERT INTO tc.config_revision (id, revision) VALUES (1, 0);
INSERT INTO tc.correlation_resolver_state (id, last_processed_id) VALUES (1, 0);

INSERT INTO tc.retention_policy (table_name, retention, partition_granularity, precreate) VALUES
    ('span',               interval '14 days', 'HOUR',  interval '3 days'),
    ('trace_attribute',    interval '14 days', 'HOUR',  interval '3 days'),
    ('span_link',          interval '14 days', 'DAY',   interval '3 days'),
    ('trace_summary',      interval '30 days', 'DAY',   interval '3 days'),
    ('trace_enrichment',   interval '30 days', 'DAY',   interval '3 days'),
    ('trace_correlation',  interval '90 days', 'DAY',   interval '3 days'),
    ('business_operation', interval '90 days', 'MONTH', interval '3 days');

-- Built-in indexed keys (ADR-05). They are written with origin 2 (service.name, from the
-- resource) or 4 (DERIVED: root span name, trace error status).
INSERT INTO tc.attribute_key (key, value_type) VALUES
    ('service.name', 'STRING'), ('span.name', 'STRING'), ('status', 'STRING');
INSERT INTO tc.indexed_attribute (key_id, origin_scope, numeric, status, created_by)
SELECT id, CASE key WHEN 'service.name' THEN 'RESOURCE' ELSE 'SPAN' END, false, 'ACTIVE', 'system'
FROM tc.attribute_key WHERE key IN ('service.name', 'span.name', 'status');
```

For `business_operation`, the MONTH granularity combined with a 90-day retention means a monthly partition is dropped when its *upper* bound is older than 90 days. Operations therefore actually live 90 to about 121 days. `correlation_value` rows are deleted by maintenance once their `operation_first_seen` is older than the `business_operation` retention (§8.4).

> **Superseded by changelog 005 (ADR-13).** Changelog 005 (§4.7) re-seeds every row above to the FULL tier with `retention = 7 days` and adds the two DIGEST-tier rows (97 days). A `business_operation` month partition is then dropped once the whole month is older than n days **and** every operation in it is compacted, so operations stay in the FULL tier for at most about 38–40 days. `correlation_value` rows are deleted once their operation is compacted (§8.4).

### 4.5 004 — grants

Changelog 004 holds the per-database privileges shown in §13.2. Role creation itself (cluster level) lives in infrastructure-as-code.

### 4.6 Adding indexes later (non-transactional migrations)

`CREATE INDEX` on a partitioned parent cannot use `CONCURRENTLY`. Later index additions on large data tables therefore follow the standard pattern. Each such change is a new changelog file (for example `006-trace-summary-new-index.sql`) whose changesets use `runInTransaction:false`; the per-partition statements are one changeset each, so a failure resumes at the failed partition.

```sql
CREATE INDEX trace_summary_new_idx ON ONLY tc.trace_summary (/* cols */);      -- invalid, parent only
-- for each partition (generated by a script, one statement per partition):
CREATE INDEX CONCURRENTLY trace_summary_p20261121_new_idx ON tc.trace_summary_p20261121 (/* cols */);
ALTER INDEX tc.trace_summary_new_idx ATTACH PARTITION tc.trace_summary_p20261121_new_idx;
-- the parent index becomes valid automatically once every partition has an attached index
```

Partitions created after the parent index exists receive the index automatically.

### 4.7 005 — two-tier retention (ADR-13)

Changelog 005 consists of regular transactional changesets. Every `ALTER TABLE … ADD COLUMN` below has a constant default, so it is a catalogue-only change, and the altered tables are tiny configuration tables. The new partitioned parents are empty, so their parent-level indexes are created directly (no §4.6 procedure is needed).

`${fullDataDays}` and `${digestDays}` are Liquibase changelog parameters (`spring.liquibase.parameters.fullDataDays` / `digestDays`). tc-schema-migrator fills them from `tc.retention.full-days` (default 7) and `tc.retention.digest-days` (default 90). They are used **only by this seed**. Afterwards the values are owned by `retention_policy` and changed through `PUT /api/v1/admin/retention-policies/tiers`.

```sql
--liquibase formatted sql
-- 005-two-tier-retention.sql (changeset headers omitted below for readability; see §4.0)

------------------------------------------------------------------------
-- New configuration columns
------------------------------------------------------------------------
ALTER TABLE tc.indexed_attribute
    ADD COLUMN retain_in_digest boolean NOT NULL DEFAULT false,   -- copy this key into the DIGEST tier
    ADD COLUMN digest_transform text    NOT NULL DEFAULT 'NONE',  -- applied when the value is digested
    ADD CONSTRAINT indexed_attribute_transform_ck CHECK (digest_transform IN ('NONE','SHA256','MASK'));

ALTER TABLE tc.business_operation_definition
    ADD COLUMN outcome_rule jsonb NULL,
    -- {"outcomeKey":"business.outcome","successValues":["SUCCESS"],
    --  "failureValues":["FAILED","DENIED"],"finalStepLabel":"token-exchange"}   (all fields optional)
    ADD CONSTRAINT business_operation_definition_outcome_rule_ck CHECK (
        outcome_rule IS NULL OR (
            jsonb_typeof(outcome_rule) = 'object'
            AND (NOT outcome_rule ? 'outcomeKey'     OR jsonb_typeof(outcome_rule -> 'outcomeKey')     = 'string')
            AND (NOT outcome_rule ? 'successValues'  OR jsonb_typeof(outcome_rule -> 'successValues')  = 'array')
            AND (NOT outcome_rule ? 'failureValues'  OR jsonb_typeof(outcome_rule -> 'failureValues')  = 'array')
            AND (NOT outcome_rule ? 'finalStepLabel' OR jsonb_typeof(outcome_rule -> 'finalStepLabel') = 'string')));

ALTER TABLE tc.retention_policy
    ADD COLUMN tier text NOT NULL DEFAULT 'FULL',
    ADD CONSTRAINT retention_policy_tier_ck CHECK (tier IN ('FULL','DIGEST')),
    ADD CONSTRAINT retention_policy_full_min_ck CHECK (tier <> 'FULL' OR retention >= interval '2 days'),
    DROP CONSTRAINT retention_policy_table_ck,
    ADD CONSTRAINT retention_policy_table_ck CHECK (table_name IN
        ('span','trace_attribute','span_link','trace_summary',
         'trace_enrichment','trace_correlation','business_operation',
         'trace_digest','business_operation_digest'));

------------------------------------------------------------------------
-- Configuration: traces that are never digested (unless they belong to a business operation)
------------------------------------------------------------------------
CREATE TABLE tc.compaction_exclusion (
    id                int         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_name      text        COLLATE "C" NULL,       -- root service; NULL = any service
    root_name_pattern text        COLLATE "C" NOT NULL,   -- SQL LIKE on the root span name
    description       text        NULL,
    created_by        text        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT compaction_exclusion_uk         UNIQUE NULLS NOT DISTINCT (service_name, root_name_pattern),
    CONSTRAINT compaction_exclusion_pattern_ck CHECK (octet_length(root_name_pattern) >= 1)  -- max length: configured
);

------------------------------------------------------------------------
-- Operational: one row per trace_day, progress of DigestCompactor (§6.8)
------------------------------------------------------------------------
CREATE TABLE tc.compaction_state (
    trace_day           date        PRIMARY KEY,
    status              text        NOT NULL DEFAULT 'PENDING',
    last_chunk_end      timestamptz NULL,     -- resume point; = trace_day + 1 day when every chunk is digested
    traces_digested     bigint      NOT NULL DEFAULT 0,
    traces_excluded     bigint      NOT NULL DEFAULT 0,
    operations_digested bigint      NOT NULL DEFAULT 0,
    started_at          timestamptz NULL,
    finished_at         timestamptz NULL,
    error               text        NULL,
    CONSTRAINT compaction_state_status_ck CHECK (status IN ('PENDING','RUNNING','DONE','FAILED')),
    CONSTRAINT compaction_state_done_ck   CHECK (status <> 'DONE' OR finished_at IS NOT NULL)
);

------------------------------------------------------------------------
-- trace_digest: RANGE(trace_day), DAILY. One row per trace. Write-once (DIGEST tier).
------------------------------------------------------------------------
CREATE TABLE tc.trace_digest (
    start_time            timestamptz NOT NULL,   -- = trace_summary.start_time
    duration_ns           bigint      NOT NULL,
    compacted_at          timestamptz NOT NULL DEFAULT now(),
    trace_id              uuid        NOT NULL,
    business_operation_id uuid        NULL,       -- logical -> business_operation(_digest), root survivor
    trace_day             date        NOT NULL,   -- = trace_summary.trace_day (partition key)
    root_service_id       int         NULL,       -- logical -> service.id
    root_name_id          int         NULL,       -- logical -> span_name.id
    span_count            int         NOT NULL,
    error_count           int         NOT NULL,
    outcome               smallint    NOT NULL,   -- 1 OK (error_count = 0), 2 ERROR
    service_ids           int[]       NOT NULL,
    attributes            jsonb       NOT NULL DEFAULT '{}'::jsonb,
        -- retained keys -> sorted array of distinct transformed values:
        -- {"user_id":["9f86d081…"],"client_id":["bookstore-web"],"auth.factor":["SMS"]}
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

------------------------------------------------------------------------
-- business_operation_digest: RANGE(op_day), DAILY. One row per (survivor) business operation.
------------------------------------------------------------------------
CREATE TABLE tc.business_operation_digest (
    first_seen          timestamptz NOT NULL,
    last_seen           timestamptz NOT NULL,
    compacted_at        timestamptz NOT NULL DEFAULT now(),
    operation_id        uuid        NOT NULL,   -- = business_operation.id (uuidv7, survivor only)
    op_day              date        NOT NULL,   -- = first_seen::date UTC (partition key)
    operation_definition_id int         NOT NULL,   -- logical -> business_operation_definition.id
    trace_count         int         NOT NULL,
    outcome             smallint    NOT NULL,   -- OperationOutcome: 0 UNKNOWN, 1 SUCCESS, 2 FAILED, 3 INCOMPLETE
    display_value       text        NOT NULL,
    outcome_detail      text        NULL,       -- raw outcomeKey value(s), max 512 B
    steps               jsonb       NOT NULL,
        -- [{"stepLabel":"first-factor","traceId":"4bf9…","startTime":"2026-11-21T14:03:12.123456Z",
        --   "durationMs":412,"outcome":"OK"}, …]  ordered by startTime
    attributes          jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- union of the members' retained attributes
    CONSTRAINT business_operation_digest_pk         PRIMARY KEY (operation_id, op_day),
    CONSTRAINT business_operation_digest_day_ck     CHECK (op_day = (first_seen AT TIME ZONE 'UTC')::date),
    CONSTRAINT business_operation_digest_time_ck    CHECK (last_seen >= first_seen),
    CONSTRAINT business_operation_digest_outcome_ck CHECK (outcome BETWEEN 0 AND 3),
    -- outcome_detail is cut to CorrelationLimits.maxDecidingValueLength by the application
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

------------------------------------------------------------------------
-- Root of a merge chain: follows business_operation.merged_into to the survivor.
-- Path compression (§6.7) keeps chains at ≤ 1 hop; the depth cap guards against a corrupt cycle.
-- Probes the PK of each retained monthly partition (2–3 under ADR-13) because first_seen is unknown.
------------------------------------------------------------------------
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

------------------------------------------------------------------------
-- Seed: retention tiers (replaces the 003 values) and the default exclusion
------------------------------------------------------------------------
UPDATE tc.retention_policy
SET retention = make_interval(days => ${fullDataDays}), tier = 'FULL';        -- all 7 existing rows

INSERT INTO tc.retention_policy (table_name, retention, partition_granularity, precreate, tier) VALUES
    ('trace_digest',              make_interval(days => ${fullDataDays} + ${digestDays}), 'DAY', interval '3 days', 'DIGEST'),
    ('business_operation_digest', make_interval(days => ${fullDataDays} + ${digestDays}), 'DAY', interval '3 days', 'DIGEST');

INSERT INTO tc.compaction_exclusion (service_name, root_name_pattern, description, created_by)
VALUES (NULL, 'GET /actuator/%', 'Spring Boot actuator probes (health, metrics)', 'system');

UPDATE tc.config_revision SET revision = revision + 1, updated_at = now() WHERE id = 1;

------------------------------------------------------------------------
-- Grants (same role model as §13.2)
------------------------------------------------------------------------
-- tc_maintenance acts as tc_owner and needs no grants (DigestCompactor, PartitionManager).
GRANT SELECT ON tc.compaction_exclusion TO tc_ingester, tc_enricher;      -- config snapshot, read-only
GRANT SELECT, INSERT, UPDATE, DELETE ON tc.compaction_exclusion TO tc_admin;
GRANT SELECT ON tc.compaction_state TO tc_admin;                          -- ops page: compaction lag
GRANT EXECUTE ON FUNCTION tc.operation_root(uuid) TO tc_query;            -- read path (§7.7–§7.9)
-- tc_admin already has SELECT, INSERT, UPDATE on indexed_attribute, business_operation_definition and
-- retention_policy, which covers the new columns.
-- tc_query: the 004 ALTER DEFAULT PRIVILEGES already grants SELECT on every new table
-- (trace_digest, business_operation_digest, compaction_exclusion, compaction_state).
```

**Seeded `retention_policy` after changelog 005** (defaults n = 7, m = 90):

| table_name | tier | retention | granularity | Effective life of the data |
|---|---|---|---|---|
| `span`, `trace_attribute` | FULL | 7 d | HOUR | 7 d + up to about 1 d drop-gate slack (§8.7) |
| `span_link`, `trace_summary`, `trace_enrichment` | FULL | 7 d | DAY | 7 d + about 1 d slack |
| `trace_correlation` | FULL | 7 d | DAY | 7 d + 1 d slack + `max(merge_window)` + 1 d step-label hold (§8.7), about 10 d by default |
| `business_operation` | FULL | 7 d | MONTH | until the whole month is older than 7 d **and** compacted, at most about 40 d |
| `trace_digest` | DIGEST | 97 d | DAY | filled at about day n + 1, dropped at n + m |
| `business_operation_digest` | DIGEST | 97 d | DAY | filled at about day n + 2, dropped at n + m |

The control plane keeps the two tiers consistent. `PUT /api/v1/admin/retention-policies/tiers` with `{"fullDataDays":7,"digestDays":90}` sets `retention = fullDataDays` on every FULL row and `retention = fullDataDays + digestDays` on every DIGEST row, in one transaction with the `config_revision` bump. It validates `fullDataDays ≥ 2`, `fullDataDays > ceil(max(business_operation_definition.merge_window))` in days, and `digestDays ≥ 0`. `digestDays = 0` disables the DIGEST tier: DigestCompactor stops and the drop gate is skipped.

---
## 5. Data encoding: from `TraceMessage` v1 to rows

### 5.1 Identifiers

| Wire field | Wire form | Column | Conversion |
|---|---|---|---|
| `traceId` | 32 lowercase hex chars | `uuid` | **Java:** `new UUID(Long.parseUnsignedLong(h.substring(0,16),16), Long.parseUnsignedLong(h.substring(16),16))`.<br>**SQL:** `'4bf92f3577b34da6a3ce929d0e0e4736'::uuid` (PostgreSQL accepts 32 hex digits without hyphens).<br>The byte order is preserved. **Back to hex:** `replace(trace_id::text, '-', '')`.<br>All-zero ids are invalid and the fragment goes to the DLQ. |
| `spanId`, `parentSpanId`, link `spanId` | 16 hex chars | `bigint` | **Java:** `Long.parseUnsignedLong(h, 16)`. The unsigned 64-bit value is reinterpreted as a signed two's-complement `long`, so `ffffffffffffffff` becomes −1.<br>**SQL:** `('x' \|\| h)::bit(64)::bigint`. **Back to hex:** `lpad(to_hex(span_id), 16, '0')` (for negatives, `to_hex` prints the two's-complement form).<br>A `parentSpanId` of `"0000000000000000"` becomes `NULL`. A `spanId` of 0 is invalid. |
| `spanContext.traceFlags` | signed `byte` | `trace_flags smallint` | `flags & 0xFF`, so the wire value −128 becomes 128. |
| `spanContext.traceState` | `Map<String,String>` | `trace_state text` | Serialised as W3C `k1=v1,k2=v2`. The producer map has already lost the original member order, which is a known limitation. `NULL` when empty. |
| `kind` | enum name | `kind smallint` | Uses OTLP numbering: INTERNAL 1, SERVER 2, CLIENT 3, PRODUCER 4, CONSUMER 5. Unknown values map to 0. |
| `status.code` | `UNSET`/`OK`/`ERROR` | `status_code smallint` | 0 / 1 / 2. `status.description` is stored only when non-empty. |

The duplicate ids inside `spanContext` and `parentSpanContext` are validated against the top-level fields. On mismatch the span is counted and the top-level value wins. They are not stored.

`spanContext.remote` and `parentSpanContext.remote` are stored as flag bits (§5.5).

### 5.2 Time

The wire carries epoch nanoseconds as strings. Per span:

```
start_ns    = Long.parseLong(startEpochNanos)
end_ns      = Long.parseLong(endEpochNanos)
start_time  = timestamptz with µs precision = floor(start_ns / 1000) µs since epoch
duration_ns = hasEnded && end_ns >= start_ns ? end_ns - start_ns : 0   (+ flag NOT_ENDED / NEGATIVE_DURATION)
```

- The conversion is done in Java, because it is exact.
- In SQL, never use `to_timestamp(ns / 1e9)`. It goes through `double precision`, whose roughly 16 significant digits cannot represent epoch seconds with 6 decimal places plus rounding, so it loses microseconds.
- The exact SQL form is `timestamptz 'epoch' + (ns / 1000) * interval '1 microsecond'`.

Consequences:
- Sub-microsecond ordering between sibling spans is lost. The UI orders by `(start_time, span_id)`.
- `trace_summary.duration_ns` is computed from µs-resolution bounds. It can differ from the exact root `duration_ns` by less than 1 µs.
- Event times inside `events` jsonb keep the full nanosecond string.

### 5.3 Attribute encoding in jsonb ("type-preserving native JSON")

`span.attributes`, `resource.attributes`, `instrumentation_scope.attributes`, the attribute maps inside `events` and `links`, `span_link.attributes` and `trace_enrichment.value` all use the same encoding:
- every attribute is stored as its **native JSON value**;
- a JSON object (which no OTel primitive or array value can be) is used as an explicit **type wrapper** only where the native form would be ambiguous or lossy.

| OTel `AttributeType` | Wire `value` | jsonb stored | Notes |
|---|---|---|---|
| `STRING` | JSON string | `"u-42"` | Truncated to 4 KiB on a UTF-8 boundary. The span gets flag `ATTR_TRUNCATED`. |
| `BOOLEAN` | JSON boolean | `true` | |
| `LONG` | JSON number | `1763733600123456789` | **The ingester parses the value with Jackson as `long`** (`JsonParser.getLongValue()` / `NumberType.LONG`), never via `double`. jsonb stores numbers as `numeric`, so all 64 bits are preserved exactly. A LONG prints without a decimal point. |
| `DOUBLE` | JSON number | `0.25`, `3.0` | Written with a decimal point or exponent, so it is distinguishable from LONG. jsonb `numeric` keeps the display scale, so `3.0` stays `3.0`. |
| `DOUBLE` NaN / ±Infinity | non-JSON token or string | `{"@t":"DOUBLE","v":"NaN"}` | JSON cannot represent non-finite numbers, so they use the wrapper form. |
| `STRING_ARRAY` | JSON array | `["SMS","PUSH"]` | Each element is truncated to 4 KiB. At most 128 elements; beyond that, flag `ATTR_TRUNCATED`. |
| `BOOLEAN_ARRAY` / `LONG_ARRAY` / `DOUBLE_ARRAY` | JSON array | `[true,false]`, `[1,2]`, `[1.5]` | The element rules match the scalar rules. |
| Empty array of any type | `[]` | `{"@t":"LONG_ARRAY","v":[]}` | An empty array carries no type information otherwise. |
| Unknown future type | anything | `{"@t":"<type>","v":<raw>}` | Forward compatibility. The value is never dropped. |

Reasons for this encoding:
- It is about 30–40% smaller than a uniform `{"type":…,"value":…}` per attribute, which is what the wire uses.
- It is directly usable with the `->`, `@>` and `jsonb_path_*` operators in brute-force search (UC-13).
- It is lossless, including 64-bit LONG values.

**The UI precision rule.** JavaScript `Number` loses precision above 2^53. `tc-query-api` therefore serialises LONG attributes as JSON *strings*, with a `type` hint in the DTO. A LONG is recognised as a jsonb `number` whose text has no `.` and no exponent.

Key catalogue:
- each key seen for the first time is inserted into `attribute_key` together with the type it was first seen with;
- if a later span carries the same key with a different type, `value_type` is updated once to `MIXED`.

### 5.4 Values in `trace_attribute` (registered keys only)

| Source type | `value_str` | `value_num` |
|---|---|---|
| STRING | The string, cut to 512 bytes (UTF-8 safe). | NULL. If the key is `numeric` and the string parses as a finite double, the parsed value. |
| LONG | Decimal text, exact, for example `"1763733600123456789"`. | `(double) value` if the key is `numeric` (approximate above 2^53). |
| DOUBLE | Shortest round-trip text (`Double.toString` normalised). | The value. |
| BOOLEAN | `"true"` / `"false"` | NULL |
| *_ARRAY | **One row per distinct element.** | As for the scalar type. |

Consequences:
- Equality on a LONG uses `value_str`, which is exact.
- Range predicates use `value_num`, which is approximate.
- Arrays have *contains* semantics.

### 5.5 Other span columns

`flags smallint` bits:

| Bit | Value | Meaning |
|---|---|---|
| 0 | `0x01` | `ATTR_TRUNCATED`: an attribute value or array was cut |
| 1 | `0x02` | `NOT_ENDED`: `hasEnded = false` |
| 2 | `0x04` | `PARENT_REMOTE`: `parentSpanContext.remote` |
| 3 | `0x08` | `EVENTS_OR_LINKS_TRUNCATED` |
| 4 | `0x10` | `NEGATIVE_DURATION_CLAMPED` |
| 5 | `0x20` | `INDEX_VALUE_TRUNCATED`: a `trace_attribute` value was cut to 512 B |

`dropped_counts`:
- format `{"attributes":n,"events":n,"links":n}`;
- written only when some count is greater than 0, otherwise `NULL`;
- the source is `droppedAttributesCount`, `droppedEventsCount` and `droppedLinksCount`.

`events` is a JSON array of `{"name","time","attributes","dropped"}`:
- `time` is the epoch-nanos string;
- `dropped` appears only when it is greater than 0;
- at most 128 events per span.

`links` is a JSON array of `{"traceId","spanId","traceState","attributes"}`. The full list is kept for rendering. Each link is also a row in `span_link`, which serves graph queries.

The span name is resolved through `span_name (service_id, name)`. A service that already has 5,000 names is a cardinality explosion, for example a URL used as the span name. In that case:
- `name_id` is NULL and `name_overflow` holds the name;
- a metric is emitted.

### 5.6 Resource and scope deduplication

```
text(s)             = utf8Length(s) + ':' + utf8(s)
canonical(resource) = text(schemaUrl) + concat(sort_by_key(attrs).map(a ->
                          text(a.key) + text(a.type) + a.tokens.size + ':' + concat(a.tokens.map(text))))
resource.id         = xxHash64(canonical, seed=0)   as signed long
```

Every text carries its length and every array its size, so two different contents never share an encoding (e.g. `[]` and `[""]`, or a value containing a separator character).

The probability of a collision among n distinct resources is about n²/2^65. That is 2.7·10⁻¹⁰ for n = 100,000.

On a `DictionaryCache` miss, the ingester inserts the resource and re-reads the stored attributes once. If they differ, it increments the `resource_hash_collision_total` metric and stores the span with the existing id. That is a mis-attribution, not a loss, and it is accepted as negligible.

`service_id` is derived from the resource attribute `service.name`. If it is missing, the value is `unknown_service`.

---

## 6. Write path

### 6.1 Transaction boundaries (tc-ingester, one Kafka poll batch)

```
poll() -> N records (max.poll.records, e.g. 500 fragments ~ 2-5k spans)
  1. parse + validate + normalise (in memory); poison records -> BatchListenerFailedException(index)
  2. dictionary resolution (cache; on miss: short autocommit statements, §6.2)
  3. BEGIN
       COPY stg_span, stg_trace, stg_attr, stg_link, stg_corr  (PgJDBC CopyManager)
       one writable-CTE statement (§6.4)
     COMMIT                                  -- synchronous_commit = on
  4. return from listener -> container commits offsets (AckMode BATCH)
```

**Dictionary inserts run outside the main transaction, in step 2.**
- If the main transaction rolls back, the dictionary rows remain. That is harmless, because they are referenced by nothing yet and are reused by the retry.
- The main transaction stays short and never waits on a dictionary row lock held by another ingester thread.

**Failure modes:**
- **A crash after COMMIT but before the offset commit.** The batch is redelivered and the idempotent writes (§6.6) absorb it.
- **A database error in step 3.** The whole batch is retried with `ExponentialBackOff`, up to 60 s, indefinitely (UC-17).
- **A data error the validator missed.** SQLSTATE classes `22` and `23`, except the missing-partition case. The writer bisects the batch: it retries halves until a single record fails, then throws `BatchListenerFailedException` for that index. That costs about log₂(n) extra transactions, and only for poison.

**Ordering and locking.** All fragments of a trace arrive on the same Kafka partition, so the same consumer thread handles them (key = traceId). Consequences:
- two ingest transactions never touch the same `trace_summary` row concurrently;
- no row-lock waits and no deadlocks occur between ingest threads.

### 6.2 Dictionary insert-if-absent

The pattern is the same for `service`, `span_name` and `attribute_key`, with `resource` and `instrumentation_scope` keyed by hash. Only cache misses reach the database.

```sql
-- $1 text[] = names missing from the cache
WITH ins AS (
    INSERT INTO tc.service (name)
    SELECT n FROM unnest($1::text[]) AS n
    ON CONFLICT (name) DO NOTHING
    RETURNING id, name
)
SELECT id, name FROM ins
UNION ALL
SELECT s.id, s.name FROM tc.service s WHERE s.name = ANY ($1::text[]);
```

The `SELECT` branch uses the statement's snapshot, so it cannot see rows that a *concurrent* transaction committed after the statement started. The `ON CONFLICT` branch skips those same rows.

The caller therefore checks for names that are still missing and re-runs the plain `SELECT` for them. One retry is always enough, because the conflicting transaction has committed by then.

To avoid wasting identity values (`ON CONFLICT DO NOTHING` still consumes `nextval`), the cache first runs `SELECT … WHERE name = ANY($1)` and inserts only the names that are truly missing.

Resources and scopes:

```sql
INSERT INTO tc.resource (id, service_id, schema_url, attributes, first_seen, last_seen)
SELECT * FROM unnest($1::bigint[], $2::int[], $3::text[], $4::jsonb[], $5::timestamptz[], $5::timestamptz[])
ON CONFLICT (id) DO NOTHING;

-- last_seen heartbeat, at most once per resource per hour (cache-driven), HOT-friendly:
UPDATE tc.resource SET last_seen = $2
WHERE id = ANY ($1::bigint[]) AND last_seen < $2 - interval '1 hour';
```

### 6.3 Session-scoped staging tables

`COPY` has no `ON CONFLICT`. Rows are therefore copied into temporary staging tables and moved with set-based SQL.

The temporary tables are created **once per physical connection** with `ON COMMIT DELETE ROWS`, which keeps catalogue churn low:

```sql
CREATE TEMP TABLE IF NOT EXISTS stg_span (
    trace_id uuid, span_id bigint, parent_span_id bigint, start_time timestamptz, duration_ns bigint,
    service_id int, name_id int, name_overflow text, kind smallint, status_code smallint,
    status_message text, resource_id bigint, scope_id bigint, trace_flags smallint, trace_state text,
    attributes jsonb, events jsonb, links jsonb, dropped_counts jsonb, flags smallint
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS stg_trace (
    trace_id uuid PRIMARY KEY, trace_day date NOT NULL, fragment_count int NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS stg_attr (
    trace_id uuid, span_id bigint, start_time timestamptz,
    key_id int, value_str text COLLATE "C", value_num float8, origin smallint
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS stg_link (
    trace_id uuid, span_id bigint, start_time timestamptz,
    linked_trace_id uuid, linked_span_id bigint, attributes jsonb
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS stg_corr (
    trace_id uuid, span_id bigint, start_time timestamptz,
    operation_definition_id int, correlation_key_id int, value text COLLATE "C", origin smallint
) ON COMMIT DELETE ROWS;
```

**Pooling requirement.** Temporary tables that live across transactions need a session-bound server connection. PgBouncer *transaction* pooling breaks `ON COMMIT DELETE ROWS` and `PRESERVE ROWS` temporary tables. So:
- `tc-ingester` connects through a dedicated PgBouncer **session-mode** pool `tc_ingest`;
- its Hikari pool is fixed-size, so session pooling costs nothing.

The fallback for transaction pooling is `ON COMMIT DROP`, which creates the tables in every transaction. It works, but produces about 5 `pg_class`/`pg_attribute` inserts and deletes per batch. At about 100 transactions per second that bloats the catalogue, so it is not recommended.

Other settings and details:
- `temp_buffers = 64MB` is set for role `tc_ingester`, so staging stays in local memory.
- Autovacuum never analyses temporary tables. The CTE below is written so that the default estimates lead to hash joins over small inputs. For batches over 20,000 spans, the writer issues `ANALYZE stg_span` after `COPY`.
- The `COPY` format is `FORMAT binary`: `uuid` as 16 raw bytes, `timestamptz` as int64 µs since 2000-01-01, and `jsonb` as a version byte `0x01` followed by the text.
- `FORMAT text` is the simpler fallback. Its throughput difference is measured in the benchmark (§11).

### 6.4 The ingest statement (writable CTE)

```sql
WITH ins_span AS (
    INSERT INTO tc.span (start_time, span_id, duration_ns, resource_id, scope_id, parent_span_id,
                         trace_id, service_id, name_id, kind, status_code, trace_flags, flags,
                         name_overflow, status_message, trace_state, dropped_counts,
                         attributes, events, links)
    SELECT start_time, span_id, duration_ns, resource_id, scope_id, parent_span_id,
           trace_id, service_id, name_id, kind, status_code, trace_flags, flags,
           name_overflow, status_message, trace_state, dropped_counts,
           attributes, events, links
    FROM stg_span
    ON CONFLICT (trace_id, span_id, start_time) DO NOTHING
    RETURNING trace_id, span_id, start_time, parent_span_id, duration_ns,
              service_id, name_id, status_code
),
agg AS (
    SELECT i.trace_id, t.trace_day, t.fragment_count,
           min(i.start_time)                                               AS start_time,
           max(i.start_time + (i.duration_ns / 1000) * interval '1 microsecond') AS end_time,
           count(*)::int                                                   AS span_count,
           count(*) FILTER (WHERE i.status_code = 2)::int                  AS error_count,
           array_agg(DISTINCT i.service_id ORDER BY i.service_id)          AS service_ids,
           (array_agg(i.service_id ORDER BY i.start_time) FILTER (WHERE i.parent_span_id IS NULL))[1] AS root_service_id,
           (array_agg(i.name_id    ORDER BY i.start_time) FILTER (WHERE i.parent_span_id IS NULL))[1] AS root_name_id,
           (array_agg(i.span_id    ORDER BY i.start_time) FILTER (WHERE i.parent_span_id IS NULL))[1] AS root_span_id
    FROM ins_span i
    JOIN stg_trace t USING (trace_id)
    GROUP BY i.trace_id, t.trace_day, t.fragment_count
),
up_summary AS (
    INSERT INTO tc.trace_summary AS s
           (trace_id, trace_day, start_time, end_time, duration_ns,
            root_service_id, root_name_id, root_span_id,
            span_count, error_count, service_ids, fragment_count,
            first_ingested_at, last_ingested_at)
    SELECT a.trace_id, a.trace_day, a.start_time, a.end_time,
           (extract(epoch FROM a.end_time - a.start_time) * 1000000)::bigint * 1000,
           a.root_service_id, a.root_name_id, a.root_span_id,
           a.span_count, a.error_count, a.service_ids, a.fragment_count,
           now(), now()
    FROM agg a
    ON CONFLICT (trace_id, trace_day) DO UPDATE SET
        start_time       = LEAST(s.start_time, EXCLUDED.start_time),
        end_time         = GREATEST(s.end_time, EXCLUDED.end_time),
        duration_ns      = (extract(epoch FROM GREATEST(s.end_time, EXCLUDED.end_time)
                                            - LEAST(s.start_time, EXCLUDED.start_time)) * 1000000)::bigint * 1000,
        root_service_id  = COALESCE(s.root_service_id, EXCLUDED.root_service_id),   -- first root wins
        root_name_id     = COALESCE(s.root_name_id,    EXCLUDED.root_name_id),
        root_span_id     = COALESCE(s.root_span_id,    EXCLUDED.root_span_id),
        span_count       = s.span_count  + EXCLUDED.span_count,
        error_count      = s.error_count + EXCLUDED.error_count,
        service_ids      = ARRAY(SELECT DISTINCT x FROM unnest(s.service_ids || EXCLUDED.service_ids) AS x ORDER BY x),
        fragment_count   = s.fragment_count + EXCLUDED.fragment_count,
        last_ingested_at = EXCLUDED.last_ingested_at
    RETURNING 1
),
ins_attr AS (
    INSERT INTO tc.trace_attribute (ts, value_num, trace_id, key_id, origin, value_str)
    SELECT a.start_time, a.value_num, a.trace_id, a.key_id, a.origin, a.value_str
    FROM stg_attr a
    JOIN ins_span i ON i.trace_id = a.trace_id AND i.span_id = a.span_id AND i.start_time = a.start_time
    RETURNING 1
),
ins_link AS (
    INSERT INTO tc.span_link (start_time, span_id, linked_span_id, trace_id, linked_trace_id, attributes)
    SELECT l.start_time, l.span_id, l.linked_span_id, l.trace_id, l.linked_trace_id, l.attributes
    FROM stg_link l
    JOIN ins_span i ON i.trace_id = l.trace_id AND i.span_id = l.span_id AND i.start_time = l.start_time
    RETURNING 1
),
ins_corr AS (
    INSERT INTO tc.trace_correlation (observed_at, trace_id, operation_definition_id, correlation_key_id, origin, value)
    SELECT c.start_time, c.trace_id, c.operation_definition_id, c.correlation_key_id, c.origin, c.value
    FROM stg_corr c
    JOIN ins_span i ON i.trace_id = c.trace_id AND i.span_id = c.span_id AND i.start_time = c.start_time
    RETURNING 1
)
SELECT (SELECT count(*) FROM ins_span)   AS spans_inserted,
       (SELECT count(*) FROM up_summary) AS traces_upserted,
       (SELECT count(*) FROM ins_attr)   AS attrs_inserted,
       (SELECT count(*) FROM ins_link)   AS links_inserted,
       (SELECT count(*) FROM ins_corr)   AS correlations_inserted;
```

How the statement executes:
- Every data-modifying CTE runs exactly once, against the same snapshot. The `RETURNING` set of `ins_span` is materialised once and reused by every consumer.
- Tuple routing sends each row to its hourly or daily partition.
- `ON CONFLICT` on a partitioned table uses the partition's unique index. That index includes the partition key, so a conflicting row can only live in the same partition.

How `stg_attr` and `stg_corr` are prepared:
- `AttributeIndexer` and `CorrelationExtractor` apply the in-memory `IndexedAttributeRegistry` (from the `config_revision` snapshot).
- They **deduplicate within the batch** per `(trace_id, key_id, value_str)` (respectively `(trace_id, type, value)`), keeping the first span that carried the value.
- `service.name` rows (origin 2) are emitted once per (trace, service) per batch.
- `span.name` rows (origin 4) are emitted for root spans only.
- `status = 'ERROR'` rows (origin 4) are emitted once per trace per batch that contains an error span.

**`TraceDayResolver`.** Before COPY, every trace in the batch gets a `trace_day`:
1. The Caffeine cache (traceId → trace_day) is consulted first.
2. On a miss, the resolver runs one batched lookup:
   ```sql
   SELECT trace_id, min(trace_day) FROM tc.trace_summary
   WHERE trace_id = ANY ($1::uuid[]) AND trace_day BETWEEN $2::date - 1 AND $2::date + 1
   GROUP BY trace_id;
   ```
   Here `$2` is the UTC date `d` of the fragment's minimum start, which equals "today" in steady state. The probe covers `d − 1`, `d` and `d + 1`. `d + 1` catches a summary row created by an earlier-arriving fragment that started after midnight, when this fragment started before midnight. Using the fragment's own date rather than the wall clock keeps a replay of older data (UC-18) consistent.
3. Otherwise the resolver uses the date of the fragment's minimum `start_time`.

The guarantee is `date(trace.start_time) ≤ trace_day ≤ date(trace.start_time) + 1` for traces shorter than about one day. The read path relies on it (§7.2).

### 6.5 Enrichment write (tc-enricher)

One transaction per enricher poll batch:

```sql
WITH src AS (
    SELECT * FROM unnest($1::timestamptz[], $2::uuid[], $3::bigint[], $4::bigint[], $5::int[],
                         $6::int[], $7::jsonb[], $8::int[], $9::bigint[],
                         $10::text[], $11::float8[], $12::boolean[], $13::int[], $14::int[])
         AS t(observed_at, trace_id, span_id, source_id, source_revision,
              key_id, value, kafka_partition, kafka_offset,
              value_str, value_num, indexed, corr_type_id, corr_key_id)
),
ins AS (
    INSERT INTO tc.trace_enrichment (observed_at, source_id, kafka_offset, span_id, trace_id,
                                     source_revision, key_id, kafka_partition, value)
    SELECT observed_at, source_id, kafka_offset, span_id, trace_id,
           source_revision, key_id, kafka_partition, value
    FROM src
    ON CONFLICT (source_id, kafka_partition, kafka_offset, key_id, observed_at) DO NOTHING
    RETURNING source_id, kafka_partition, kafka_offset, key_id, observed_at
),
new_rows AS (
    SELECT s.* FROM src s
    JOIN ins i USING (source_id, kafka_partition, kafka_offset, key_id, observed_at)
),
ins_attr AS (
    INSERT INTO tc.trace_attribute (ts, value_num, trace_id, key_id, origin, value_str)
    SELECT observed_at, value_num, trace_id, key_id, 3, value_str
    FROM new_rows WHERE indexed
    RETURNING 1
)
INSERT INTO tc.trace_correlation (observed_at, trace_id, operation_definition_id, correlation_key_id, origin, value)
SELECT observed_at, trace_id, corr_type_id, corr_key_id, 3, value_str
FROM new_rows WHERE corr_type_id IS NOT NULL;
```

- The flags `indexed`, `corr_type_id` and `corr_key_id` are computed in Java from the configuration snapshot.
- Enrichment can arrive before or after the spans of its trace (UC-06). No pending queue exists, because the join happens at read time.
- An `observed_at` outside the retained or pre-created range is handled like a span timestamp: too old means dropped plus a metric, and later than now + 1 day goes to `tc-enrichment.dlq`.

### 6.6 Why counters and derived rows are replay-safe (UC-02, UC-18)

| Write | Idempotency mechanism |
|---|---|
| `span` | `ON CONFLICT (trace_id, span_id, start_time) DO NOTHING`. A replayed span produces no `RETURNING` row. |
| `trace_summary.span_count`, `error_count`, `fragment_count` | Aggregated **only from `ins_span`**, the rows actually inserted. If a replayed fragment inserts 0 spans, the trace does not appear in `agg`, so no update happens. |
| `start_time`, `end_time`, `duration_ns`, `service_ids`, root columns | `LEAST`/`GREATEST`, set union and first-non-null are idempotent and commutative, so arrival order does not matter. |
| `trace_attribute`, `span_link`, `trace_correlation` (ingester) | Joined to `ins_span`, so they are written only together with a new span. A partially duplicated batch writes derived rows only for its new spans. Duplicate attribute rows from *different* fragments of the same trace are tolerated and removed with `DISTINCT` at read time. |
| `trace_enrichment` and its derived rows | `UNIQUE (source_id, kafka_partition, kafka_offset, key_id, observed_at)`. `observed_at` is the Kafka record timestamp, which is identical on replay. Derived rows come from `RETURNING` only. |
| Dictionaries and resources | Insert-if-absent. |
| Resolver | Idempotent trace-by-trace linking with a lagging watermark (§6.7). |

`ON CONFLICT DO NOTHING` also absorbs duplicates *within one statement*, for example two copies of the same fragment in one poll batch. `ON CONFLICT DO UPDATE` on `trace_summary` would raise "cannot affect row a second time", which is why `agg` groups by the conflict key. The Java side additionally deduplicates spans per batch, which is cheaper.

### 6.7 Correlation resolver (tc-maintenance, single leader, every 10 s)

**Watermark correctness.** Identity values are assigned at insert time, but transactions commit in a different order. Suppose a transaction holding `id = 100` commits after the resolver has read `id = 101`. A naive `id > watermark` would skip row 100 forever.

The fix:
1. The ingester and enricher roles have `transaction_timeout = 120s` (PG 17+).
2. The resolver keeps the persisted watermark `correlation_resolver_state.last_processed_id` at the highest id read in a run that started at least `transaction_timeout + 30 s` ago.
3. Every run therefore re-reads up to about 2.5 minutes of rows. That is about 75,000 rows at M, a cheap index range scan.
4. Linking is idempotent, so re-reading is harmless.

```sql
-- R1: new input (scans the (id) index of each retained daily partition; empty ranges cost one descent each)
SELECT id, observed_at, trace_id, operation_definition_id, correlation_key_id, value
FROM tc.trace_correlation
WHERE id > $watermark
ORDER BY id
LIMIT 20000;

-- R2: other values already known for the same traces (aliases that arrived earlier)
SELECT trace_id, operation_definition_id, value, min(observed_at) AS observed_at
FROM tc.trace_correlation
WHERE trace_id = ANY ($traces::uuid[])
GROUP BY trace_id, operation_definition_id, value;

-- R3: existing operations for all values in the component candidates
SELECT cv.operation_definition_id, cv.value, cv.operation_id, cv.operation_first_seen,
       bo.last_seen, bo.merged_into
FROM tc.correlation_value cv
JOIN tc.business_operation bo
  ON bo.id = cv.operation_id AND bo.first_seen = cv.operation_first_seen   -- pruned to one partition
WHERE (cv.operation_definition_id, cv.value) IN (SELECT * FROM unnest($types::int[], $values::text[]));
```

**Linking in memory, per business operation definition** (`OperationLinker`, a stateless domain service):
- The traces of the batch are linked one at a time, in the order of their earliest new observation, exactly as if each came in a run of its own.
- A value can join its operation (from R3) if the trace carried it within the operation's merge window. Outside the window the value was recycled and is treated like a new value, unless the trace already carried it in an earlier run (from R2): then a newer operation has taken it over and it is left out. This handles session or code reuse.
- For each trace:
  - **no operation:** create one;
  - **one operation:** attach to it;
  - **several operations:** keep the survivor (named first, then earliest `first_seen`, then smallest id), and set `merged_into = survivor` on the others (UC-10), unless the merged operation would exceed the configured size limit: then nothing is merged and each of the operations counts the trace.
- `trace_count` counts each trace once: an operation that already holds one of the trace's earlier values counts it already. A replay therefore does not inflate it.

All writes go in one transaction. `SELECT … FOR UPDATE` on the state row serialises a second leader if ShedLock leases overlap:

```sql
SELECT last_processed_id FROM tc.correlation_resolver_state WHERE id = 1 FOR UPDATE;

INSERT INTO tc.business_operation (id, first_seen, last_seen, operation_definition_id, trace_count, display_value)
SELECT * FROM unnest($ids::uuid[], $first::timestamptz[], $last::timestamptz[], $types::int[], $counts::int[], $display::text[]);
-- ids are generated in Java as UUIDv7 or via SELECT uuidv7() so they are known before the insert

UPDATE tc.business_operation bo
SET last_seen = GREATEST(bo.last_seen, u.last_seen), trace_count = bo.trace_count + u.added
FROM unnest($sid::uuid[], $sfs::timestamptz[], $sls::timestamptz[], $sadd::int[]) AS u(id, first_seen, last_seen, added)
WHERE bo.id = u.id AND bo.first_seen = u.first_seen;

UPDATE tc.business_operation bo
SET merged_into = u.survivor
FROM unnest($lid::uuid[], $lfs::timestamptz[], $lsurv::uuid[]) AS u(id, first_seen, survivor)
WHERE bo.id = u.id AND bo.first_seen = u.first_seen;
-- path compression: anything that pointed at a loser now points at the survivor
UPDATE tc.business_operation SET merged_into = $survivor WHERE merged_into = ANY ($losers::uuid[]);

INSERT INTO tc.correlation_value AS cv (operation_definition_id, value, operation_id, operation_first_seen, updated_at)
SELECT * , now() FROM unnest($vt::int[], $vv::text[], $vop::uuid[], $vfs::timestamptz[])
ON CONFLICT (operation_definition_id, value) DO UPDATE
   SET operation_id = EXCLUDED.operation_id, operation_first_seen = EXCLUDED.operation_first_seen, updated_at = now()
   WHERE cv.operation_id IS DISTINCT FROM EXCLUDED.operation_id;
UPDATE tc.correlation_value SET operation_id = $survivor, operation_first_seen = $survivor_fs, updated_at = now()
WHERE operation_id = ANY ($losers::uuid[]);

UPDATE tc.correlation_resolver_state SET last_processed_id = $safe_watermark, updated_at = now() WHERE id = 1;
COMMIT;
```

Notes:
- `first_seen` is the partition key of `business_operation`. It is set once, at creation, to the minimum `observed_at` of the component, and **never updated**, because updating it would move the row between partitions.
- If an earlier trace is discovered later (a late or replayed fragment), the timeline still shows it, because the timeline is computed from the traces themselves.
- The `merged_into` updates touch non-indexed columns, so with fillfactor 85 they are HOT updates.

**Worked example (IdP, §10 of the brief):**
- T1 inserts `(idp.login, as-777)`, and the resolver creates operation O1.
- T2 inserts `(idp.login, as-777)` and `(idp.login, code-abc)`. Both values land in one component with O1, so `code-abc` is mapped to O1.
- T3 inserts `(idp.login, code-abc)`. R3 finds O1, so T3 is attached and `trace_count` becomes 3.
- If T3 had been processed before T2, two operations would have existed (O1 for `as-777`, O2 for `code-abc`). T2 then joins both values, which merges them: the survivor is O1 and O2 gets `merged_into = O1`.

### 6.8 Compaction (DIGEST tier, ADR-13)

`DigestCompactor` runs in tc-maintenance under the ShedLock lock `digest-compactor`, every 5 minutes, on a **direct connection to the primary** as `tc_maintenance` (acting as `tc_owner`), with `SET statement_timeout = '15min'` per chunk statement. It reads only `trace_summary`, `trace_attribute` (retained keys), `trace_correlation`, `correlation_value`, `business_operation` and configuration tables. It **never reads `span`**.

**Unit of work and lifecycle.** One `compaction_state` row per `trace_day` D.

| Step | Condition / action | `compaction_state` |
|---|---|---|
| Register | D is eligible once `D + 1 day ≤ now() − n days` (the whole day is older than n). `INSERT … ON CONFLICT DO NOTHING`. Days are always processed **oldest first**. | `PENDING` |
| Phase 1: traces | Ensure the `trace_digest` partition for D exists (on-demand CREATE + ATTACH, §8.7). Digest D in chunks (DC1 below), each chunk in its own transaction together with the progress update. | `RUNNING`, `last_chunk_end` advances; equals `D + 1 day` when phase 1 is complete |
| Phase 2: operations | Digest every eligible business operation (BO1–BO3 below). | `RUNNING` |
| Finish | `VACUUM (FREEZE, ANALYZE)` on `trace_digest_p<D>`. This also flushes the GIN pending list. | `DONE`, `finished_at` |
| Error | The exception text is stored. The next run resumes from `last_chunk_end`. | `FAILED` (retried) |

The drop gate (§8.7) only trusts `DONE`, which is set **after** phase 2. This ordering guarantees that phase 2 can still read the FULL-tier rows of day D.

**Chunks.** `trace_day` D contains traces with `start_time` in [D − 1 day, D + 1 day), because `date(start_time) ≤ trace_day ≤ date(start_time) + 1` (§6.4).
- Chunk 0 is [D − 1 day, D). It holds only the few traces that started just before midnight and were assigned to D.
- Chunks 1–24 are the hours [D + h, D + h + 1 h).
- `last_chunk_end` starts at `D − 1 day`. It is the exclusive upper bound of the last committed chunk, so a restart resumes at the next chunk.

**Attribute window.** The ADR-13 outer window for day D is `ts ∈ [D − 1 h, D + 1 day + 6 h)`. It is also the window the drop gate protects. Each chunk reads the part of that window that can belong to its traces:
- `$attr_from = GREATEST(chunk_min_start − 1 h, D − 1 h)`;
- `$attr_to = LEAST($chunk_to + 6 h, D + 1 day + 6 h)`.

`chunk_min_start` is `$chunk_from` for hourly chunks. For chunk 0 it is the real minimum `start_time`, one probe of `trace_summary_start_idx`. A trace therefore captures retained values with `ts` up to 6–7 h after its start, and that bound is configurable as `tc.compaction.attribute-lookahead`, default 6 h. Later values, which can only be late enrichments, are not digested. This is a documented refinement of ADR-13's day window (§15).

**DC1: digest one chunk of trace_day D** (one transaction with DC2):

```sql
-- $day date; $chunk_from, $chunk_to, $attr_from, $attr_to timestamptz;
-- $retained_key_ids int[] = indexed_attribute.key_id WHERE retain_in_digest AND status <> 'DISABLED'
--                           (taken from the same config snapshot as "retained" below; enables the index condition)
WITH chunk AS (
    SELECT s.trace_id, s.trace_day, s.start_time, s.duration_ns, s.root_service_id, s.root_name_id,
           s.span_count, s.error_count, s.service_ids
    FROM tc.trace_summary s
    WHERE s.trace_day = $day
      AND s.start_time >= $chunk_from AND s.start_time < $chunk_to
),
retained AS (
    SELECT ia.key_id, k.key, ia.digest_transform
    FROM tc.indexed_attribute ia
    JOIN tc.attribute_key k ON k.id = ia.key_id
    WHERE ia.retain_in_digest AND ia.status <> 'DISABLED'
),
attr AS (                                     -- distinct transformed values per (trace, key)
    SELECT DISTINCT a.trace_id, r.key,
           CASE r.digest_transform
               WHEN 'SHA256' THEN encode(sha256(convert_to(a.value_str, 'UTF8')), 'hex')
               WHEN 'MASK'   THEN left(a.value_str, 2) || '***'
               ELSE a.value_str
           END AS v
    FROM tc.trace_attribute a
    JOIN retained r ON r.key_id = a.key_id
    WHERE a.key_id = ANY ($retained_key_ids::int[])
      AND a.ts >= $attr_from AND a.ts < $attr_to
      AND a.trace_id IN (SELECT trace_id FROM chunk)
),
attr_json AS (
    SELECT trace_id, jsonb_object_agg(key, vals) AS attributes
    FROM (SELECT trace_id, key, jsonb_agg(v ORDER BY v) AS vals
          FROM attr GROUP BY trace_id, key) per_key
    GROUP BY trace_id
),
op_raw AS (                                   -- first-observed business operation of the trace
    SELECT DISTINCT ON (c.trace_id) c.trace_id, cv.operation_id
    FROM tc.trace_correlation c
    JOIN tc.correlation_value cv USING (operation_definition_id, value)
    WHERE c.observed_at >= $attr_from AND c.observed_at < $attr_to
      AND c.trace_id IN (SELECT trace_id FROM chunk)
    ORDER BY c.trace_id, c.observed_at, c.operation_definition_id
),
op AS (                                       -- resolved to the root survivor, once per distinct operation
    SELECT r.trace_id, roots.root_id AS business_operation_id
    FROM op_raw r
    JOIN (SELECT operation_id, tc.operation_root(operation_id) AS root_id
          FROM (SELECT DISTINCT operation_id FROM op_raw) d) roots USING (operation_id)
),
excluded_name AS (                            -- root span names matched by compaction_exclusion (dictionary-sized)
    SELECT DISTINCT sn.id AS root_name_id
    FROM tc.compaction_exclusion x
    JOIN tc.span_name sn ON sn.name LIKE x.root_name_pattern
    JOIN tc.service  sv ON sv.id = sn.service_id
    WHERE x.service_name IS NULL OR x.service_name = sv.name
),
cand AS (
    SELECT c.*, o.business_operation_id, aj.attributes,
           (o.business_operation_id IS NULL
            AND c.root_name_id IN (SELECT root_name_id FROM excluded_name)) AS is_excluded
    FROM chunk c
    LEFT JOIN op o        USING (trace_id)
    LEFT JOIN attr_json aj USING (trace_id)
),
ins AS (
    INSERT INTO tc.trace_digest (start_time, duration_ns, compacted_at, trace_id, business_operation_id,
                                 trace_day, root_service_id, root_name_id, span_count, error_count,
                                 outcome, service_ids, attributes)
    SELECT start_time, duration_ns, now(), trace_id, business_operation_id,
           trace_day, root_service_id, root_name_id, span_count, error_count,
           CASE WHEN error_count > 0 THEN 2 ELSE 1 END,
           service_ids, COALESCE(attributes, '{}'::jsonb)
    FROM cand
    WHERE NOT is_excluded
    ORDER BY start_time                       -- keeps trace_digest_start_idx right-leaning (≈ 90 % leaf fill)
    ON CONFLICT (trace_id, trace_day) DO NOTHING
    RETURNING 1
)
SELECT (SELECT count(*) FROM ins)                       AS traces_digested,
       (SELECT count(*) FROM cand WHERE is_excluded)    AS traces_excluded;
```

**DC2: progress** (same transaction, then COMMIT):

```sql
UPDATE tc.compaction_state
SET status = 'RUNNING', last_chunk_end = $chunk_to,
    traces_digested = traces_digested + $digested, traces_excluded = traces_excluded + $excluded,
    started_at = COALESCE(started_at, now()), error = NULL
WHERE trace_day = $day AND last_chunk_end IS NOT DISTINCT FROM $expected_prev_chunk_end;   -- 0 rows -> another leader won; abort
```

Notes on DC1:
- **Plan.** `chunk` is a range on `trace_summary_start_idx` in one daily partition, about 3.6 M rows per hour at M. `attr` is an index-only scan of `trace_attribute_kv_ts_idx` (`key_id = ANY(…)`, `ts` checked in the index, `trace_id` from `INCLUDE`) over the 7–8 hourly partitions of the window, hash-semi-joined to `chunk`. `op` probes `trace_correlation_trace_idx`, or hash-joins the 1–2 pruned daily partitions; the planner decides. Pruning happens at plan time because every bound is a parameter and `tc_maintenance` uses custom plans.
- **Transforms.** `SHA256` is `encode(sha256(convert_to(v,'UTF8')),'hex')` and `MASK` is the first 2 characters + `***`. tc-query-api applies the identical SHA-256 to search values (§7.8). A plain SHA-256 of a low-entropy id can be reversed by enumeration (§13.3), so for such keys prefer pseudonymous ids at the source.
- **Root resolution.** `tc.operation_root()` (§4.7) is called once per distinct operation of the chunk, not once per trace. `correlation_value` already points at survivors after path compression, so the function normally returns its argument after one PK probe per retained month partition.
- **One operation per trace.** If a trace belongs to operations of several business operation definitions, `trace_digest.business_operation_id` holds the one whose correlation row was observed first. `business_operation_digest.steps` still lists the trace under every operation (BO2).
- **Exclusions.** A trace is excluded only if its root span name matches a `compaction_exclusion` row **and** it belongs to no business operation. Traces whose root name overflowed (`root_name_id` NULL) are never excluded.
- **Idempotency.** `ON CONFLICT (trace_id, trace_day) DO NOTHING` together with the guarded progress update makes a re-run of a chunk harmless. That covers a crash between COMMIT and the next chunk, and two leaders. A retained key that is added later is **not retroactive**, because existing digest rows are never rewritten.

**Phase 2: business operations.** An operation is digested once (a) `merged_into IS NULL` (losers are never digested separately), (b) `last_seen < now() − n days`, and (c) phase 1 is complete for every day in `[date(first_seen), date(last_seen) + 1]`. Because `fullDataDays > merge_window` (validated), such an operation is closed: no new trace can join it.

Candidates come from the digests of D − 1 and D, through the partial index `trace_digest_operation_idx`. A daily **sweep** (BO0) catches operations that no trace digest references, for example a trace that belongs to two operations, or a late enrichment that moved `last_seen`. The sweep also feeds the `business_operation` drop gate (§8.7).

```sql
-- BO0 (daily sweep): closed, non-merged operations without a digest, in partitions older than n days
SELECT bo.id, bo.first_seen
FROM tc.business_operation bo
WHERE bo.first_seen < now() - $full_days - interval '1 day'
  AND bo.merged_into IS NULL
  AND bo.last_seen  < now() - $full_days
  AND NOT EXISTS (SELECT 1 FROM tc.business_operation_digest d
                  WHERE d.operation_id = bo.id
                    AND d.op_day = (bo.first_seen AT TIME ZONE 'UTC')::date)
LIMIT 50000;

-- BO1 (phase 2 of day D): candidates and eligibility
WITH cand AS (
    SELECT DISTINCT business_operation_id AS id
    FROM tc.trace_digest
    WHERE trace_day BETWEEN $day - 1 AND $day AND business_operation_id IS NOT NULL
)
SELECT bo.id, bo.first_seen
FROM cand
JOIN tc.business_operation bo
  ON bo.id = cand.id
 AND bo.first_seen >= $bo_oldest_partition_start           -- static pruning bound
 AND bo.first_seen <= uuid_extract_timestamp(cand.id)      -- first_seen ≤ creation time (§7.5)
WHERE bo.merged_into IS NULL
  AND bo.last_seen < now() - $full_days
  AND NOT EXISTS (SELECT 1 FROM tc.business_operation_digest d
                  WHERE d.operation_id = bo.id
                    AND d.op_day = (bo.first_seen AT TIME ZONE 'UTC')::date)
  AND (SELECT count(*) FROM tc.compaction_state cs
       WHERE cs.trace_day BETWEEN (bo.first_seen AT TIME ZONE 'UTC')::date
                              AND (bo.last_seen  AT TIME ZONE 'UTC')::date + 1
         AND cs.last_chunk_end >= (cs.trace_day + 1)::timestamptz)          -- phase 1 complete
      = ((bo.last_seen AT TIME ZONE 'UTC')::date + 1) - (bo.first_seen AT TIME ZONE 'UTC')::date + 1;
```

Java processes the eligible ids in batches of up to 2 000 operations, one transaction per batch. Each batch runs BO2 (SQL, read), then `OutcomeRule.evaluate` (Java), then BO3 (SQL, write).

```sql
-- BO2: per operation: members, steps, attribute union and the inputs of the outcome rule
-- $ids uuid[], $first uuid-aligned timestamptz[]; $min_day/$max_day and $min_obs/$max_obs are the batch-wide
-- static bounds (min first_seen − max merge_window, max last_seen + max merge_window), for plan-time pruning
WITH ops AS (
    SELECT bo.id, bo.first_seen, bo.last_seen, bo.operation_definition_id, bo.display_value,
           ct.merge_window, ct.outcome_rule
    FROM unnest($ids::uuid[], $first::timestamptz[]) AS u(id, first_seen)
    JOIN tc.business_operation bo ON bo.id = u.id AND bo.first_seen = u.first_seen
    JOIN tc.business_operation_definition  ct ON ct.id = bo.operation_definition_id
),
via_corr AS (            -- members through the correlation path (gives the step label)
    SELECT o.id AS operation_id, c.trace_id, c.correlation_key_id, c.observed_at
    FROM ops o
    JOIN tc.correlation_value cv ON cv.operation_id = o.id
    JOIN tc.trace_correlation c
      ON c.operation_definition_id = cv.operation_definition_id AND c.value = cv.value
     AND c.observed_at >= o.first_seen - o.merge_window AND c.observed_at <= o.last_seen + o.merge_window
    WHERE c.observed_at >= $min_obs AND c.observed_at <= $max_obs
),
via_digest AS (          -- members recorded on the trace digest (their correlation rows may have expired)
    SELECT o.id AS operation_id, d.trace_id, NULL::int AS correlation_key_id, d.start_time AS observed_at
    FROM ops o
    JOIN tc.trace_digest d ON d.business_operation_id = o.id
    WHERE d.trace_day BETWEEN $min_day AND $max_day
),
members AS (
    SELECT DISTINCT ON (operation_id, trace_id) operation_id, trace_id, correlation_key_id
    FROM (SELECT * FROM via_corr UNION ALL SELECT * FROM via_digest) m
    ORDER BY operation_id, trace_id, (correlation_key_id IS NULL), observed_at     -- prefer a row with a key
),
steps AS (
    SELECT m.operation_id, d.trace_id, d.start_time, d.duration_ns, d.error_count, d.attributes, ck.step_label
    FROM members m
    JOIN tc.trace_digest d ON d.trace_id = m.trace_id AND d.trace_day BETWEEN $min_day AND $max_day
    LEFT JOIN tc.correlation_key ck ON ck.id = m.correlation_key_id
),
attr_union AS (
    SELECT operation_id, jsonb_object_agg(key, vals) AS attributes
    FROM (SELECT s.operation_id, kv.key, jsonb_agg(DISTINCT v.val ORDER BY v.val) AS vals
          FROM steps s
          CROSS JOIN LATERAL jsonb_each(s.attributes) AS kv(key, arr)
          CROSS JOIN LATERAL jsonb_array_elements(kv.arr) AS v(val)
          GROUP BY s.operation_id, kv.key) per_key
    GROUP BY operation_id
),
outcome_input AS (       -- latest member trace (by start) that carries outcomeKey
    SELECT DISTINCT ON (s.operation_id) s.operation_id,
           s.attributes -> (o.outcome_rule ->> 'outcomeKey') AS outcome_values      -- jsonb array or NULL
    FROM steps s JOIN ops o ON o.id = s.operation_id
    WHERE o.outcome_rule ? 'outcomeKey' AND s.attributes ? (o.outcome_rule ->> 'outcomeKey')
    ORDER BY s.operation_id, s.start_time DESC, s.trace_id DESC
)
SELECT o.id, o.first_seen, o.last_seen, o.operation_definition_id, o.display_value, o.outcome_rule,
       count(s.trace_id)::int                                             AS trace_count,
       bool_or(s.error_count > 0)                                         AS any_error,
       bool_or(s.step_label = o.outcome_rule ->> 'finalStepLabel')        AS final_step_reached,
       oi.outcome_values,
       jsonb_agg(jsonb_build_object(
           'stepLabel',  s.step_label,
           'traceId',    replace(s.trace_id::text, '-', ''),
           'startTime',  to_char(s.start_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'durationMs', s.duration_ns / 1000000,
           'outcome',    CASE WHEN s.error_count > 0 THEN 'ERROR' ELSE 'OK' END)
           ORDER BY s.start_time, s.trace_id) FILTER (WHERE s.trace_id IS NOT NULL) AS steps,
       COALESCE(au.attributes, '{}'::jsonb)                               AS attributes
FROM ops o
LEFT JOIN steps s          ON s.operation_id  = o.id
LEFT JOIN outcome_input oi ON oi.operation_id = o.id
LEFT JOIN attr_union au    ON au.operation_id = o.id
GROUP BY o.id, o.first_seen, o.last_seen, o.operation_definition_id, o.display_value, o.outcome_rule,
         oi.outcome_values, au.attributes;
```

**`OutcomeRule.evaluate` (Java, ADR-13 §3).** The order of evaluation is fixed:

```
rule = outcome_rule (may be null)
1. if rule.outcomeKey != null && outcome_values != null:
       vals = outcome_values (sorted distinct strings of the latest member trace carrying the key)
       outcome_detail = String.join(",", vals), cut to 512 bytes
       outcome = any(v in rule.failureValues) ? FAILED          // a failure value wins over a success value
               : any(v in rule.successValues) ? SUCCESS
               : UNKNOWN
2. else if any_error:                              outcome = FAILED
3. else if rule.finalStepLabel != null:            outcome = final_step_reached ? SUCCESS : INCOMPLETE
4. else:                                           outcome = SUCCESS
OperationOutcome codes: 0 UNKNOWN, 1 SUCCESS, 2 FAILED, 3 INCOMPLETE
```

The outcome value is read from `trace_digest.attributes`. The FULL-tier `trace_attribute` rows of the earliest members may already be dropped when the operation closes. Therefore saving an `outcome_rule` with an `outcomeKey` makes the control plane flag that key `retain_in_digest = true` with `digest_transform = 'NONE'`, in addition to the ADR-13 rule that the key must be indexed. The same `OutcomeRule.evaluate` computes the on-the-fly outcome of FULL-tier operations in the API (§7.9).

```sql
-- BO3: write (same transaction as BO2), then account for the day
INSERT INTO tc.business_operation_digest
       (first_seen, last_seen, compacted_at, operation_id, op_day, operation_definition_id,
        trace_count, outcome, display_value, outcome_detail, steps, attributes)
SELECT u.first_seen, u.last_seen, now(), u.id, (u.first_seen AT TIME ZONE 'UTC')::date, u.type_id,
       u.trace_count, u.outcome, u.display_value, u.detail, COALESCE(u.steps, '[]'::jsonb), u.attributes
FROM unnest($ids::uuid[], $first::timestamptz[], $last::timestamptz[], $types::int[], $counts::int[],
            $outcomes::smallint[], $display::text[], $details::text[], $steps::jsonb[], $attrs::jsonb[])
     AS u(id, first_seen, last_seen, type_id, trace_count, outcome, display_value, detail, steps, attributes)
ON CONFLICT (operation_id, op_day) DO NOTHING;

UPDATE tc.compaction_state SET operations_digested = operations_digested + $inserted WHERE trace_day = $day;
-- after the last batch:
UPDATE tc.compaction_state SET status = 'DONE', finished_at = now() WHERE trace_day = $day;
```

- `trace_count` in the digest is the number of member traces actually found, which can differ from `business_operation.trace_count` if some member traces were excluded upstream or expired before compaction.
- Long-running operations: step labels come from `trace_correlation`, which is held for `max(merge_window) + 1 day` after the normal FULL-tier gate (§8.7). For operations spanning longer than that, the earliest steps can have `"stepLabel": null`. This is rare (ASSUMPTION) and documented.
- Partitions of `business_operation_digest` are created on demand for `op_day`. If `op_day` is already older than `now − (n + m)`, the row is skipped and counted.

**Metrics.** `tc_compaction_lag_days` (today − n − 1 − newest `DONE` day), `tc_compaction_traces_total{result=digested|excluded}`, `tc_compaction_operations_total`, `tc_compaction_chunk_seconds`, `tc_compaction_gate_blocked_partitions`. The workload at M is in §9.4.

---

## 7. Read path (tc-query-api, read replica)

All read SQL runs on the hot standby as role `tc_query`, with these settings:
- `default_transaction_read_only = on`;
- `statement_timeout = 15s` by default;
- `plan_cache_mode = force_custom_plan`. A generic plan over hierarchies of about 400 partitions must open and lock every partition before initial pruning, at least through PG 17; re-verify on 18. Custom plans prune at plan time.

Every query is time-bounded. For a FULL-tier query the API clamps `from` to `now() − retention + 1 h`, so it never touches a partition that maintenance is about to drop (§8.3). Older ranges are served by the DIGEST tier (§7.7–§7.9), which is clamped to `now() − (n + m) + 1 day`.

The plan shapes below are **illustrative expectations**, not captured output. The benchmark (§11) records the real `EXPLAIN (ANALYZE, BUFFERS)` for each.

### 7.1 Trace by id (UC-11)

**Without time hints.** Step 1 probes every retained summary partition:

```sql
SELECT trace_day, start_time, end_time, duration_ns, root_service_id, root_name_id, root_span_id,
       span_count, error_count, service_ids, fragment_count
FROM tc.trace_summary
WHERE trace_id = $1
  AND trace_day BETWEEN (now() AT TIME ZONE 'UTC')::date - ($full_days + 1) AND (now() AT TIME ZONE 'UTC')::date + 1;
```
```
Append  (≈9 subplans after pruning at n = 7; future pre-created partitions are empty)
  -> Index Scan using trace_summary_p20261121_pkey on trace_summary_p20261121
       Index Cond: (trace_id = $1)
  -> Index Scan using trace_summary_p20261120_pkey ...           (×8)
```
- Each probe is a descent of 3–4 levels. The upper levels are cached, so the cost is about 9 leaf reads in total: a few milliseconds warm and about 10 random reads cold.
- If no row is found, the API falls back to `trace_digest` (§7.7).
- Normally 0 or 1 row comes back. Two rows (two `trace_day`s) are possible when fragments arrived more than a day apart. The API merges them with `min(start_time)`, `max(end_time)` and a sum of counts.

Step 2 reads the spans only for the hours covered:

```sql
SELECT start_time, span_id, parent_span_id, duration_ns, service_id, name_id, name_overflow, kind,
       status_code, status_message, resource_id, scope_id, trace_flags, trace_state, flags,
       attributes, events, links, dropped_counts
FROM tc.span
WHERE trace_id = $1 AND start_time >= $2 AND start_time <= $3      -- $2/$3 = summary start/end
ORDER BY start_time, span_id
LIMIT 50001;                                                        -- max spans per trace + 1
```
```
Sort
  -> Append   (custom plan: only partitions overlapping [$2,$3], usually 1-2 of ~410)
       -> Index Scan using span_p2026112114_pkey on span_p2026112114
            Index Cond: ((trace_id = $1) AND (start_time >= $2) AND (start_time <= $3))
```
- The PK leads with `trace_id`, so the index scan reads exactly the trace's entries. `start_time`, the third key column, is checked inside the index.
- The heap fetches touch about 20 rows spread over about 20 pages. With 20 spans, a cold read costs about 25 random reads, a few milliseconds on NVMe.

**With hints** (`?start=&end=` from a search result): skip step 1, or restrict it to the hinted days, and use the hints in step 2.

**Steps 3–6** run in parallel on virtual threads, each a separate pooled connection:

```sql
-- resources / scopes / dictionary names (small, cached in the API as well)
SELECT id, service_id, schema_url, attributes FROM tc.resource WHERE id = ANY ($ids::bigint[]);
SELECT id, name, version, schema_url, attributes FROM tc.instrumentation_scope WHERE id = ANY ($ids::bigint[]);

-- enrichments: ≤ 11 daily probes on (trace_id) at n = 7
SELECT e.observed_at, e.span_id, e.source_id, e.source_revision, k.key, e.value
FROM tc.trace_enrichment e JOIN tc.attribute_key k ON k.id = e.key_id
WHERE e.trace_id = $1 AND e.observed_at >= now() - ($full_days + 1) * interval '1 day'
ORDER BY e.observed_at;

-- links in both directions (UC-20 data for the trace view)
SELECT 'OUT' AS dir, span_id, linked_trace_id, linked_span_id, attributes FROM tc.span_link
WHERE trace_id = $1 AND start_time >= $2::date AND start_time < $3::date + 1
UNION ALL
SELECT 'IN', span_id, trace_id, linked_span_id, attributes FROM tc.span_link
WHERE linked_trace_id = $1 AND start_time >= $2 - interval '1 day' AND start_time < now();

-- correlations and their operations
SELECT c.operation_definition_id, c.correlation_key_id, c.value, cv.operation_id
FROM tc.trace_correlation c
LEFT JOIN tc.correlation_value cv USING (operation_definition_id, value)
WHERE c.trace_id = $1 AND c.observed_at >= $2 - interval '1 day' AND c.observed_at < now();
```

The backward link lookup (`linked_trace_id = $1`) is bounded at "one day before the trace started" up to now. A linking span (a consumer, or a batch job) starts after the span it links to. Clock skew is absorbed by the one-day margin.

### 7.2 Trace search (UC-12)

**Mapping of request predicates:**

| Predicate | Source |
|---|---|
| `serviceName` | `trace_attribute` with key `service.name` (any span of the trace) |
| `spanName` | key `span.name` (root span) |
| `status = ERROR` | key `status`, value `ERROR` |
| `status = OK` or `UNSET` | Post-filter `error_count = 0` on the summary |
| `attributes[]` | Registered keys in `trace_attribute` |
| `minDurationMs`, `maxDurationMs` | `trace_summary.duration_ns` |

An attribute whose key is not registered gets HTTP 400, unless `scanMode = BRUTE_FORCE` (§7.4).

**Time semantics.** A trace matches if:
- its `start_time` lies in [`from`, `to`), **and**
- every predicate is satisfied by an attribute row with `ts` in [`from`, `to + slack`).

`slack` defaults to 1 h. It catches attributes on spans that start up to an hour after the trace root.

Because `trace_day` lies in [date(start), date(start) + 1] (§6.4), summary pruning uses `trace_day BETWEEN date(from) AND date(to) + 1`.

Results are ordered by `(start_time DESC, trace_id DESC)`. The cursor is `base64(start_time, trace_id, filterHash)`.

**Single selective predicate**, for example `user_id = 'u-42'` over 24 h:

```sql
WITH cand AS (
    SELECT DISTINCT trace_id
    FROM tc.trace_attribute
    WHERE key_id = $k AND value_str = $v
      AND ts >= $from AND ts < $to_plus_slack
)
SELECT s.trace_id, s.start_time, s.duration_ns, s.root_service_id, s.root_name_id,
       s.span_count, s.error_count, s.service_ids
FROM cand
JOIN tc.trace_summary s ON s.trace_id = cand.trace_id
WHERE s.trace_day BETWEEN $day_from AND $day_to_plus_1
  AND s.start_time >= $from AND s.start_time < $to
  AND (s.start_time, s.trace_id) < ($cursor_ts, $cursor_id)          -- omitted on page 1
  AND s.duration_ns >= coalesce($min_ns, 0)
ORDER BY s.start_time DESC, s.trace_id DESC
LIMIT $limit + 1;
```
```
Limit
  -> Sort (start_time DESC, trace_id DESC)
     -> Nested Loop
          -> HashAggregate (trace_id)
               -> Append (25 hourly partitions)
                    -> Index Only Scan using trace_attribute_p2026112114_key_id_value_str_ts_trace_id_idx
                         Index Cond: (key_id = $k AND value_str = $v AND ts >= .. AND ts < ..)
                         Heap Fetches: 0                       <- requires vacuumed visibility map (§8.5)
          -> Append (3 daily partitions)
               -> Index Scan using trace_summary_p20261121_pkey  Index Cond: (trace_id = cand.trace_id)
```

The `INCLUDE (trace_id)` column makes the candidate scan index-only. Insert-triggered vacuum keeps the visibility map set on closed hourly partitions, so `Heap Fetches` stays near 0. The `HashAggregate` removes duplicate attribute rows.

**Large candidate sets** (for example `client_id = bookstore-web`, millions per day). Materialising every candidate is wasteful, so the API switches to **time slicing** when the planner's row estimate for `cand` exceeds 20,000:
1. Scan the attribute index *backwards* in hourly slices, starting at `min(to, cursor_ts) + slack`.
2. After each slice with lower bound `L`, every trace with `start_time ≥ L` that has a matching row (within the slack) has been found. This holds because a trace's attribute rows have `ts ≥ start_time`.
3. Once at least `limit + 1` found traces have `start_time ≥ L`, they are exactly the top of the ordering. Stop there.

Pagination stays exact and stable, which Tempo's "first N found" search is not. In the worst case the API reads about one slice per page.

**Multiple predicates.** Order the predicates by estimated selectivity. The estimate comes from `pg_stats` on the `key_id`/`value_str` columns of the newest partition, or from the cheap planner estimate of `EXPLAIN (FORMAT JSON)`. Then:

- **Both selective** (< 20,000 rows each): use `INTERSECT`, which the planner runs as a hash setop.
  ```sql
  SELECT trace_id FROM tc.trace_attribute WHERE key_id=$k1 AND value_str=$v1 AND ts >= $from AND ts < $to_s
  INTERSECT
  SELECT trace_id FROM tc.trace_attribute WHERE key_id=$k2 AND value_str=$v2 AND ts >= $from AND ts < $to_s
  ```
- **One selective, the others not.** For example `user_id = u-42` and `auth.factor = SMS`. Verify each remaining predicate with a **time-bounded EXISTS**, using the candidate's own time window from the summary:
  ```sql
  WITH cand AS ( /* selective predicate + join trace_summary as above, without LIMIT */ )
  SELECT c.*
  FROM cand c
  WHERE EXISTS (
      SELECT 1 FROM tc.trace_attribute t2
      WHERE t2.key_id = $k2 AND t2.value_str = $v2
        AND t2.ts >= c.start_time - $scope_slack
        AND t2.ts <= c.end_time + $scope_slack
        AND t2.trace_id = c.trace_id)
  ORDER BY c.start_time DESC, c.trace_id DESC
  LIMIT $limit + 1;
  ```
  - `$scope_slack` is 0 for keys with `origin_scope` SPAN or RESOURCE, and 1 day for ENRICHMENT or ANY. Enrichment `ts` is the Kafka timestamp, which can lie outside the trace.
  - The inner scan is a range on `(key_id, value_str, ts)` covering only a few seconds. Even for a very common value it reads a few hundred index tuples and checks the included `trace_id`.
  - Run-time partition pruning (`Subplans Removed`) restricts each probe to the 1–2 hourly partitions of the candidate.
- **No selective predicate at all** (only common values over 7 days): use the slicing strategy on the most selective predicate, verifying the others with EXISTS per slice.

**Operators:**

| Operator | SQL on `trace_attribute` |
|---|---|
| `EQ` | `value_str = $v`. A value longer than 512 bytes is matched as prefix (`value_str = left($v, 512)` on a UTF-8 boundary) and verified against `span.attributes` for the candidates. |
| `IN` | `value_str = ANY ($vs::text[])`. The B-tree performs a multi-value scan (SAOP). |
| `PREFIX` | `value_str LIKE $p \|\| '%'` (with `$p` escaped). Works with the B-tree thanks to `COLLATE "C"`. |
| `GT/GTE/LT/LTE` | Keys flagged `numeric` only: `key_id = $k AND value_num > $x AND ts …`, which uses the partial index `trace_attribute_num_ts_idx`. |
| `NEQ` | Never a driving predicate, because it is non-selective. It is applied as `NOT EXISTS` with the same time-bounded probe. A request with only NEQ predicates gets 400. |

**Duration-only and service-only searches.** These run straight against `trace_summary`:

```sql
SELECT ... FROM tc.trace_summary
WHERE trace_day BETWEEN $d1 AND $d2 AND start_time >= $from AND start_time < $to
  AND duration_ns >= $min_ns
  AND (start_time, trace_id) < ($cts, $cid)
ORDER BY start_time DESC, trace_id DESC LIMIT $limit + 1;
```

The planner chooses between two plans:
- an index scan on `trace_summary_start_idx` with an incremental sort on `trace_id` and a filter on duration, which is good when long traces are common;
- a skip scan (PG 18) on `trace_summary_root_svc_dur_idx`, iterating over the distinct `root_service_id` values with `duration_ns >= $min_ns`, followed by a sort. This is good for rare long traces.

When `serviceName` means *root* service, `trace_summary_root_svc_start_idx` serves it directly.

### 7.3 Autocomplete (UC-15)

```sql
-- services, span names (dictionaries, tiny)
SELECT name FROM tc.service WHERE name LIKE $p || '%' ORDER BY name LIMIT 50;
SELECT sn.name FROM tc.span_name sn JOIN tc.service s ON s.id = sn.service_id
WHERE s.name = $svc AND sn.name LIKE $p || '%' ORDER BY sn.name LIMIT 50;

-- searchable keys
SELECT k.key, k.value_type, ia.numeric, ia.origin_scope
FROM tc.indexed_attribute ia JOIN tc.attribute_key k ON k.id = ia.key_id
WHERE ia.status = 'ACTIVE' ORDER BY k.key;

-- distinct values by prefix: loose index scan (emulated skip scan), default window = last 1 h
WITH RECURSIVE v(val) AS (
    (SELECT value_str FROM tc.trace_attribute
     WHERE key_id = $k AND value_str >= $p AND value_str LIKE $p || '%' AND ts >= $from AND ts < $to
     ORDER BY value_str LIMIT 1)
    UNION ALL
    SELECT (SELECT value_str FROM tc.trace_attribute
            WHERE key_id = $k AND value_str > v.val AND value_str LIKE $p || '%' AND ts >= $from AND ts < $to
            ORDER BY value_str LIMIT 1)
    FROM v WHERE v.val IS NOT NULL
)
SELECT val FROM v WHERE val IS NOT NULL LIMIT $limit;
```

- Each recursion step is a `Merge Append` of index scans over the pruned hourly partitions, returning the next distinct value.
- Cost is proportional to `limit × partitions`, not to the row count.
- Values of PII keys are masked or omitted for callers without the `TC_PII` scope (UC-19).

### 7.4 Bounded brute-force search on non-indexed attributes (UC-13)

The request requires `scanMode = BRUTE_FORCE` and a window of 1 h or less, which the API enforces. It runs in its own transaction:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL max_parallel_workers_per_gather = 8;
SELECT DISTINCT s.trace_id
FROM tc.span s
WHERE s.start_time >= $from AND s.start_time < $to          -- prunes to 1-2 hourly partitions
  AND ( s.attributes -> $key = to_jsonb($value::text)        -- scalar match
        OR (jsonb_typeof(s.attributes -> $key) = 'array'
            AND s.attributes -> $key @> jsonb_build_array($value::text)) )
LIMIT $limit * 3;
COMMIT;
```
```
Limit -> Unique -> Gather Merge (8 workers) -> Sort -> Parallel Seq Scan on span_p2026112114
                                                        Filter: ((attributes -> 'k') = '"v"' OR ...)
```

Variants of the predicate:
- Numeric and boolean values use `to_jsonb($value::bigint)`, `to_jsonb($value::numeric)` or `'true'::jsonb`, according to the type requested.
- A resource-level key is resolved first against the small `resource` table: `SELECT id FROM tc.resource WHERE attributes -> $key = …`. The span scan then filters `resource_id = ANY ($ids)`, which is still a sequential scan but needs no jsonb evaluation.
- Enrichment keys use `trace_enrichment` (`key_id = $k AND value = $v::jsonb`) over the window.

> **Note on the cost at profile M.** One hourly `span` partition averages 72 M rows, about 35 GB of heap.
> - A parallel scan with 8 workers runs at roughly 2–4 GB/s from page cache or NVMe. jsonb key extraction costs about 0.5–1 µs per row.
> - A full hour therefore takes **about 10–30 s**.
> - The API keeps 1 h as the hard maximum (brief §6) with `statement_timeout = 30s` for this mode. **At profile M the UI defaults to a 15-minute window, and at L brute force is limited to 5 minutes.**
> - On timeout the API returns `503` + `Retry-After` with problem type `/problems/query-timeout` (03 §4.3) and a suggestion to narrow the window or register the key (UC-07).
> - Recommend registering keys that users search for regularly.

### 7.5 Business-operation timeline (UC-14)

```sql
-- 1. resolve the survivor (follow merged_into; path compression keeps chains at length ≤ 1)
SELECT id, first_seen, last_seen, operation_definition_id, display_value, trace_count, merged_into
FROM tc.business_operation
WHERE id = $op
  AND first_seen >= uuid_extract_timestamp($op) - interval '120 days'   -- coarse pruning:
  AND first_seen <= uuid_extract_timestamp($op);                        -- first_seen ≤ creation time
-- if merged_into IS NOT NULL, repeat with merged_into

-- 2. ordered timeline
WITH vals AS (
    SELECT operation_definition_id, value FROM tc.correlation_value WHERE operation_id = $survivor
),
hits AS (
    SELECT DISTINCT ON (c.trace_id) c.trace_id, c.correlation_key_id, c.observed_at
    FROM vals v
    JOIN tc.trace_correlation c
      ON c.operation_definition_id = v.operation_definition_id AND c.value = v.value
    WHERE c.observed_at >= $first_seen - $merge_window AND c.observed_at <= $last_seen + $merge_window
    ORDER BY c.trace_id, c.observed_at
)
SELECT s.trace_id, s.start_time, s.duration_ns, s.root_service_id, s.root_name_id,
       s.span_count, s.error_count, ck.step_label
FROM hits h
JOIN tc.correlation_key ck ON ck.id = h.correlation_key_id
JOIN tc.trace_summary s
  ON s.trace_id = h.trace_id
 AND s.trace_day BETWEEN ($first_seen - $merge_window)::date AND ($last_seen + $merge_window)::date + 1
ORDER BY s.start_time;
```

- `uuid_extract_timestamp()` reads the millisecond timestamp embedded in a UUIDv7. The resolver creates the id when `first_seen` is already known, so `first_seen ≤ ts(id)` holds, and the probe touches at most 4–5 monthly partitions.
- The lookup **by type and value** (`GET /business-operations?type=&value=`) is `correlation_value` primary-key lookup, then the timeline query above.
- Listing operations without a value uses `business_operation_type_first_seen_idx` with `first_seen` between `from` and `to`.

### 7.6 Related traces (UC-20)

```sql
-- linked traces in both directions (bounded as in §7.1) ...
SELECT linked_trace_id AS related, 'LINK_OUT' AS via FROM tc.span_link
WHERE trace_id = $1 AND start_time >= $d1 AND start_time < $d2 + 1
UNION
SELECT trace_id, 'LINK_IN' FROM tc.span_link
WHERE linked_trace_id = $1 AND start_time >= $start - interval '1 day' AND start_time < now()
UNION
-- ... and traces in the same business operation(s)
SELECT c2.trace_id, 'OPERATION:' || cv.operation_id
FROM tc.trace_correlation c1
JOIN tc.correlation_value cv USING (operation_definition_id, value)
JOIN tc.correlation_value cv2 ON cv2.operation_id = cv.operation_id
JOIN tc.trace_correlation c2
  ON c2.operation_definition_id = cv2.operation_definition_id AND c2.value = cv2.value
 AND c2.observed_at >= $start - interval '1 day' - $merge_window
 AND c2.observed_at <  $end + interval '1 day' + $merge_window
WHERE c1.trace_id = $1 AND c1.observed_at >= $start - interval '1 day' AND c1.observed_at < now()
  AND c2.trace_id <> $1
LIMIT 200;
```

The result is joined to `trace_summary`, as in search, for display.

### 7.7 Trace by id in the DIGEST tier (UC-11, UC-22)

If step 1 of §7.1 finds no `trace_summary` row, the API probes `trace_digest`:

```sql
SELECT trace_day, start_time, duration_ns, root_service_id, root_name_id, span_count, error_count,
       outcome, service_ids, business_operation_id, attributes, compacted_at
FROM tc.trace_digest
WHERE trace_id = $1
  AND trace_day BETWEEN (now() AT TIME ZONE 'UTC')::date - ($full_days + $digest_days)
                    AND (now() AT TIME ZONE 'UTC')::date - $full_days + 1;
```
- The probe covers up to about 92 daily partitions that hold data; partitions of the last n days are still empty. Each probe is one PK descent, so this is about 90 leaf reads cold, 10–30 ms on NVMe. With `?start=` hints from a search result it touches 1–2 partitions.
- The response is `{"tier":"DIGEST","spansExpired":true,"digest":{…}}`. It has no spans, enrichments or links. Dictionary ids are resolved as in §7.1, and `business_operation_id` links to §7.9.
- A row in both tiers is possible only briefly, between compaction and the FULL-tier drop. The API then prefers the FULL tier and reports `"tier":"FULL"`.
- Values of PII keys are masked for callers without `TC_PII` (UC-19). Values with `digest_transform` SHA256 or MASK are shown as stored.

### 7.8 Search across tiers (UC-12, UC-22)

**Window and split.**
- The maximum window is 7 days when it touches the FULL tier and 31 days when it lies entirely in the DIGEST tier. `from` is clamped to `now − (n + m) + 1 day`.
- The nominal tier boundary is `now − n days`. The **exact split is by `trace_day`** against the compaction watermark W, the newest day with `status = 'DONE'`. Days are compacted oldest first, so every day ≤ W is DONE. W is cached for 1 minute.
  - The FULL part is §7.2 with the extra predicate `s.trace_day > W`. The drop gate keeps every such day in the FULL tier.
  - The DIGEST part is `trace_digest` with `trace_day ≤ W`.
- The two parts are therefore disjoint and leave no gap, even for traces whose `trace_day` is the day after their start.

**Predicate mapping in the DIGEST part:**

| Request predicate | DIGEST-tier SQL |
|---|---|
| `attributes[]` EQ on a retained key | Merged into one containment document: `d.attributes @> $doc`, for example `{"user_id":["9f86d081…"],"auth.factor":["SMS"]}`. GIN `jsonb_path_ops`; several keys in one document mean AND. |
| `attributes[]` IN | `(d.attributes @> $doc1 OR d.attributes @> $doc2 …)`, which becomes a BitmapOr of GIN scans. |
| `attributes[]` NEQ | `NOT (d.attributes @> $doc)`. Applied as a filter only, never as the driving predicate (as in §7.2). |
| `serviceName` | `d.root_service_id = $svc OR d.service_ids @> ARRAY[$svc]` |
| `spanName` (root) | `d.root_name_id = ANY ($name_ids)` |
| `status` | `d.outcome = 2` (ERROR) or `d.outcome = 1` |
| `minDurationMs`, `maxDurationMs` | `d.duration_ns` |
| Key not retained, key with transform MASK, or operator PREFIX / GT / GTE / LT / LTE | The DIGEST part is **skipped**, and the response gets `"warnings":[{"code":"DIGEST_TIER_UNSUPPORTED_PREDICATE","key":"…"}]`. |
| `scanMode = BRUTE_FORCE` | FULL tier only. |

**Hashed keys.** For a retained key with `digest_transform = 'SHA256'`, the API computes `hex(sha256(utf8(value)))` in Java, the same function as DC1, before it builds `$doc`. The value is cut to 512 bytes on a UTF-8 boundary first, exactly as `trace_attribute.value_str` was. MASK is lossy, so equality on a MASK key is not offered.

```sql
-- DIGEST part (replica, role tc_query); $doc built by the API, jsonb
SELECT d.trace_id, d.start_time, d.duration_ns, d.root_service_id, d.root_name_id, d.span_count,
       d.error_count, d.outcome, d.service_ids, d.business_operation_id, 'DIGEST' AS tier
FROM tc.trace_digest d
WHERE d.trace_day BETWEEN $day_from AND LEAST($day_to_plus_1, $watermark_day)
  AND d.start_time >= $from AND d.start_time < $to
  AND d.attributes @> $doc                                                 -- omitted if no attribute predicate
  AND ($svc::int IS NULL OR d.root_service_id = $svc OR d.service_ids @> ARRAY[$svc::int])
  AND d.duration_ns >= coalesce($min_ns, 0)
  AND ($status::smallint IS NULL OR d.outcome = $status)
  AND (d.start_time, d.trace_id) < ($cursor_ts, $cursor_id)                -- omitted on page 1
ORDER BY d.start_time DESC, d.trace_id DESC
LIMIT $limit + 1;
```
```
Limit
  -> Sort (start_time DESC, trace_id DESC)
     -> Append (≤ 31 daily partitions)
          -> Bitmap Heap Scan on trace_digest_p20261020
               Recheck Cond: (attributes @> $doc)
               Filter: (start_time >= .. AND start_time < ..)
               -> Bitmap Index Scan on trace_digest_p20261020_attributes_idx
                    Index Cond: (attributes @> $doc)
```
For non-selective documents such as `client_id = bookstore-web`, the planner instead chooses a `Merge Append` of `trace_digest_start_idx` index scans with `attributes @> $doc` as a filter. That plan stops after `limit + 1` rows, the same idea as time slicing in §7.2. The plan shape is illustrative and is measured in the benchmark (§11).

**Merged keyset pagination.**
1. Both parts receive the same cursor predicate `(start_time, trace_id) < ($cursor_ts, $cursor_id)` and the same `LIMIT $limit + 1`.
2. The API merge-sorts the two result lists by `(start_time DESC, trace_id DESC)` and returns the first `limit` rows. Each item carries `"tier"`.
3. The next cursor is the last returned `(start_time, trace_id)`. Both tiers share the ordering key, so pagination is exact and stable across the boundary.
4. Short cut: if the FULL part already returned `limit + 1` rows that all start at or after `W + 1 day`, the DIGEST part cannot contribute and is not executed. This is the common case for "newest first" pages.

### 7.9 Business operations across tiers (UC-14, UC-22)

**`GET /business-operations/{id}`.**
1. Resolve the id in the FULL tier with `tc.operation_root($id)` and §7.5. If found, return the live timeline. Its `outcome` is computed on the fly by the same `OutcomeRule.evaluate` as compaction (§6.8), from the timeline's traces and the FULL-tier attribute values of `outcomeKey`.
2. Otherwise probe the digest:
   ```sql
   SELECT operation_id, op_day, first_seen, last_seen, operation_definition_id, display_value, trace_count,
          outcome, outcome_detail, steps, attributes
   FROM tc.business_operation_digest
   WHERE operation_id = $id
     AND op_day BETWEEN (now() AT TIME ZONE 'UTC')::date - ($full_days + $digest_days)
                    AND (uuid_extract_timestamp($id) AT TIME ZONE 'UTC')::date;   -- first_seen ≤ ts(id)
   ```
   The timeline is rendered from `steps`. Each `traceId` opens the digest view of §7.7.
3. Ids of merged losers are not digested. Once the FULL tier has expired they return 404. UI links always use the survivor id.

**`GET /business-operations?type=&value=&outcome=&from=&to=`.** This searches both tiers, split by `first_seen` at the boundary of the operation tier: operations not yet compacted live in `business_operation`, compacted ones in `business_operation_digest`. The digest is looked up first by `operation_id` to drop duplicates during the short overlap.

```sql
-- FULL part: by value (correlation_value PK), else by type and time (business_operation_type_first_seen_idx)
SELECT tc.operation_root(cv.operation_id) AS id FROM tc.correlation_value cv
WHERE cv.operation_definition_id = $type AND cv.value = $value;

-- DIGEST part, by value: display_value, or any retained correlation-key value via GIN
SELECT operation_id, first_seen, last_seen, display_value, trace_count, outcome, outcome_detail
FROM tc.business_operation_digest
WHERE operation_definition_id = $type
  AND op_day BETWEEN $from::date AND $to::date
  AND (display_value = $value OR attributes @> $value_doc)    -- $value_doc = {"<corr key>":["<value>"]} per retained key
  AND ($outcome::smallint IS NULL OR outcome = $outcome)
ORDER BY first_seen DESC, operation_id DESC
LIMIT $limit + 1;

-- DIGEST part, without value: (operation_definition_id, outcome, first_seen DESC) or (operation_definition_id, first_seen DESC)
SELECT operation_id, first_seen, last_seen, display_value, trace_count, outcome, outcome_detail
FROM tc.business_operation_digest
WHERE operation_definition_id = $type AND outcome = $outcome
  AND first_seen >= $from AND first_seen < $to AND op_day BETWEEN $from::date AND $to::date
  AND (first_seen, operation_id) < ($cursor_ts, $cursor_id)
ORDER BY first_seen DESC, operation_id DESC
LIMIT $limit + 1;
```

- **Outcome filter on the FULL part.** FULL-tier operations have no stored outcome. The filter is applied after `OutcomeRule.evaluate` has computed each candidate's outcome. The FULL part covers at most the last n + 1 days and one type, and evaluation is bounded at `limit × 5` candidates per page. When that bound is hit, the response reports `"partial": true`.
- **Pagination.** The two tiers are merged by `(first_seen DESC, operation_id DESC)` with the same keyset rule as §7.8. Each item carries `"tier"`.

---
## 8. Partition management (UC-16)

### 8.1 Naming convention and bounds (all UTC)

| Granularity | Name | Bounds example |
|---|---|---|
| HOUR | `<table>_p<YYYYMMDDHH>`, e.g. `span_p2026112114` | `FROM ('2026-11-21 14:00:00+00') TO ('2026-11-21 15:00:00+00')` |
| DAY (timestamptz key) | `<table>_p<YYYYMMDD>`, e.g. `trace_correlation_p20261121` | `FROM ('2026-11-21 00:00:00+00') TO ('2026-11-22 00:00:00+00')` |
| DAY (date key: `trace_summary`) | `trace_summary_p20261121` | `FROM ('2026-11-21') TO ('2026-11-22')` |
| MONTH | `<table>_p<YYYYMM>`, e.g. `business_operation_p202611` | `FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00')` |

PartitionManager derives the bounds from the name. It cross-checks them against `pg_get_expr(c.relpartbound, c.oid)` for children found via `pg_inherits`. A partition that does not follow the convention raises an alert and is never dropped automatically.

### 8.2 Pre-creation with CREATE + ATTACH (low-lock)

Every 10 minutes, and once on start-up, PartitionManager runs under the ShedLock lock `partition-manager`. It makes sure that every partition overlapping `[now − 1 granularity, now + retention_policy.precreate]` exists.

It does **not** use `CREATE TABLE … PARTITION OF`, because that statement takes an ACCESS EXCLUSIVE lock on the parent. That lock causes two problems:
- It queues behind in-flight ingest transactions on the primary and then blocks them.
- The lock is WAL-logged and replayed on the hot standby. There it conflicts with *every* running query on `tc.span`, so each hourly creation would cancel or delay replica queries.

Instead, PartitionManager creates a standalone table and attaches it. `ATTACH PARTITION` needs only a SHARE UPDATE EXCLUSIVE lock on the parent.

```sql
SET lock_timeout = '5s';                  -- retry with jitter on timeout; never queue behind long locks
SET ROLE tc_owner;

CREATE TABLE IF NOT EXISTS tc.span_p2026112114
    (LIKE tc.span INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION)
    WITH (fillfactor = 100,
          autovacuum_vacuum_insert_threshold    = 2000000,
          autovacuum_vacuum_insert_scale_factor = 0,
          autovacuum_analyze_threshold          = 2000000,
          autovacuum_analyze_scale_factor       = 0,
          autovacuum_freeze_min_age             = 0
          /* , toast_tuple_target = 128  -- only behind the inline-compression flag (§4.3) */);
ALTER TABLE tc.span ATTACH PARTITION tc.span_p2026112114
    FOR VALUES FROM ('2026-11-21 14:00:00+00') TO ('2026-11-21 15:00:00+00');
-- the matching PK index is created on the (empty) partition during ATTACH

-- daily and monthly examples
CREATE TABLE IF NOT EXISTS tc.trace_summary_p20261124
    (LIKE tc.trace_summary INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION)
    WITH (fillfactor = 80, autovacuum_vacuum_threshold = 200000, autovacuum_vacuum_scale_factor = 0.02,
          autovacuum_vacuum_insert_threshold = 1000000, autovacuum_vacuum_insert_scale_factor = 0,
          autovacuum_freeze_min_age = 0);
ALTER TABLE tc.trace_summary ATTACH PARTITION tc.trace_summary_p20261124
    FOR VALUES FROM ('2026-11-24') TO ('2026-11-25');

CREATE TABLE IF NOT EXISTS tc.business_operation_p202612
    (LIKE tc.business_operation INCLUDING DEFAULTS INCLUDING CONSTRAINTS) WITH (fillfactor = 85);
ALTER TABLE tc.business_operation ATTACH PARTITION tc.business_operation_p202612
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
```

Storage parameters per table:

| Table | fillfactor | Autovacuum / freeze | Rationale |
|---|---|---|---|
| `span`, `trace_attribute` (HOUR) | 100 | insert threshold 2 M, scale 0, analyze 2 M, `autovacuum_freeze_min_age = 0` | Append-only tables. Frequent vacuums while the pages are still hot set visibility-map bits (enabling index-only scans) and freeze rows early. |
| `span_link`, `trace_enrichment`, `trace_correlation` (DAY) | 100 | insert threshold 500 k, scale 0, freeze_min_age 0 | Append-only and small. |
| `trace_summary` (DAY) | 80 | vacuum threshold 200 k, scale 0.02, insert threshold 1 M, freeze_min_age 0 | About 3 updates per trace create dead tuples. Most of these updates are non-HOT because they change indexed columns. Spare space on the page keeps new row versions local. |
| `business_operation` (MONTH) | 85 | defaults, scale 0.05 | Updates of `last_seen`, `trace_count` and `merged_into` are HOT, since none of these columns is indexed. |
| `trace_digest`, `business_operation_digest` (DAY, DIGEST tier, created on demand) | 100 | insert threshold 1 M, scale 0, freeze_min_age 0; GIN `gin_pending_list_limit = 16MB` | Write-once by DigestCompactor, then frozen by an explicit `VACUUM (FREEZE, ANALYZE)` (§8.7). |

`trace_enrichment` and `trace_correlation` partitions are created without `INCLUDING IDENTITY`. Rows are always inserted through the parent, which generates the identity values.

### 8.3 Retention with DETACH CONCURRENTLY + DROP

A partition is expired once its upper bound is at or before `now() − retention`. Under ADR-13, FULL-tier partitions must additionally pass the compaction drop gate (§8.7).

```sql
-- autocommit, direct connection (DETACH … CONCURRENTLY cannot run inside a transaction block)
SET lock_timeout = '5s';
ALTER TABLE tc.span DETACH PARTITION tc.span_p2026110713 CONCURRENTLY;
DROP TABLE tc.span_p2026110713;
-- if a DETACH was interrupted (the pod was killed between its two internal transactions):
ALTER TABLE tc.span DETACH PARTITION tc.span_p2026110713 FINALIZE;
```

How this interacts with locks and long-running queries:

- **On the parent.** `DETACH … CONCURRENTLY` takes only SHARE UPDATE EXCLUSIVE on the parent, so inserts and selects continue. In its second transaction it waits until every transaction whose snapshot could still see the partition has finished. On the primary these are only short ingest and enrichment transactions. The concurrent form is possible only because no DEFAULT partition exists.
- **On the standby.** `DROP TABLE` takes ACCESS EXCLUSIVE on the *detached* table only. When that lock replays on the standby, it conflicts only with replica queries that are still reading the expired partition. In practice there are none, because the API clamps `from` to `now − retention + 1 h`. Any such query is cancelled after `max_standby_streaming_delay` (§12.3).
- **Timing.** Drops run at a random minute in the hour, never at :00 when ingest rolls over to a new partition.
- **Audit.** Every drop is logged together with the row estimate and counted in the `tc_partition_dropped_total` metric.

**Changing granularity.** Suppose `span` moves from HOUR to DAY for profile S. The new granularity takes effect at the first boundary after the last existing partition, because ranges never overlap. The existing partitions simply age out.

### 8.4 Cleanup of non-partitioned tables

| Table | Job | SQL |
|---|---|---|
| `correlation_value` | Daily, in batches of 10 000 | ADR-13: a row is deleted once its operation is compacted **and** it is older than the `trace_correlation` hold (§8.7): `DELETE FROM tc.correlation_value WHERE ctid = ANY (ARRAY(SELECT cv.ctid FROM tc.correlation_value cv WHERE cv.updated_at < now() - $full_days - $max_merge_window - interval '1 day' AND EXISTS (SELECT 1 FROM tc.business_operation_digest d WHERE d.operation_id = cv.operation_id AND d.op_day = (cv.operation_first_seen AT TIME ZONE 'UTC')::date) LIMIT 10000))`. With `digestDays = 0`: `operation_first_seen < now() - $bo_retention` as before. |
| `compaction_state` | Daily | `DELETE FROM tc.compaction_state WHERE trace_day < (now() AT TIME ZONE 'UTC')::date - ($full_days + $digest_days)` |
| `resource` (DictionaryGc) | Daily | `DELETE FROM tc.resource WHERE last_seen < now() - ($span_retention + interval '1 day')` |

The resource grace period is span retention plus one day. That is longer than the 3-day Kafka retention, so a replay can never reference a resource that has already been deleted. Spans that are already too old are dropped before they are inserted.

`service`, `span_name` and `attribute_key` rows are never deleted.

### 8.5 Vacuum, analyze and freezing

**Append-only partitions** (`span`, `trace_attribute`, `span_link`, `trace_enrichment`, `trace_correlation`):
- Insert-triggered autovacuum visits the current hourly partition every few minutes. At profile M, 2 M rows is about 100 s of span inserts.
- Each run scans only the pages that are not yet all-visible. With no dead tuples, index vacuuming is skipped.
- The run sets visibility-map bits. This is what makes the `trace_attribute` candidate scans index-only (`Heap Fetches: 0`).
- With `autovacuum_freeze_min_age = 0` the same run also **freezes** those pages. Most of them are still dirty within the current checkpoint cycle, so freezing them costs only small freeze WAL records, not an additional full-page image.

**Why freeze early.**
- XID consumption at M is about 150 transactions/s: 24 ingest threads each committing roughly every 250 ms, plus enrichment, the resolver and maintenance. That is about 13 M XIDs per day.
- With the default `autovacuum_freeze_max_age` of 200 M, unfrozen tables would hit forced anti-wraparound vacuums after about 15 days.
- At that point the roughly 200 partitions of the 97-day DIGEST tables (ADR-13), and the monthly `business_operation` partitions, would all come due within a short window. Each forced vacuum would read and write whole cold partitions, which is the classic anti-wraparound storm.
- With early freezing, closed partitions are already all-frozen, so anti-wraparound vacuums are trivial visibility-map scans.
- As a safety net, PartitionManager runs `VACUUM (FREEZE, ANALYZE)` on each hourly partition at H+2 if `relallvisible / relpages < 0.99`, and on `trace_summary` day partitions at D+2.

**Mutable tables.**
- `trace_summary` uses regular autovacuum on the current day, triggered at 2 % dead tuples.
- Index bloat from non-HOT updates is moderated by B-tree bottom-up deletion (PostgreSQL 14+). The capacity model budgets 1.5× for these indexes (§9.2).
- Once a partition is static (D+2), an optional `REINDEX (CONCURRENTLY)` can run if `pgstatindex` reports `avg_leaf_density` below 60 %.

**Statistics.**
- Autovacuum analyses leaf partitions but **never partitioned parents**.
- StatsRefresher runs `ANALYZE` on every newly closed partition.
- Every 6 h it also runs `ANALYZE ONLY` on the parents. This PG 18 form computes the parent's sampled statistics without re-analysing every child; verify the exact semantics on 18.7. Parent statistics matter for multi-partition estimates.
- Run `ALTER TABLE tc.trace_attribute ALTER COLUMN value_str SET STATISTICS 1000`, which propagates to the partitions. The search planner's selectivity choice (§7.2) then sees the frequent values.

**Autovacuum capacity.**
- `autovacuum_max_workers = 6` and `autovacuum_worker_slots = 16`. In PG 18 the worker count can be raised on reload, up to the number of slots.
- `autovacuum_vacuum_cost_limit = 2000`.
- Monitor `pg_stat_progress_vacuum`.

### 8.6 pg_partman 5.5.1 as an alternative

pg_partman is declarative-only since 5.0.1; the latest release is 5.5.1 (2026-11-17). It could replace PartitionManager:

```sql
CREATE SCHEMA partman; CREATE EXTENSION pg_partman SCHEMA partman;
SELECT partman.create_parent(p_parent_table => 'tc.span', p_control => 'start_time',
                             p_interval => '1 hour', p_premake => 72,
                             p_default_table => false);          -- verify parameter names in 5.5.1
UPDATE partman.part_config
SET retention = '7 days', retention_keep_table = false, infinite_time_partitions = true  -- FULL tier n; NOTE: pg_partman has no compaction drop gate, so it may only be used with digestDays = 0 or with retention disabled and drops left to PartitionManager
WHERE parent_table = 'tc.span';
-- background maintenance: shared_preload_libraries = 'pg_partman_bgw', pg_partman_bgw.interval = 600
```

**Pros.** It is mature and familiar to DBAs, and its background worker needs no application leader.

**Cons.**
- A DEFAULT partition is created unless it is explicitly disabled.
- It uses its own partition-suffix format.
- Storage parameters must come from its template table. Verify which reloptions 5.5.1 copies from the template.
- Verify whether its retention uses `DETACH … CONCURRENTLY`. A plain DETACH takes ACCESS EXCLUSIVE on the parent.
- Not every managed PostgreSQL offering provides the extension.

**Decision.** TCS ships its own PartitionManager. It provides per-step metrics, `lock_timeout` with retry, CREATE+ATTACH, and `retention_policy` editable via `tc-control-plane`. pg_partman remains the documented fallback; with it, `retention_policy` is synced into `part_config`. pg_partman's age-based retention cannot express the compaction drop gate (§8.7), which is a further reason for the own PartitionManager under ADR-13.

### 8.7 Two-tier retention: drop gate, DIGEST drops, lag alerting (ADR-13)

**Retention per tier.** PartitionManager reads `retention_policy.tier`:

| Tier | Tables | Drop condition for a partition with bounds [lo, hi) |
|---|---|---|
| FULL | `span`, `trace_attribute`, `span_link`, `trace_summary`, `trace_enrichment` | `hi ≤ now − n days` **and** the drop gate holds |
| FULL | `trace_correlation` | as above, **and** `hi ≤ now − n days − max(business_operation_definition.merge_window) − 1 day`. This keeps step labels for operations compacted a day later (§6.8). |
| FULL | `business_operation` (MONTH) | `hi ≤ now − n days` **and** sweep BO0 (§6.8) finds no non-merged operation in the partition without a `business_operation_digest` row |
| DIGEST | `trace_digest`, `business_operation_digest` | `hi ≤ now − (n + m) days`. No gate is needed. |

**The drop gate (never drop uncompacted data).** For every FULL-tier partition [lo, hi) except `business_operation`, every trace day D whose attribute window `[D − 1 h, D + 1 day + 6 h)` overlaps [lo, hi) must have `compaction_state.status = 'DONE'`:

```sql
-- true = the partition may be dropped; $lo/$hi from the partition bounds (§8.1)
SELECT NOT EXISTS (
    SELECT 1
    FROM generate_series(($lo - interval '1 day 6 hours')::date, ($hi + interval '1 hour')::date, interval '1 day') AS g(d)
    LEFT JOIN tc.compaction_state cs ON cs.trace_day = g.d::date
    WHERE g.d::date + interval '1 day 6 hours' > $lo          -- window of D overlaps [lo, hi)
      AND g.d::date - interval '1 hour'        < $hi
      AND cs.status IS DISTINCT FROM 'DONE');
```
- For `trace_summary`, `[lo, hi)` is the day itself, so the gate is "day D is DONE".
- If the gate blocks a partition, it is kept. That costs disk, never data. `tc_compaction_gate_blocked_partitions` counts such partitions.
- With `digestDays = 0` the gate and DigestCompactor are disabled, and the FULL tier is dropped by age alone.
- Late or replayed data (UC-18) older than n days is rejected by the writers before insert, as before (§6.6). Compacted days are therefore never changed afterwards.

**DIGEST partitions.**
- They are created **on demand** by DigestCompactor for day D (`trace_digest`) and for `op_day` (`business_operation_digest`), with the same CREATE + ATTACH routine. `precreate` is ignored for tier DIGEST.
- Storage parameters: `fillfactor = 100`, `autovacuum_vacuum_insert_threshold = 1000000`, `autovacuum_vacuum_insert_scale_factor = 0`, `autovacuum_freeze_min_age = 0`. GIN indexes keep `fastupdate = on` with `gin_pending_list_limit = 16MB`, and the `VACUUM (FREEZE, ANALYZE)` at the end of each day flushes the pending list.
- `business_operation_digest` partitions receive rows for a few days after their `op_day` + n. PartitionManager runs `VACUUM (FREEZE, ANALYZE)` on them at `op_day + n + 3 days`.
- They are dropped at `n + m` days with `DETACH … CONCURRENTLY` + `DROP`, exactly as in §8.3. With the defaults, day D's digest lives from about D + 8 until D + 97.

**Compaction lag alerting.**
- `tc_compaction_lag_days` = `(today − n − 1) − newest DONE trace_day`. It is 0 in steady state.
- **Warning at > 1 day, critical at > 2 days.** Every day of lag keeps one extra day of FULL-tier data, about 1.2 TB at M (§9.3). The 25 % headroom absorbs about 3 days.
- `compaction_state.status = 'FAILED'` for more than 30 minutes raises an alert with `error`.
- Runbook:
  1. Check `compaction_state.error` and the `tc_compaction_chunk_seconds` p99.
  2. If disk pressure becomes critical, an operator can explicitly force-drop the oldest FULL-tier day with `tc-maintenance drop-uncompacted --day=YYYY-MM-DD`. This loses that day's digest, and the decision is logged.

---

## 9. Capacity planning

### 9.1 Inputs (brief §7)

| Symbol | Meaning | S | M (baseline) | L |
|---|---|---|---|---|
| T | traces/s, average | 100 | 1 000 | 5 000 |
| N | spans per trace | 20 | 20 | 20 |
| p | peak factor | 3 | 3 | 3 |
| R = T·N | spans/s, avg / peak | 2 000 / 6 000 | 20 000 / 60 000 | 100 000 / 300 000 |
| D = T·86 400 | traces/day | 8.64 M | 86.4 M | 432 M |
| D·N | spans/day | 172.8 M | 1.728 B | 8.64 B |

The model also uses the following behavioural ASSUMPTIONS, all to be measured in the benchmark (§11):
- 4 fragments per trace;
- 12 `trace_attribute` rows per trace, 2 of them numeric;
- 0.9 enrichment values per trace;
- 0.5 correlation rows per trace;
- 0.1 links per trace;
- 0.1 business operations per trace;
- 0.25 new correlation values per trace;
- `span.attributes` jsonb averaging 350 B;
- indexed values averaging 20 B.

Additional ASSUMPTIONS for the DIGEST tier (ADR-13):
- 5 retained keys per trace with one value each. Keys average 13 B (`user_id`, `client_id`, `auth.factor`, `sms.delivery.status`, `idp.auth_session_id`). Four values average 20 B and one is a 64 B SHA-256 hex value.
- 30 % of traces belong to a business operation, with 3 member traces per operation (0.1 operations per trace, as above).
- A business operation's `attributes` union holds about 6 keys, some with several values, for about 360 B of jsonb.
- `display_value` averages 24 B. `outcome_detail` is mostly NULL.
- GIN `jsonb_path_ops` costs about 12 B per indexed array element: a 4 B hash key with compressed posting lists. Low-cardinality values cost about 1–2 B per item pointer, unique ids about 20–24 B per entry. This is the least certain figure (range 30–120 B per trace) and must be measured.
- No traces are excluded (conservative). Every excluded health-check trace saves its digest row.

### 9.2 Per-row byte arithmetic

**Storage constants.**
- A page is 8 192 B. After the 24 B page header, 8 168 B are usable.
- Each row needs a 4 B line pointer.
- The heap tuple header is 23 B plus the null bitmap, rounded up to 8 B (MAXALIGN). That gives **24 B** when there are no nulls or the bitmap needs at most 1 byte (8 columns or fewer), and **32 B** for 9–64 columns with nulls present.
- An index tuple has an 8 B header plus the aligned keys, MAXALIGNed, plus a 4 B line pointer.
- A varlena value has a 1 B header up to 126 B (unaligned) and a 4 B header above that (4-aligned).

**B-tree leaf fill.**
- Random keys (uuid first): about 69 % (ln 2).
- Increasing keys (time first): about 90 %.
- Mixed patterns: about 75 %.

**`span`** (32 B header):

| Part | Bytes |
|---|---|
| Header + null bitmap | 32 |
| 6 × 8-byte columns (start_time, span_id, duration_ns, resource_id, scope_id, parent_span_id) | 48 |
| trace_id uuid | 16 |
| 2 × int + 4 × smallint | 16 |
| attributes jsonb (4 B header + 350) | 354 |
| Tuple: 466 → MAXALIGN 472, + 4 B line pointer | 476 |
| floor(8 168 / 476) = 17 rows/page → 8 192 / 17 | **482** |
| PK entry: 8 + 16 + 8 + 8 = 40, + 4 B line pointer = 44, at 69 % fill | **64** |
| **Total per span** | **≈ 546 B** (brief: ≈ 520 B) |

**Inline lz4 variant.** ASSUMPTION: the attributes compress 1.6×, from 350 B to 220 B. The tuple is then 336 + 4 = 340 B, which gives 24 rows/page and 341 B per row. With the 64 B PK entry, a span costs **≈ 405 B (−26 %)**.

**`trace_attribute`** (24 B header):

| Part | Bytes |
|---|---|
| Header | 24 |
| ts 8, trace_id 16, key_id 4, origin 2, value_str 21 (value_num 8 on numeric rows only) | 51 |
| Tuple: 75 → MAXALIGN 80, + 4 B line pointer | **84** |
| `kv_ts_idx`: 8 header + 4 + 21, padded to 32, + ts 8 + trace_id 16 = 56, + 4 B line pointer = 60, at 75 % fill | **80** |
| `num_ts_idx`: (48 + 4) / 0.75 = 69 B, on 2 of 12 rows | **12** |
| **Total per row** | **176 B** (brief: 120 B) |
| × 12 rows per trace | **2 112 B per trace** |

**`trace_summary`** (32 B header):

| Part | Bytes |
|---|---|
| Header | 32 |
| 6 × 8-byte columns | 48 |
| trace_id 16 + 6 × 4-byte columns (int/date) | 40 |
| service_ids int[] (24 B array header + 4 × 4 B elements) | 40 |
| Tuple: 160 + 4 = 164, at fillfactor 80 | **205** |
| PK (uuid, date): 36 B at 69 % fill | 52 |
| `start_idx`: 20 B at 90 % fill | 22 |
| `root_svc_start_idx`: 28 B at 85 % fill | 33 |
| `root_svc_dur_idx`: 28 B at 69 % fill | 41 |
| Index subtotal: 148 B × 1.5 (non-HOT update churn) | 222 |
| **Total per trace** | **≈ 430 B** (brief: 350 B) |

**Other tables:**

| Table | Row (heap + line pointer) | Indexes | Per row | Rows/trace | **B/trace** |
|---|---|---|---|---|---|
| `trace_enrichment` | 108 | UNIQUE 58; trace_id 41 | 207 | 0.9 | **186** (brief: 135) |
| `trace_correlation` | 84 | type_value 69; trace 41; id 22 | 216 | 0.5 | **108** (brief: 50) |
| `span_link` | 84 | 2 × 41 | 166 | 0.1 | **17** |
| `business_operation` | ≈ 100 | PK 40; type_first_seen 31 | ≈ 150 | 0.1 | **15** |
| `correlation_value` | 84 | PK 52; operation 41; × 1.2 delete churn | ≈ 212 | 0.25 | **53** |

**Total per trace:**
- **13 841 B (≈ 13.8 KB)** with uncompressed attributes;
- **11 021 B (≈ 11.0 KB)** with inline lz4;
- for comparison, the brief estimated 12.4 KB.

These are the FULL-tier bytes per trace. They are unchanged by ADR-13; only the number of days they are kept changes.

**`trace_digest`** (DIGEST tier, ADR-13). 13 columns, so the null bitmap needs 2 B:
- a row with a NULL (`business_operation_id`, about 70 % of rows) has a 32 B header;
- a row without NULLs has no bitmap and a 24 B header.

| Part | Without operation (70 %) | With operation (30 %) |
|---|---|---|
| Header (+ null bitmap) | 32 | 24 |
| start_time, duration_ns, compacted_at (3 × 8) | 24 | 24 |
| trace_id uuid (+ business_operation_id uuid) | 16 | 32 |
| trace_day, root_service_id, root_name_id, span_count, error_count (5 × 4) | 20 | 20 |
| outcome smallint (+ 2 B pad to int alignment) | 4 | 4 |
| service_ids int[] (24 B array header + 4 × 4 B) | 40 | 40 |
| attributes jsonb: 4 varlena + 4 root + 10 JEntries × 4 + keys 65 + 5 arrays × 8 + values 144 + ≈ 8 pad | 305 | 305 |
| Tuple → MAXALIGN, + 4 B line pointer | 441 → 448 + 4 = **452** | 449 → 456 + 4 = **460** |
| Weighted average | **≈ 455** | |
| floor(8 168 / 455) = 17 rows/page (fillfactor 100, write-once) → 8 192 / 17 | **482** | |
| PK (trace_id, trace_day): 8 + 20 → 32, + 4 = 36 B at 69 % (random uuid) | **52** | |
| `start_idx`: 20 B at 90 % (chunks inserted `ORDER BY start_time`) | **22** | |
| `root_svc_start_idx`: 28 B at 85 % | **33** | |
| `operation_idx` (partial): 28 B at 75 % = 37 B, on 30 % of rows | **11** | |
| `attributes_gin` (`jsonb_path_ops`): 5 elements × ≈ 12 B (**ASSUMPTION**) | **60** | |
| Index subtotal (write-once, so no update-churn factor) | 178 | |
| **Total per trace** | **≈ 660 B** | |

**`business_operation_digest`.** 12 columns. `outcome_detail` is usually NULL, so the header is 32 B.

| Part | Bytes |
|---|---|
| Header + null bitmap | 32 |
| first_seen, last_seen, compacted_at (3 × 8) | 24 |
| operation_id uuid | 16 |
| op_day, operation_definition_id, trace_count (3 × 4) + outcome 2 | 14 |
| display_value text (1 B header + 24) | 25 |
| steps jsonb: 3 objects × ≈ 172 B (44 B header and JEntries + 42 B keys + ≈ 86 B values and padding) + 16 B array + 4 B varlena, after 1 B alignment pad | 537 |
| attributes jsonb (union, **ASSUMPTION**) | 360 |
| Tuple: 1 008 (MAXALIGNed) + 4 B line pointer | **1 012** |
| floor(8 168 / 1 012) = 8 rows/page → 8 192 / 8 | **1 024** |
| PK (operation_id, op_day): 36 B at 75 % (uuidv7, mostly increasing) | **48** |
| `type_first_seen_idx`: 28 B at 75 % | **37** |
| `type_value_idx`: 8 + 4 + 25 → 40, + 4 = 44 B at 69 % | **64** |
| `type_outcome_idx`: 28 B at 75 % | **37** |
| `attributes_gin`: about 6–7 elements × ≈ 12 B (**ASSUMPTION**) | **80** |
| **Total per operation** | **≈ 1 290 B** |
| × 0.1 operations per trace | **129 B per trace** |

**DIGEST tier total: ≈ 789 B per trace** (660 + 129). That is **5.7 %** of the 13.8 KB FULL-tier trace, or 7.2 % of the inline-lz4 figure. The addendum's first estimate of 0.45–0.55 KB per trace left out the indexes and the `steps` array. The digest jsonb columns could also use `STORAGE MAIN COMPRESSION lz4` (§4.3), because `steps` repeats its keys in every element. Any saving is unmeasured and **not** counted here.

### 9.3 Disk per PostgreSQL node

The size of each table is D × (bytes per trace for that table) × retention in days, plus at most one partition of slack.

#### 9.3.1 Before ADR-13 (previous design, kept for comparison)

**Profile M:**

| Table | B/trace | GB/day | Retention | **GB retained** |
|---|---|---|---|---|
| span (uncompressed attributes) | 10 920 | 943.5 | 14 d | 13 209 |
| span (inline lz4, ASSUMPTION) | 8 100 | 699.8 | 14 d | *9 798* |
| trace_attribute | 2 112 | 182.5 | 14 d | 2 555 |
| span_link | 17 | 1.5 | 14 d | 21 |
| trace_summary | 430 | 37.2 | 30 d | 1 115 |
| trace_enrichment | 186 | 16.1 | 30 d | 482 |
| trace_correlation | 108 | 9.3 | 90 d | 840 |
| business_operation | 15 | 1.3 | 90 + 31 d | 157 |
| correlation_value | 53 | 4.6 | 90 d | 412 |
| Dictionaries, config, resources | — | — | — | < 5 |
| **Total, uncompressed** | 13 841 | **1 196** | | **≈ 18.8 TB** |
| **Total, inline lz4** | 11 021 | **952** | | **≈ 15.4 TB** |

**All profiles, with 25 % headroom.** The headroom covers in-flight partitions, temp files, bloat, `max_wal_size` plus archive backlog, and operational slack.

| Profile | Uncompressed | + 25 % | Inline lz4 | + 25 % |
|---|---|---|---|---|
| S | 1.9 TB | **2.35 TB** | 1.54 TB | **1.9 TB** |
| M | 18.8 TB | **23.5 TB** | 15.4 TB | **19.2 TB** |
| L | 94 TB | **117 TB** | 77 TB | **96 TB** |

> **Note:** the brief's figure of about 19 TB per node at M corresponded to the inline-lz4 column. The previous recommendation was 24 TB of usable NVMe per node at M. It is **superseded** by §9.3.2.

#### 9.3.2 With two-tier retention (ADR-13), profile M, n = 7, m = 90

Effective retention follows §4.7 and §8.7. FULL-tier tables are kept n days plus about 1 day of drop-gate slack (the day being compacted), so the model uses **8 days**. `trace_correlation` and `correlation_value` are additionally held for `max(merge_window)` + 1 day, which gives 10 days at the default 1-day merge window. A `business_operation` month partition lives at most 31 + 7 + 2 ≈ 40 days. A DIGEST partition holds data for m days: it is filled about n + 1 days after its trace day and dropped at n + m.

| Table | Tier | B/trace | GB/day | Effective retention | **GB retained** |
|---|---|---|---|---|---|
| span (uncompressed attributes) | FULL | 10 920 | 943.5 | 8 d | 7 548 |
| span (inline lz4, ASSUMPTION) | FULL | 8 100 | 699.8 | 8 d | *5 598* |
| trace_attribute | FULL | 2 112 | 182.5 | 8 d | 1 460 |
| span_link | FULL | 17 | 1.5 | 8 d | 12 |
| trace_summary | FULL | 430 | 37.2 | 8 d | 298 |
| trace_enrichment | FULL | 186 | 16.1 | 8 d | 129 |
| trace_correlation | FULL | 108 | 9.3 | 10 d | 93 |
| business_operation | FULL | 15 | 1.3 | ≤ 40 d | 52 |
| correlation_value | FULL | 53 | 4.6 | 10 d | 46 |
| Dictionaries, config, resources | — | — | — | — | < 5 |
| **FULL tier, uncompressed** | | 13 841 | **1 196** | | **9 643 ≈ 9.6 TB** |
| **FULL tier, inline lz4** | | 11 021 | **952** | | **7 693 ≈ 7.7 TB** |
| trace_digest | DIGEST | 660 | 57.0 | 90 d (m) | 5 132 |
| business_operation_digest | DIGEST | 129 | 11.1 | 90 d (m) | 1 003 |
| compaction_state, compaction_exclusion | — | — | — | — | < 1 |
| **DIGEST tier** | | 789 | **68.2** | | **6 135 ≈ 6.1 TB** |
| **Total, uncompressed** | | | | | **15 778 ≈ 15.8 TB** |
| **Total, inline lz4** | | | | | **13 828 ≈ 13.8 TB** |

**Variant m = 30** (n = 7): DIGEST tier = 68.2 GB/day × 30 = 1 711 + 334 = **2 045 GB ≈ 2.0 TB**. The total is **11 688 GB ≈ 11.7 TB** uncompressed and **9 738 GB ≈ 9.7 TB** with inline lz4.

**All profiles, with 25 % headroom.** Sizes scale linearly with D; the dictionaries are negligible.

| Profile | m | Uncompressed | + 25 % | Inline lz4 | + 25 % |
|---|---|---|---|---|---|
| S | 90 | 1.58 TB | **1.97 TB** | 1.38 TB | **1.73 TB** |
| M | 90 | 15.8 TB | **19.7 TB** | 13.8 TB | **17.3 TB** |
| L | 90 | 78.9 TB | **98.6 TB** | 69.1 TB | **86.4 TB** |
| S | 30 | 1.17 TB | **1.46 TB** | 0.97 TB | **1.22 TB** |
| M | 30 | 11.7 TB | **14.6 TB** | 9.7 TB | **12.2 TB** |
| L | 30 | 58.4 TB | **73.1 TB** | 48.7 TB | **60.9 TB** |

**Before and after, profile M, with 25 % headroom:**

| | Before (§9.3.1) | ADR-13, m = 90 | ADR-13, m = 30 |
|---|---|---|---|
| Uncompressed | 23.5 TB | **19.7 TB (−16 %)** | **14.6 TB (−38 %)** |
| Inline lz4 | 19.2 TB | **17.3 TB (−10 %)** | **12.2 TB (−36 %)** |
| Span detail available | 14 d | 7 d | 7 d |
| Search and trace header (summary or digest) | 30 d | 97 d (7 d full + 90 d digest) | 37 d |
| Business operations | 90 d (no outcome) | 97 d with outcome and steps | 37 d with outcome and steps |

The saving is smaller than a naive "half the span days" estimate for three reasons:
- the 90-day digest still costs 6.1 TB;
- the drop gate adds a day of FULL-tier slack;
- the digest's GIN and `steps` columns are not free.

In exchange, history with outcomes extends from 30–90 days to 97 days.

> **Recommendation (ADR-13 defaults n = 7, m = 90):**
> - provision **20 TB of usable NVMe per node at M**, down from 24 TB;
> - **18 TB** is enough if the benchmark confirms an inline lz4 ratio of at least 1.5× (make inline compression the default in that case);
> - **15 TB** is enough with m = 30.
> - Profile S: 2 TB. Profile L: about 100 TB, which still does not fit comfortably on a single primary (§14).
> - **Compaction lag eats headroom.** Each day of lag keeps one more FULL-tier day, 1.2 TB at M uncompressed. The 3.9 TB of headroom at M (19.7 − 15.8) therefore absorbs about 3 days of lag, which is why the alert thresholds of §8.7 are 1 and 2 days.
> - **Rollout.** When changelog 005 shortens retention, PartitionManager drops the old 14-, 30- and 90-day tails gradually, as soon as the drop gate allows. Compaction starts with the oldest trace day still present in `trace_summary`. Until the backlog is compacted, the disk keeps the old footprint.

### 9.4 Write throughput, WAL and IOPS (profile M)

**Table data written.** 13.8 KB/trace × 1 000 traces/s is **13.8 MB/s on average and about 41 MB/s at peak**. With inline lz4 it is 11 MB/s average and 33 MB/s peak.

**Rows inserted per second (average / peak):**
- spans: 20 k / 60 k;
- `trace_attribute`: 12 k / 36 k;
- summary statements (1 insert + 3 updates per trace): 4 k / 12 k;
- other tables: about 2 k / 6 k.

That amounts to about 53 k index entries/s on average and 160 k/s at peak.

**WAL volume.**
- Each record carries about 50 B of overhead, and a trace produces about 80 records. That gives about 13.8 KB + 4 KB ≈ 17.8 KB per trace, or **17.8 MB/s**.
- Full-page images add to this. New heap pages need none, but the random index leaf pages of the hot hour (about 1–1.5 M pages) need one per checkpoint cycle each. At `checkpoint_timeout = 15min` with `wal_compression = zstd` (ASSUMPTION: about 3×), this adds roughly 4–8 MB/s.
- **Total WAL: ≈ 22–26 MB/s on average, ≈ 65–78 MB/s at peak, ≈ 2.1 TB per day.**
- Archived WAL after pgBackRest zstd compression (ASSUMPTION: 3×) is **≈ 0.7 TB per day**.
- This is in line with the brief's estimate of 1.5–2× the data volume and about 75 MB/s at peak.
- **ADR-13 compaction** adds about 68 GB/day of digest heap and index writes. Including GIN full-page images, that is ≈ 80–100 GB of WAL per day, about +4–5 %. The total stays **≈ 2.1–2.2 TB of WAL per day**, or ≈ 0.7–0.75 TB archived. Ingest WAL is unchanged, because ADR-13 does not change what is written, only how long it is kept.

**Compaction workload (ADR-13, profile M, per day of data):**

| Item | Volume | Note |
|---|---|---|
| Read `trace_summary` (heap + `start_idx` range) | ≈ 20 GB | 86.4 M rows, 1-hour chunks, about 1/24 of the partition per chunk |
| Read `trace_attribute` index entries of retained keys | ≈ 35 GB physical; ≈ 280 GB logical | 5 keys × 80 B × 86.4 M. Each chunk scans the 7–8 hourly partitions of its window. Consecutive chunks share 7 of 8 partitions (a ≈ 11 GB working set), so ≈ 7/8 of the logical reads hit `shared_buffers` or the page cache. |
| Read `trace_correlation`, `correlation_value`, `business_operation` | ≈ 5 GB | 0.5 correlation rows per trace; root resolution once per distinct operation |
| Read `trace_digest` for phase 2 | ≈ 15 GB | 30 % of the day's digest rows, plus PK probes |
| **Physical reads** | **≈ 75 GB/day** | |
| Write `trace_digest` + `business_operation_digest` | **≈ 68 GB/day** | 57.0 + 11.1 GB, plus WAL as above |
| Duration (**ASSUMPTION**, to be benchmarked) | **≈ 20–40 min per day of data** | 25 chunks of about 3.6 M traces, 50–100 s each (jsonb aggregation and GIN inserts dominate), plus phase 2 and VACUUM. That is ≈ 30–60 MB/s of extra writes and 1–2 cores while it runs. |

Compaction runs on the primary, once per day of data in steady state, and fits within the 300 MB/s and 20 k IOPS floor next to peak ingest. If benchmarks show interference, the job can be confined to off-peak hours. Lag is allowed up to the 1-day warning threshold.

**Disk load on the primary at peak:**

| Writer | Throughput | IOPS |
|---|---|---|
| WAL (sequential; group commit at about 150 commits/s) | ≈ 80 MB/s | — |
| Checkpointer and bgwriter (dirty heap pages plus random index pages) | ≈ 60 MB/s | 5–10 k random 8 KB writes |
| Autovacuum | ≈ 30 MB/s | — |
| Archiver | ≈ 20 MB/s | — |
| **Total** | **≈ 200 MB/s** | **≈ 10–15 k** |

The brief's floor of 20 k IOPS and 300 MB/s therefore gives about 1.5× headroom. Local NVMe is recommended because brute-force scans and backups add bursts on top.

### 9.5 Memory

The hot set of randomly accessed index pages:

| Hot set | Average hour | Peak hour |
|---|---|---|
| `span` PK of the current hour (72 M / 216 M spans × 64 B) | 4.6 GB | 13.8 GB |
| `trace_attribute` `kv_ts_idx` of the current hour (43 M / 130 M rows × 80 B) | 3.5 GB | 10.4 GB |
| `trace_summary` PK + duration index of the current day (86.4 M × 93 B, by end of day) | 8.0 GB | 8.0 GB |
| Previous hour (late fragments) | ≈ 1 GB | ≈ 3 GB |
| Dictionaries and upper index levels | < 1 GB | < 1 GB |
| **Total** | **≈ 17 GB** | **≈ 35 GB** |

On a primary with **128 GB RAM**:
- `shared_buffers = 32GB` covers the average hot set; at peak, the roughly 80 GB OS page cache absorbs the rest.
- `effective_cache_size = 96GB`.
- `work_mem`: 16 MB globally, 32 MB for `tc_ingester`, 64 MB for `tc_query`.
- `maintenance_work_mem = 2GB`; `autovacuum_work_mem = 1GB` (× 6 workers).
- `temp_buffers = 64MB` for `tc_ingester`.
- `huge_pages = on`.

The replica has the same size. Other profiles:
- **S:** 32 GB RAM, `shared_buffers = 8GB`.
- **L:** 512 GB RAM, `shared_buffers = 128GB`. The peak-hour hot set is about 175 GB, so use 30-minute partitions or one of the alternatives.

### 9.6 CPU (ASSUMPTION; per-row costs to be measured)

Primary at peak:

| Work | Cores |
|---|---|
| Span inserts (60 k/s × ≈ 25 µs) | 1.5 |
| Attribute inserts (36 k/s × ≈ 10 µs) | 0.4 |
| Summary upserts (12 k/s × ≈ 40 µs) | 0.5 |
| CTE evaluation and COPY parsing | ≈ 1 |
| WAL writer, walsenders, archiver | ≈ 1.5 |
| Checkpointer and bgwriter | ≈ 0.5 |
| Autovacuum | ≈ 4 |
| Enrichment, resolver, maintenance | ≈ 1 |
| Digest compaction (ADR-13; about 30 min per day, while it runs) | ≈ 1–2 |
| **Total** | **≈ 10–12 (≈ 12–14 while compaction runs)** |

**32 vCPU** keeps the primary below 40 % at peak and lets either node take the query load after a failover (symmetric nodes). If the primary never serves queries, 16 vCPU is the minimum.

The replica also needs 32 vCPU: brute-force scans use up to 8 parallel workers, and the steady search load is about 2–4 cores.

For profile S use 8 vCPU. For profile L use 64 or more, at which point the alternatives become attractive.

### 9.7 Connections and pooling

| Client | Instances × Hikari pool | Server connections | Target / mode |
|---|---|---|---|
| tc-ingester | 3 × 10 (8 consumer threads + 2 dictionary) | 30 | Primary, via PgBouncer **session-mode pool `tc_ingest`** (needed for the session temp staging tables, §6.3) |
| tc-enricher | 2 × 6 | 12 | Primary, PgBouncer transaction mode |
| tc-control-plane | 2 × 5 | 10 | Primary, PgBouncer transaction mode |
| tc-maintenance | 2 × 4 (only the leader is busy) | 8 | Primary, **direct connection** (autocommit `DETACH CONCURRENTLY`, long `VACUUM`, `SET ROLE`) |
| tc-schema-migrator | 1 × 2 | 2 | Primary, direct |
| tc-query-api | 2 × 20 (virtual threads; the pool caps DB concurrency) | 40 | Replica, PgBouncer transaction mode |
| Patroni, monitoring, replication, reserve | — | ≈ 20 | |

- **PostgreSQL:** `max_connections = 300` on both nodes. A standby must have a value at least as high as the primary's.
- **PgBouncer** 1.26.1, two instances:
  - `max_client_conn = 2000`;
  - `default_pool_size = 20`;
  - `max_prepared_statements = 200`, which must be greater than 0 for PgJDBC protocol-level prepared statements to work in transaction mode.

### 9.8 Kafka topic sizing

**`otlp-traces` at profile M:**
- Uncompressed traffic is 1.6 KB/span × 20 k spans/s = 32 MB/s on average and 96 MB/s at peak.
- With zstd at about 8× (ASSUMPTION) that is about 4 MB/s, or 345 GB/day.
- With RF 3 the cluster stores 1.04 TB/day. Over 3 days of retention that is **3.1 TB**, so plan for at least 1.5 TB per broker.
- Across 24 partitions, the peak per partition is 4 MB/s, or about 2 500 spans/s.

**Catch-up after a 1 h PostgreSQL outage (UC-17).**
- The backlog is 72 M spans.
- Spare capacity is 60 k − 20 k = 40 k spans/s, so the backlog drains in about **30 min**.
- Kafka's 3-day retention is the maximum outage TCS can absorb without data loss.

> **Note (message size).** One export of 512 spans of a single trace is about 820 KB of JSON. That is close to Kafka's default 1 MiB limit for both `max.message.bytes` and the producer's `max.request.size`.
> - Set `max.message.bytes = 8388608` on `otlp-traces` to match the 8 MiB fragment limit.
> - Set `max.partition.fetch.bytes` and `fetch.max.bytes` to at least 8 MiB in tc-ingester.
> - Producers should raise `max.request.size` and use `compression.type=zstd`. The limit applies to the compressed batch.

**DLQ topics.** `otlp-traces.dlq` and `tc-enrichment.dlq` (6 partitions, 14 days) are sized for less than 0.1 % poison messages, which is under 30 GB each at RF 3.

---

## 10. PostgreSQL configuration (profile M, primary and replica)

| Setting | Value | Justification |
|---|---|---|
| `shared_buffers` / `huge_pages` | `32GB` / `on` | Hot set (§9.5); fewer TLB misses. |
| `effective_cache_size` | `96GB` | |
| `work_mem` / `maintenance_work_mem` / `autovacuum_work_mem` | `16MB` / `2GB` / `1GB` | Role overrides in the table below. |
| `max_connections` | `300` | §9.7; same on both nodes. |
| `wal_level` | `replica` | Streaming replication and PITR. |
| `wal_compression` | `zstd` | Full-page images from random index pages dominate WAL bytes. |
| `wal_buffers` | `64MB` | High WAL rate. |
| `max_wal_size` / `min_wal_size` | `64GB` / `8GB` | Checkpoints stay time-driven (at peak, 15 min ≈ 70 GB). |
| `checkpoint_timeout` / `checkpoint_completion_target` | `15min` / `0.9` | Fewer full-page images; crash recovery takes about 1–3 min. |
| `synchronous_commit` | `on` | Kafka offsets are committed after the PG commit. `off` would lose rows permanently. **Do not change.** |
| `synchronous_standby_names` | managed by Patroni (`synchronous_mode: true`) | §12.2. |
| `io_method` / `io_workers` | `io_uring` (if built `--with-liburing` and the kernel allows it), otherwise `worker` with `8` | PG 18 async I/O for seq scans, bitmap heap scans and vacuum. Takes effect only at server start. |
| `effective_io_concurrency` / `maintenance_io_concurrency` | `128` / `128` | NVMe queue depth; drives read-ahead. |
| `random_page_cost` / `seq_page_cost` | `1.1` / `1.0` | NVMe. |
| `default_toast_compression` | `lz4` | The default is `pglz`. lz4 needs a `--with-lz4` build, which is standard in PGDG packages. |
| `autovacuum_max_workers` / `autovacuum_worker_slots` | `6` / `16` | Many partitions; adjustable on reload in PG 18. |
| `autovacuum_naptime` / `autovacuum_vacuum_cost_limit` | `15s` / `2000` | The current-hour partitions need frequent visits; NVMe absorbs the I/O. |
| `autovacuum_freeze_max_age` | `400000000` | Early per-partition freezing (§8.5) makes forced anti-wraparound vacuums rare. |
| `max_worker_processes` / `max_parallel_workers` | `32` / `16` | |
| `max_parallel_workers_per_gather` | primary `2`, replica `4` (brute force uses 8 locally) | |
| `max_parallel_maintenance_workers` | `4` | |
| `jit` | `off` | Short OLTP-like statements; JIT compile time would exceed the gain. |
| `max_locks_per_transaction` | `512` | Many partitions × indexes; PG 18 sizes the fast-path lock slots from this value. |
| `plan_cache_mode` | `auto`, but `force_custom_plan` for `tc_query` | §7. |
| `default_statistics_target` | `100` (1000 on `value_str`) | §8.5. |
| `timezone` / `log_timezone` | `UTC` | Partition bounds and `trace_day`. |
| `hot_standby` / `hot_standby_feedback` / `max_standby_streaming_delay` | `on` / `on` / `30s` | §12.3. |
| `max_slot_wal_keep_size` | `500GB` | A dead slot cannot fill the disk (about 6 h of WAL at peak). |
| `archive_mode` / `archive_command` | `on` / `pgbackrest --stanza=tc archive-push %p` (`archive-async=y`) | PITR. |
| `idle_in_transaction_session_timeout` | `60s` | |
| `log_min_duration_statement` / `log_parameter_max_length` | `1s` / `0` | Bind values (PII) are never logged. |
| `log_autovacuum_min_duration` / `log_lock_waits` / `track_io_timing` | `10s` / `on` / `on` | |
| `shared_preload_libraries` | `pg_stat_statements` (plus `pg_partman_bgw` if pg_partman is chosen) | |
| `ssl` / `password_encryption` | `on` / `scram-sha-256` | §13. |

**Per-role settings** (`ALTER ROLE … SET`):

| Role | statement_timeout | transaction_timeout | lock_timeout | Other |
|---|---|---|---|---|
| `tc_ingester` | 60 s | 120 s | 10 s | `temp_buffers=64MB`, `work_mem=32MB` |
| `tc_enricher` | 30 s | 120 s | 10 s | |
| `tc_query` | 15 s (brute force raises it to 30 s locally) | 60 s | 1 s | `default_transaction_read_only=on`, `work_mem=64MB`, `plan_cache_mode=force_custom_plan`, `max_parallel_workers_per_gather=4` |
| `tc_admin` | 10 s | 30 s | 5 s | |
| `tc_maintenance` | 0 | 0 | 5 s | |
| `tc_migrator` | 0 | 0 | 10 s | |

Any role can change `statement_timeout` for its own session, so it is a default rather than a hard cap. `tc-query-api` therefore enforces the real limits in code.

---

## 11. Benchmark plan (validates every ASSUMPTION)

**Corpus generation from the starter.**
1. Extend `opentelemetry-demo` into four IdP services: `idp-gateway`, `idp-auth-service`, `idp-otp-service` and `idp-token-service`.
   - Use the starter with sampling 1.0, zstd, and `@TraceAttribute` for `user_id`, `client_id`, `auth.factor`, `idp.auth_session_id` and `oauth.authorization_code`.
   - Use HTTP and Kafka hops so that traces have about 20 spans on average, with a tail up to 500.
2. Drive the login flow (brief §10) with Gatling or k6 for 1 h and capture `otlp-traces`. This is the **seed corpus** of real `TraceMessage` bytes.
3. Build an **amplifier** (Java) that replays the seed into a benchmark topic at a configurable T. For each replay it:
   - rewrites trace and span ids consistently within a trace;
   - shifts timestamps to "now";
   - randomises user, session and code values with Zipf-distributed cardinality.

   This preserves realistic fragment interleaving.
4. Build an enrichment generator for `sms-gateway.delivery-reports` that sets the `traceparent` headers of the T2 traces.

**Metrics.**
- **Size:** `pg_total_relation_size` / `pg_indexes_size` deltas divided by traces ingested give B/trace per table, to compare against §9.2. Use `pgstattuple` / `pgstatindex` for leaf density.
- **WAL:** `pg_stat_wal` (`wal_records`, `wal_fpi`, `wal_bytes`) per minute, and the ratio of WAL bytes to table bytes.
- **Ingest:** spans/s per ingester vCPU, consumer lag, and commit latency p50/p99 (Micrometer timer around COPY + CTE + COMMIT).
- **Cache and I/O:** buffer hit ratio (`pg_statio_user_tables`, `pg_stat_io`), `iostat` IOPS and MB/s, and `pg_stat_checkpointer`.
- **Vacuum:** duration and frequency per partition, `relallvisible / relpages` on closed partitions, and `age(relfrozenxid)`.
- **Replication:** `pg_stat_replication.replay_lag` p99, `pg_stat_database_conflicts`, and the commit-latency increase from synchronous replication.
- **Queries:** p50/p95/p99 for UC-11 (with and without hints), UC-12 (selective, non-selective and multi-predicate), UC-13 (15 min and 1 h windows), UC-14, UC-15 and UC-20. Measure on the replica while ingest runs at peak, and record `EXPLAIN (ANALYZE, BUFFERS)` for each.
- **Compression:** `span` size with and without `toast_tuple_target=128`, and the lz4 CPU cost.
- **Compaction (ADR-13):**
  - B/trace of `trace_digest` and `business_operation_digest`, measured separately for heap, B-tree and GIN, against §9.2. The GIN size is the least certain input.
  - `tc_compaction_chunk_seconds` p50/p99, and the total time to compact one day at 1× M while ingest runs at peak.
  - Ingest commit-latency p99 and replica replay lag with and without compaction running.
  - Read amplification of the `trace_attribute` window (`pg_stat_io`, buffer hits versus reads).
- **DIGEST-tier queries:** UC-11 digest fallback, cross-tier UC-12 (selective and non-selective `@>`, 31-day DIGEST-only window), and business-operation search by value and by outcome.

**Scenarios.**
1. 6 h at 1× M.
2. 1 h at 3× M.
3. A 30 min PostgreSQL outage followed by catch-up.
4. A 6 h replay, after which counters must be unchanged.
5. A Patroni switchover under load.
6. A 24 h soak with partition rotation and drops.
7. **Compaction (ADR-13).** Run with `fullDataDays = 2` and `digestDays = 3`, for 5 days at 1× M. A compressed-time variant shifts the amplifier timestamps back by n days into pre-created partitions, with a test build that disables the too-old guard. Cover:
   - steady-state compaction and the drop gate;
   - a forced compaction failure: FULL-tier partitions must be kept, lag must alert, and nothing uncompacted may be dropped;
   - a kill of the leader during a chunk: it resumes from `last_chunk_end` with no duplicates;
   - digest partition drops at n + m.

**Pass criteria.**
- Ingest sustains 60 k spans/s for 1 h with non-growing lag and commit p99 < 500 ms.
- B/trace is within ±20 % of the model; otherwise re-size.
- WAL is at most 2× the table bytes.
- Replay lag p99 < 10 s.
- Query latency:
  - UC-11 p95 < 300 ms;
  - UC-12 selective p95 < 1 s for a 24 h window and < 3 s for a 7 d window;
  - UC-15 p95 < 200 ms;
  - UC-14 p95 < 500 ms;
  - UC-13 on a 15 min window at M < 30 s.
- A replay produces no duplicate spans and leaves `span_count` unchanged.
- No anti-wraparound vacuum occurs during the soak.
- No replica query is cancelled by partition maintenance.
- Compaction:
  - one day at M is compacted in < 1 h, with ingest commit p99 still < 500 ms;
  - digest B/trace is within ±20 % of 789 B;
  - the drop gate never drops a partition of a day that is not DONE;
  - `traces_digested + traces_excluded` equals the `trace_summary` row count of the day.
- DIGEST-tier latency:
  - UC-11 fallback p95 < 300 ms;
  - cross-tier UC-12 selective p95 < 1.5 s for a 7 d window and < 3 s for a 31 d DIGEST-only window;
  - business-operation digest search p95 < 500 ms.

---

## 12. High availability, backup and recovery

### 12.1 Topology

- **Patroni 4.1.5** manages the PostgreSQL 18.7 nodes. The DCS is a 3-node etcd cluster or the Kubernetes API.
- The **primary** serves ingest, enrichment, the control plane and maintenance.
- The **synchronous hot standby** is both the failover target and the read target of `tc-query-api`.
- Services `tc-pg-primary` and `tc-pg-replica` are managed by Patroni and fronted by PgBouncer. tc-maintenance and tc-schema-migrator connect to the primary directly.

### 12.2 Synchronous replication (decision) and its trade-off

TCS uses **Patroni `synchronous_mode: true`**, with one synchronous standby and `synchronous_mode_strict: false`.

**Why.** Kafka offsets are committed right after the PostgreSQL commit (ADR-02).
- With asynchronous replication, a failover could lose rows whose offsets were already committed. Those rows would never be replayed automatically.
- With synchronous mode, Patroni only promotes a node that is known to be synchronous, so the RPO is 0 for committed ingest batches.

**Costs and how they are bounded.**
- Each commit pays an extra network round trip plus the standby's WAL flush, typically 1–3 ms within one data centre. This is negligible at about 150 commits/s with batched transactions.
- A standby that is overloaded by queries can slow ingest. That load is bounded by the query-side timeouts and the brute-force limits. A third node (async, query-only) can isolate query load completely if measurements show interference.
- If the standby fails, the non-strict mode lets Patroni degrade to asynchronous operation instead of blocking writes. Alerts fire.

**Runbook: failover to a node that was asynchronous** (degraded mode, or a manual failover to an async node).
1. Determine the replication lag window at the time of failure. Use Patroni history and `pg_last_xact_replay_timestamp()` on the new primary.
2. Stop `tc-ingester` and every `tc-enricher.<sourceName>` group by scaling to 0.
3. Rewind each group to the failure time minus the lag window minus a safety margin of 5 min:

   ```
   kafka-consumer-groups --group tc-ingester --reset-offsets \
       --to-datetime <failure_time - lag_window - 5min> --topic otlp-traces --execute
   ```

4. Restart the groups. Idempotent writes (§6.6) absorb the replayed records.

The correlation resolver needs no action: its watermark lives in the same database and rolls back consistently with the data.

### 12.3 Reading from the replica

- **`hot_standby_feedback = on`.**
  - Replica queries hold back the primary's vacuum horizon. This affects only the mutable tables, and the 15–30 s query timeouts bound it.
  - Without feedback, the frequent `trace_summary` updates would cancel replica queries with "conflict with recovery".
- **Lock conflicts still apply.** Replay of a partition DROP or of index DDL can conflict with replica queries. The API's `from` clamp (§8.3) makes these conflicts rare.
- **`max_standby_streaming_delay = 30s`.** Replay waits at most as long as the longest allowed query, which is brute force at 30 s. Larger values would increase staleness.
- **Staleness is visible.** Every API response carries `dataWatermark`, derived from `pg_last_xact_replay_timestamp()`.

### 12.4 Backup and PITR (pgBackRest)

**Configuration.**
- `repo1` on object storage, with `archive-async=y`, `process-max=8` and `compress-type=zst`.
- Backups run from the standby (`backup-standby=y`): a weekly full backup plus daily differentials.
- WAL archiving is continuous. Retention is 2 full backups plus 7 days of PITR.

**Repository size at M** (ASSUMPTION: 2.5× zstd compression on data pages):

| Component | Size |
|---|---|
| 2 full backups (2 × 6.3 TB; ADR-13 database ≈ 15.8 TB / 2.5) | 12.6 TB |
| Differentials | ≈ 2.4 TB |
| 7 days of archived WAL (7 × 0.73 TB) | ≈ 5.1 TB |
| **Total** | **≈ 20 TB (19–22 TB)** |

Before ADR-13 the figures were 2 × 7.6 TB for full backups and ≈ 22–25 TB in total.

**Restore time** (disaster recovery after losing both nodes):
- Restoring about 15.8 TB at 1–1.5 GB/s takes **3–4.5 h**. Before ADR-13 it was about 19 TB and 3.5–5.5 h.
- Replaying up to 24 h of WAL (about 2.2 TB at 150–250 MB/s) takes **2.5–4 h**.
- **RTO is therefore ≈ 5.5–8.5 h.** RPO equals the archive lag, which is seconds.
- After the restore, a Kafka replay from the restore point minus 5 min fills the gap.

**Cost-saving option.**
- Back up only the configuration, dictionaries and the `business_operation` / `correlation_value` tables, with a nightly `pg_dump -t …`. Under ADR-13, add the DIGEST-tier tables `trace_digest` and `business_operation_digest` (≈ 6.1 TB at M). Kafka cannot rebuild them, because it keeps only 3 days.
- Recover from disaster by creating a fresh schema and replaying Kafka. That restores the last 3 days, and catch-up takes about 24 h at 3× capacity.
- This option loses older trace data. It is an operator decision and is recorded as an ADR in [02-high-level-design.md](02-high-level-design.md).

---

## 13. Security

### 13.1 Roles

Roles are created by infrastructure-as-code. Secrets live in Vault or Kubernetes secrets. Authentication is `scram-sha-256` over `hostssl` only.

```sql
CREATE ROLE tc_owner       NOLOGIN;                    -- owns schema tc and all objects incl. partitions
CREATE ROLE tc_migrator    LOGIN IN ROLE tc_owner;     -- Liquibase; Hikari connection-init-sql: SET ROLE tc_owner
CREATE ROLE tc_maintenance LOGIN IN ROLE tc_owner;     -- partition DDL, VACUUM, resolver, GC (SET ROLE tc_owner)
CREATE ROLE tc_ingester    LOGIN;
CREATE ROLE tc_enricher    LOGIN;
CREATE ROLE tc_admin       LOGIN;                      -- tc-control-plane
CREATE ROLE tc_query       LOGIN;                      -- tc-query-api, replica only
```

`ATTACH`, `DETACH` and `DROP` all require ownership of the tables involved. That is why `tc_maintenance` acts as `tc_owner`.

### 13.2 Grants (`004-grants.sql`)

```sql
REVOKE ALL ON SCHEMA tc FROM PUBLIC;
GRANT USAGE ON SCHEMA tc TO tc_ingester, tc_enricher, tc_admin, tc_query;
GRANT TEMPORARY ON DATABASE tracing TO tc_ingester;

GRANT SELECT ON tc.attribute_key, tc.indexed_attribute, tc.business_operation_definition, tc.correlation_key,
                tc.retention_policy, tc.config_revision, tc.enrichment_source,
                tc.enrichment_source_revision, tc.kafka_connection
      TO tc_ingester, tc_enricher;

-- tc_ingester
GRANT SELECT, INSERT ON tc.service, tc.span_name, tc.instrumentation_scope TO tc_ingester;
GRANT INSERT, UPDATE (value_type) ON tc.attribute_key TO tc_ingester;
GRANT SELECT, INSERT, UPDATE (last_seen) ON tc.resource TO tc_ingester;
GRANT SELECT, INSERT ON tc.span, tc.trace_attribute, tc.span_link, tc.trace_correlation TO tc_ingester;
GRANT SELECT, INSERT, UPDATE ON tc.trace_summary TO tc_ingester;

-- tc_enricher
GRANT SELECT, INSERT ON tc.trace_enrichment, tc.trace_attribute, tc.trace_correlation TO tc_enricher;

-- tc_admin (control plane); revisions are append-only
GRANT SELECT, INSERT, UPDATE ON tc.kafka_connection, tc.enrichment_source, tc.attribute_key,
                               tc.indexed_attribute, tc.business_operation_definition, tc.correlation_key,
                               tc.retention_policy, tc.config_revision TO tc_admin;
GRANT SELECT, INSERT ON tc.enrichment_source_revision TO tc_admin;
GRANT DELETE ON tc.correlation_key TO tc_admin;
GRANT SELECT ON tc.service, tc.span_name TO tc_admin;

-- tc_query (read-only; the grants replicate to the standby)
GRANT SELECT ON ALL TABLES IN SCHEMA tc TO tc_query;
ALTER DEFAULT PRIVILEGES FOR ROLE tc_owner IN SCHEMA tc GRANT SELECT ON TABLES TO tc_query;
REVOKE SELECT ON tc.kafka_connection, tc.shedlock FROM tc_query;
-- Changelog 005 (ADR-13) adds grants for compaction_exclusion, compaction_state and tc.operation_root(); see §4.7.
```

Notes on these grants:
- Access *through* a partitioned parent is checked on the parent only, so partitions need no grants of their own.
- Identity columns need no sequence grants.
- **No row-level security**: TCS is single-tenant. If multi-tenancy is ever needed, add `tenant_id` as the leading column of `trace_attribute` and `trace_summary`, plus RLS policies, via a new ADR.

### 13.3 PII

1. **Minimise at the source.** Producers should send pseudonymous ids such as `user_id = u-42`, never e-mail addresses or phone numbers.
2. **Hash enrichment values.** Enrichment field mappings support the `SHA256` transform.
   > **Note:** a plain SHA-256 of a low-entropy identifier can be reversed by enumeration. The transform is therefore implemented as **HMAC-SHA256 with a pepper**:
   > - `transformArg` holds a secret reference that `SecretResolver` resolves; the pepper is never stored in the database.
   > - The key catalogue marks the key as hashed, so `tc-query-api` applies the same HMAC to search input.
3. **Mask at read time (UC-19).** Callers without the `TC_PII` scope see PII keys masked everywhere: attributes, enrichment values, autocomplete and correlation display values.
4. **Keep PII out of logs.** Set `log_parameter_max_length = 0` and never `log_statement = all`. DLQ topics contain raw payloads, so their ACLs are restricted to operators.
5. **Longer-lived PII in the DIGEST tier (ADR-13).** Retained keys live n + m days (97 by default) instead of n. Flag only the keys that are needed, and use `digest_transform` SHA256 or MASK for personal identifiers. Targeted erasure in the DIGEST tier is a maintenance-role `DELETE` on `trace_digest` and `business_operation_digest` by `trace_id` / `operation_id`.
6. **Erasure.** Bounded retention is the primary control. Exceptional targeted erasure is a manual maintenance-role `DELETE` on `span`, `trace_attribute`, `trace_enrichment` and `trace_correlation` for the affected `trace_id`s.

### 13.4 Encryption

- **In transit:** TLS 1.2 or later everywhere, including clients, PgBouncer, replication and the Patroni REST API.
- **At rest:** community PostgreSQL 18 has no TDE. Use volume encryption instead: LUKS/dm-crypt, or cloud volumes encrypted with KMS keys. Keep data checksums enabled.
- **Backups:** pgBackRest `repo1-cipher-type = aes-256-cbc` with the passphrase in Vault, plus bucket-side encryption.
- **Kafka:** SASL_SSL for all clients, and broker disk encryption.

---

## 14. Alternatives (only as alternatives)

### 14.1 ClickHouse for span storage

This schema sketch is modelled on the OTel `clickhouseexporter` tables `otel_traces` and `otel_traces_trace_id_ts`, adapted to binary ids and typed maps.

```sql
CREATE TABLE tc.spans ON CLUSTER tc
(
    Timestamp     DateTime64(9)                        CODEC(Delta, ZSTD(1)),
    TraceId       FixedString(16)                      CODEC(ZSTD(1)),   -- binary, not hex
    SpanId        UInt64                               CODEC(ZSTD(1)),
    ParentSpanId  UInt64                               CODEC(ZSTD(1)),
    TraceState    String                               CODEC(ZSTD(1)),
    ServiceName   LowCardinality(String)               CODEC(ZSTD(1)),
    SpanName      LowCardinality(String)               CODEC(ZSTD(1)),
    SpanKind      Enum8('UNSPECIFIED'=0,'INTERNAL'=1,'SERVER'=2,'CLIENT'=3,'PRODUCER'=4,'CONSUMER'=5),
    StatusCode    Enum8('UNSET'=0,'OK'=1,'ERROR'=2),
    StatusMessage String                               CODEC(ZSTD(1)),
    Duration      UInt64                               CODEC(ZSTD(1)),
    ResourceId    UInt64,
    AttrStr       Map(LowCardinality(String), String)  CODEC(ZSTD(1)),
    AttrNum       Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    AttrBool      Map(LowCardinality(String), UInt8)   CODEC(ZSTD(1)),
    Events        Nested(Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
    Links         Nested(TraceId FixedString(16), SpanId UInt64, TraceState String, Attributes Map(LowCardinality(String), String)),
    INDEX idx_trace_id  TraceId            TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_attr_keys mapKeys(AttrStr)   TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_attr_vals mapValues(AttrStr) TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_duration  Duration           TYPE minmax              GRANULARITY 1
)
ENGINE = ReplicatedMergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL 14 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE tc.trace_id_ts ON CLUSTER tc
(TraceId FixedString(16), Start DateTime64(9), End DateTime64(9),
 INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1)
ENGINE = ReplicatedMergeTree PARTITION BY toDate(Start) ORDER BY (TraceId, toUnixTimestamp(Start))
TTL toDateTime(Start) + INTERVAL 14 DAY;
CREATE MATERIALIZED VIEW tc.trace_id_ts_mv TO tc.trace_id_ts AS
SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End FROM tc.spans GROUP BY TraceId;
```

**Sizing.** Published figures:
- The Jaeger ClickHouse benchmark measured **8.6×** compression, about 75 B/span.
- Coroot measured **5.9×** on `otel_traces`, about 53 B/span. There the hex trace and span ids were the largest compressed columns, a cost that binary ids avoid.

For TCS assume **60–100 B/span** (ASSUMPTION). At M that is 1.728 B spans × 80 B ≈ **138 GB/day**:
- 14 days: 1.9 TB per replica, 3.9 TB with ×2 replicas.
- 30 days: 8.3 TB with ×2 replicas.

By comparison, PostgreSQL needed 13–18 TB per node for 14 days of `span` + `trace_attribute` alone. Under ADR-13's 7-day FULL tier (8 days effective) it needs 7.1–9.0 TB, against ≈ 1.1 TB per ClickHouse replica for the same 8 days.

**Integration.**
- Registered keys can become materialised columns. Bloom filters cover the rest.
- Idempotency relies on Replicated insert deduplication or `insert_deduplication_token` per Kafka batch, or on `ReplacingMergeTree`. These guarantees are eventual or token-based and weaker than `ON CONFLICT`; verify them before relying on them.
- Enrichment, correlation, business operations and configuration stay in PostgreSQL.

### 14.2 Tempo plus PostgreSQL side tables

**How it would work.**
- `tc-ingester` converts each `TraceMessage` to OTLP and pushes it to the Tempo distributor.
- Tempo 3.x in microservices mode ingests through Kafka. Its ingest topic must be **pre-created by admins** (ADR-01).
- Tempo writes vParquet5 blocks to object storage, with a default `block_retention` of 14 days and per-block bloom filters.
- PostgreSQL keeps `trace_summary`, `trace_attribute`, `trace_enrichment`, `trace_correlation`, `business_operation`, `correlation_value`, the dictionaries and the configuration. That is about 2.9 KB/trace, or **≈ 250 GB/day and ≈ 5.6 TB retained at M** with the pre-ADR-13 retentions. Under ADR-13 it is ≈ 2.1 TB of FULL-tier side tables plus the 6.1 TB DIGEST tier, **≈ 8.2 TB**. Moving span bodies to Tempo does not shrink the digest.
- The UI fetches trace bodies from `GET /api/traces/{id}`.

**Pros.** Cheap object storage, TraceQL, and the Grafana ecosystem.

**Cons.**
- Two systems to operate.
- No idempotent upsert: duplicates are merged only at compaction or query time.
- Bytes per span are unknown (UNVERIFIED; must be measured).
- Search returns the first N matches and is non-deterministic.

### 14.3 TimescaleDB (hypercore compression) or Citus

**TimescaleDB.**
- Make `span` and `trace_attribute` hypertables with 1 h chunks, like Promscale did.
- Compress chunks after 2 h with `compress_orderby = 'trace_id, span_id, start_time'`, and use `drop_chunks` for retention.
- Licence: compression is a Timescale License (TSL) feature, not part of the Apache-2.0 edition. *To be verified.*
- TimescaleDB is unavailable on several managed PostgreSQL offerings.
- Compression ratios and `ON CONFLICT` behaviour on compressed chunks are unverified and must be benchmarked.

**Citus.**
- Distribute all trace tables by `trace_id` (co-located), keeping time partitioning within each shard.
- Trace-by-id becomes a single-shard query; search fans out across shards.
- Writes scale horizontally beyond profile L.
- Licence (AGPL-3.0) and PostgreSQL 18 support are *to be verified*.

### 14.4 Comparison and switch criteria

| Criterion | PostgreSQL 18 (chosen) | ClickHouse + PG | Tempo + PG | Timescale | Citus |
|---|---|---|---|---|---|
| Span storage at M, 14 d, per copy (pre-ADR-13) | 13–18 TB | ≈ 2 TB | Object storage (unverified) | Unverified | Same as PG, sharded |
| Span storage at M, ADR-13 FULL tier (7 d + 1 d gate), per copy | 7.1–9.0 TB | ≈ 1.1 TB | Object storage (unverified) | Unverified | Same as PG, sharded |
| Total PostgreSQL per node at M, ADR-13 (n = 7, m = 90, + 25 %) | 17.3–19.7 TB | PG keeps the rest + 6.1 TB digest | ≈ 8.2 TB + 25 % | Unverified | Same total, sharded |
| Idempotent replay | Exact | Token-based or eventual | Merged later | Restricted on compressed chunks | Exact |
| Enrichment and correlation | Native | Second store | Second store | Native | Native |
| Attribute search | Registered keys exact; brute force bounded | Fast full scans | TraceQL | As PG | As PG, parallel |
| Operational complexity | Low | Medium | High | Low–medium | Medium–high |
| Licence | PostgreSQL | Apache-2.0 | AGPL-3.0 (verify) | TSL for compression (verify) | AGPL-3.0 (verify) |

**When to switch span storage** to ClickHouse or Tempo (keeping the control plane, enrichment and correlation in PostgreSQL), if any of these holds:
- sustained load above **profile M**, that is more than about 2 000 traces/s;
- span retention above **30 days** (ADR-13 makes this less likely: long history is served by the digest);
- a projected single-node size above about **40 TB**;
- frequent ad-hoc analytics on non-registered attributes.

Choose Citus instead if PostgreSQL semantics must be kept at profile L.

---

## 15. Deviations from the brief

All deviations are additive or clarifying. No table or column defined in the brief was renamed or removed.

1. **New column `indexed_attribute.backfill_cursor timestamptz NULL`.** It records AttributeBackfill progress (the last hour processed), so `tc-maintenance` can resume after a leader change. It is shared with docs 05 and 06.
2. **Extra indexes.**
   - `correlation_value (operation_id)`, used by the timeline and by merges.
   - `business_operation (operation_definition_id, first_seen DESC)`, for listing operations.
   - `resource (service_id)`.
   - FK-support indexes `enrichment_source (connection_id)` and `correlation_key (key_id)`.
3. **Extra constraints.**
   - `UNIQUE (operation_definition_id, key_id)` on `correlation_key`.
   - A deferrable composite FK from `enrichment_source (id, current_revision)` to `enrichment_source_revision`.
   - CHECK constraints for enums, lengths and singleton tables.
   - A CHECK that forbids inline secrets in `client_properties`.
4. **`COLLATE "C"`** on `trace_attribute.value_str`, `trace_correlation.value`, `correlation_value.value`, `attribute_key.key`, `service.name` and `span_name.name`. It gives byte-order B-tree prefix search.
5. **Index value cap of 512 bytes** on `trace_attribute.value_str` and `trace_correlation.value`, cut on a UTF-8 boundary. It keeps entries well below the B-tree tuple limit and is marked by the `INDEX_VALUE_TRUNCATED` flag. Span attributes keep the 4 KiB limit.
6. **Physical column order** of the data tables was rearranged for alignment. Names and types are unchanged.
7. **`STORAGE MAIN COMPRESSION lz4`** on the variable-length columns of `span`. The brief's `toast_tuple_target = 128` is only safe in combination with it (§4.3).
8. **`business_operation.first_seen` is immutable**, because it is the partition key.
9. **TraceDayResolver probes `d − 1`, `d` and `d + 1`**, where `d` is the UTC date of the fragment's minimum start. The brief said "today/yesterday" relative to the wall clock. The new rule is identical in steady state and also correct during replay.
10. **Roles `tc_owner` and `tc_maintenance` added.** `tc_admin` is the control-plane role.
11. **Pooling.**
    - `tc-ingester` uses the PgBouncer **session** pool `tc_ingest`, with per-connection `CREATE TEMP TABLE IF NOT EXISTS … ON COMMIT DELETE ROWS`. `ON COMMIT DROP` is only a fallback and is not recommended.
    - `tc-maintenance` and `tc-schema-migrator` connect to the primary directly.
12. **Replication.** Patroni `synchronous_mode: true` replaces the "replica same" async assumption, together with the offset-rewind runbook for async failover.
13. **Capacity.** The refined arithmetic gives 13.8 KB/trace (≈ 23.5 TB/node at M) uncompressed, or 11.0 KB/trace (≈ 19.2 TB) with inline lz4. The brief assumed 12.4 KB/trace and about 19 TB. Under ADR-13 (item 15) this becomes 19.7 TB or 17.3 TB, and the recommendation is 20 TB per node at M.
14. **Brute-force window.** The API maximum stays at 1 h. The UI defaults to 15 min at M and 5 min at L, because one hourly partition is about 35 GB at M.
15. **ADR-13 two-tier retention (requested by the user).** This changes the brief's retentions of 14, 30 and 90 days.
    - **FULL tier:** every existing data table is kept n = 7 days (`fullDataDays`, minimum 2).
    - **DIGEST tier:** `trace_digest` and `business_operation_digest` are kept until n + m = 97 days (`digestDays` = 90). `retention_policy` stores the *total* age n + m in DIGEST rows and n in FULL rows.
    - **Changelog `005-two-tier-retention.sql`** (§4.7) adds:
      - columns `indexed_attribute.retain_in_digest` and `digest_transform`;
      - column `business_operation_definition.outcome_rule`;
      - column `retention_policy.tier`;
      - tables `trace_digest`, `business_operation_digest`, `compaction_exclusion` (seeded with `GET /actuator/%`) and `compaction_state`;
      - the function `tc.operation_root(uuid)`.
    - **DigestCompactor** (§6.8), the drop gate (§8.7) and cross-tier reads (§7.7–§7.9) implement it.
    - **Refinements of the ADR-13 addendum made in this document:**
      - Each 1-hour chunk reads retained attribute values with `ts` up to `chunk end + 6 h` (`tc.compaction.attribute-lookahead`) inside the ADR's day window `[D − 1 h, D + 1 day + 6 h)`. This avoids re-reading the whole window per chunk; the drop gate still protects the full day window.
      - `trace_correlation` partitions and `correlation_value` rows are held `max(merge_window) + 1 day` beyond the FULL-tier gate, so step labels and value maps still exist when operations are compacted.
      - `compaction_state` turns `DONE` only after the operations phase.
      - Saving an outcome rule with `outcomeKey` also flags that key `retain_in_digest` with transform NONE, because operation outcomes are evaluated from `trace_digest.attributes`.
      - Equality search on MASK-transformed keys is not supported in the DIGEST tier.
    - **Capacity at M:** 19.7 TB with 25 % headroom (17.3 TB with inline lz4), compared with 23.5 TB before. The provisioning recommendation is 20 TB per node instead of 24 TB (§9.3.2).
