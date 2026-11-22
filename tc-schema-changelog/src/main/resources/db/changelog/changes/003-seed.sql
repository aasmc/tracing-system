--liquibase formatted sql

--changeset tc:003-01-singleton-rows
INSERT INTO tc.config_revision (id, revision) VALUES (1, 0);
INSERT INTO tc.correlation_resolver_state (id, last_processed_id) VALUES (1, 0);

--changeset tc:003-02-retention-policy
--validCheckSum: ANY
-- Retention from the changelog parameters fullDataDays and digestDays; a DIGEST row's retention is the total age
-- (full + digest days). Like every seed that uses parameters, this changeset describes the first installation only:
-- afterwards the control plane owns the rows. Liquibase computes the checksum after substituting the parameters,
-- so it accepts any checksum here; otherwise a value changed after installation would fail every later migration.
INSERT INTO tc.retention_policy (table_name, tier, retention, partition_granularity) VALUES
    ('span',                      'FULL',   make_interval(days => ${fullDataDays}),                 'HOUR'),
    ('trace_attribute',           'FULL',   make_interval(days => ${fullDataDays}),                 'HOUR'),
    ('span_link',                 'FULL',   make_interval(days => ${fullDataDays}),                 'DAY'),
    ('trace_summary',             'FULL',   make_interval(days => ${fullDataDays}),                 'DAY'),
    ('trace_enrichment',          'FULL',   make_interval(days => ${fullDataDays}),                 'DAY'),
    ('trace_correlation',         'FULL',   make_interval(days => ${fullDataDays}),                 'DAY'),
    ('business_operation',        'FULL',   make_interval(days => ${fullDataDays}),                 'MONTH'),
    ('trace_digest',              'DIGEST', make_interval(days => ${fullDataDays} + ${digestDays}), 'DAY'),
    ('business_operation_digest', 'DIGEST', make_interval(days => ${fullDataDays} + ${digestDays}), 'DAY');

--changeset tc:003-03-built-in-indexed-attributes
-- Keys every trace can be searched by: the service name (from the resource), and the root span name and the
-- trace's error status (derived by the ingester).
INSERT INTO tc.attribute_key (key, value_type) VALUES
    ('service.name', 'STRING'), ('span.name', 'STRING'), ('status', 'STRING');
INSERT INTO tc.indexed_attribute (key_id, origin_scope, numeric, status, created_by)
SELECT id, CASE key WHEN 'service.name' THEN 'RESOURCE' ELSE 'SPAN' END, false, 'ACTIVE', 'system'
FROM tc.attribute_key WHERE key IN ('service.name', 'span.name', 'status');

--changeset tc:003-04-initial-compaction-exclusions
--validCheckSum: ANY
-- The changelog parameter initialCompactionExclusions lists SQL LIKE patterns on the root span name, separated by
-- commas, for traces of any service that are never digested (e.g. health probes). The span name depends on the
-- client's instrumentation and on its actuator base path, so nothing is assumed: an empty list adds no exclusion.
-- Exclusions are changed at runtime through the control plane.
INSERT INTO tc.compaction_exclusion (root_name_pattern, description, created_by)
SELECT DISTINCT trim(pattern), 'set at installation', 'system'
FROM unnest(string_to_array($patterns$${initialCompactionExclusions}$patterns$, ',')) AS pattern
WHERE trim(pattern) <> '';
