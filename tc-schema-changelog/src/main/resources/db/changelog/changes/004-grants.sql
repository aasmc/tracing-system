--liquibase formatted sql

-- Privileges of the application roles. The roles themselves, and database-level privileges such as TEMPORARY,
-- belong to infrastructure-as-code. Access through a partitioned table is checked on the parent only, so
-- partitions need no grants, and identity columns need no sequence grants. tc_maintenance acts as tc_owner and
-- needs no grants. This changeset runs again whenever it changes, so new grants are added here.

--changeset tc:004-01-grants runOnChange:true
REVOKE ALL ON SCHEMA tc FROM PUBLIC;
GRANT USAGE ON SCHEMA tc TO tc_ingester, tc_enricher, tc_admin, tc_query;

-- configuration snapshot of the ingester and the enricher
GRANT SELECT ON tc.attribute_key, tc.indexed_attribute, tc.business_operation_definition, tc.correlation_key,
                tc.retention_policy, tc.config_revision, tc.enrichment_source, tc.enrichment_source_revision,
                tc.kafka_connection, tc.compaction_exclusion
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
GRANT SELECT, INSERT, UPDATE, DELETE ON tc.compaction_exclusion TO tc_admin;
GRANT SELECT ON tc.service, tc.span_name, tc.compaction_state TO tc_admin;

-- tc_query: read-only, on the replica; tables created later are readable too
GRANT SELECT ON ALL TABLES IN SCHEMA tc TO tc_query;
ALTER DEFAULT PRIVILEGES FOR ROLE tc_owner IN SCHEMA tc GRANT SELECT ON TABLES TO tc_query;
REVOKE SELECT ON tc.kafka_connection, tc.shedlock FROM tc_query;
GRANT EXECUTE ON FUNCTION tc.operation_root(uuid) TO tc_query;
