package ru.aasmc.tracing.collector.migrator;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.migrator.SqlStates.PERMISSION_DENIED;
import static ru.aasmc.tracing.collector.migrator.SqlStates.sqlStateOf;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.testinfra.JdbcProbe;
import ru.aasmc.tracing.testinfra.TestIds;

/**
 * Each application role may do exactly what its service needs: the table privileges are compared with the full
 * expected list per role, and a few statements are run while logged in as the role to show the effect.
 */
@MigratorIT
class RolesAndGrantsIT {

    private static final Set<String> CONFIGURATION_SNAPSHOT = Set.of(
            "attribute_key",
            "business_operation_definition",
            "compaction_exclusion",
            "config_revision",
            "correlation_key",
            "enrichment_source",
            "enrichment_source_revision",
            "indexed_attribute",
            "kafka_connection",
            "retention_policy");

    private final JdbcProbe db = JdbcProbe.superuser();

    @Test
    @DisplayName("""
            tc_ingester reads the configuration, writes dictionaries, spans and their side tables,
            and may update only trace summaries (and single columns of attribute_key and resource)""")
    void given_ingesterRole_when_privilegesListed_then_exactlyIngestPrivileges() {
        assertThat(tablePrivileges("tc_ingester"))
                .isEqualTo(Map.of(
                        "SELECT",
                        union(
                                CONFIGURATION_SNAPSHOT,
                                Set.of(
                                        "instrumentation_scope",
                                        "resource",
                                        "service",
                                        "span",
                                        "span_link",
                                        "span_name",
                                        "trace_attribute",
                                        "trace_correlation",
                                        "trace_summary")),
                        "INSERT",
                        Set.of(
                                "attribute_key",
                                "instrumentation_scope",
                                "resource",
                                "service",
                                "span",
                                "span_link",
                                "span_name",
                                "trace_attribute",
                                "trace_correlation",
                                "trace_summary"),
                        "UPDATE",
                        Set.of("trace_summary")));
        assertThat(canUpdateColumn("tc_ingester", "attribute_key", "value_type"))
                .isTrue();
        assertThat(canUpdateColumn("tc_ingester", "attribute_key", "key")).isFalse();
        assertThat(canUpdateColumn("tc_ingester", "resource", "last_seen")).isTrue();
        assertThat(canUpdateColumn("tc_ingester", "resource", "attributes")).isFalse();
    }

    @Test
    @DisplayName("""
            tc_enricher reads the configuration and only appends enrichments
            and their search and correlation rows""")
    void given_enricherRole_when_privilegesListed_then_exactlyEnrichmentPrivileges() {
        assertThat(tablePrivileges("tc_enricher"))
                .isEqualTo(Map.of(
                        "SELECT",
                        union(
                                CONFIGURATION_SNAPSHOT,
                                Set.of("trace_attribute", "trace_correlation", "trace_enrichment")),
                        "INSERT",
                        Set.of("trace_attribute", "trace_correlation", "trace_enrichment")));
    }

    @Test
    @DisplayName("""
            tc_admin edits the configuration, never changes a stored source revision,
            deletes only correlation keys and compaction exclusions, and reads service names and compaction progress""")
    void given_adminRole_when_privilegesListed_then_exactlyConfigurationPrivileges() {
        Set<String> editable = Set.of(
                "attribute_key",
                "business_operation_definition",
                "compaction_exclusion",
                "config_revision",
                "correlation_key",
                "enrichment_source",
                "indexed_attribute",
                "kafka_connection",
                "retention_policy");

        assertThat(tablePrivileges("tc_admin"))
                .isEqualTo(Map.of(
                        "SELECT",
                        union(
                                editable,
                                Set.of("compaction_state", "enrichment_source_revision", "service", "span_name")),
                        "INSERT",
                        union(editable, Set.of("enrichment_source_revision")),
                        "UPDATE",
                        editable,
                        "DELETE",
                        Set.of("compaction_exclusion", "correlation_key")));
    }

    @Test
    @DisplayName("""
            tc_query reads every table except the Kafka connections (they name secrets) and the scheduler locks,
            and writes nothing""")
    void given_queryRole_when_privilegesListed_then_readOnlyWithoutConnectionsAndLocks() {
        Set<String> readable = new TreeSet<>(tables());
        readable.removeAll(Set.of("kafka_connection", "shedlock"));

        assertThat(tablePrivileges("tc_query")).isEqualTo(Map.of("SELECT", readable));
    }

    @Test
    @DisplayName("""
            a table created later by tc_owner is readable by tc_query without a new grant""")
    void given_newTableOfTcOwner_when_created_then_readableByQueryRole() {
        String table = "grants_probe_" + UUID.randomUUID().toString().replace("-", "");
        db.jdbc().execute("SET ROLE tc_owner; CREATE TABLE tc." + table + " (id int); RESET ROLE");
        try {
            assertThat(db.scalar(Boolean.class, "SELECT has_table_privilege('tc_query', ?, 'SELECT')", "tc." + table))
                    .isTrue();
        } finally {
            db.jdbc().execute("DROP TABLE tc." + table);
        }
    }

    @Test
    @DisplayName("""
            logged in as the roles: tc_query cannot insert, tc_ingester cannot drop a table,
            and no role outside the grants can use schema tc""")
    void given_roleLogins_when_forbiddenStatementsRun_then_permissionDenied() {
        assertThat(sqlStateOf(() -> JdbcProbe.loggedInAs("tc_query")
                        .jdbc()
                        .update("INSERT INTO tc.service (name) VALUES (?)", TestIds.name("svc"))))
                .isEqualTo(PERMISSION_DENIED);
        assertThat(sqlStateOf(() -> JdbcProbe.loggedInAs("tc_ingester").jdbc().execute("DROP TABLE tc.span")))
                .isEqualTo(PERMISSION_DENIED);
        assertThat(sqlStateOf(() ->
                        JdbcProbe.loggedInAs("tc_query").jdbc().queryForList("SELECT name FROM tc.kafka_connection")))
                .isEqualTo(PERMISSION_DENIED);
        assertThat(db.scalar(Boolean.class, "SELECT has_schema_privilege('public', 'tc', 'USAGE')"))
                .isFalse();
    }

    @Test
    @DisplayName("""
            logged in as the roles: tc_ingester inserts into an identity column without a sequence grant
            and creates its session staging tables, and tc_query calls tc.operation_root""")
    void given_roleLogins_when_allowedStatementsRun_then_succeed() {
        JdbcProbe ingester = JdbcProbe.loggedInAs("tc_ingester");
        String key = TestIds.name("key");

        ingester.jdbc().update("INSERT INTO tc.attribute_key (key, value_type) VALUES (?, 'STRING')", key);
        ingester.jdbc().execute("CREATE TEMP TABLE IF NOT EXISTS staging_probe (id int) ON COMMIT DELETE ROWS");

        assertThat(db.scalar(Integer.class, "SELECT count(*) FROM tc.attribute_key WHERE key = ?", key))
                .isEqualTo(1);
        UUID unknown = UUID.randomUUID();
        assertThat(JdbcProbe.loggedInAs("tc_query").scalar(UUID.class, "SELECT tc.operation_root(?)", unknown))
                .isEqualTo(unknown);
    }

    /** Privilege → tables of schema {@code tc} on which {@code role} holds it for the whole table. */
    private Map<String, Set<String>> tablePrivileges(String role) {
        Map<String, Set<String>> privileges = new TreeMap<>();
        for (String privilege : List.of("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")) {
            for (String table : tables()) {
                if (db.scalar(Boolean.class, "SELECT has_table_privilege(?, ?, ?)", role, "tc." + table, privilege)) {
                    privileges.computeIfAbsent(privilege, p -> new TreeSet<>()).add(table);
                }
            }
        }
        return privileges;
    }

    private List<String> tables() {
        return db.jdbc().queryForList("""
                SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'tc' AND c.relkind IN ('r', 'p') AND NOT c.relispartition
                ORDER BY c.relname
                """, String.class);
    }

    private boolean canUpdateColumn(String role, String table, String column) {
        return db.scalar(Boolean.class, "SELECT has_column_privilege(?, ?, ?, 'UPDATE')", role, "tc." + table, column);
    }

    private static Set<String> union(Set<String> first, Set<String> second) {
        Set<String> union = new TreeSet<>(first);
        union.addAll(second);
        return union;
    }
}
