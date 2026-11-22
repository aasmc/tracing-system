package ru.aasmc.tracing.collector.migrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.aasmc.tracing.collector.migrator.SqlStates.CHECK_VIOLATION;
import static ru.aasmc.tracing.collector.migrator.SqlStates.sqlStateOf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.liquibase.autoconfigure.LiquibaseProperties;
import org.springframework.core.io.DefaultResourceLoader;
import ru.aasmc.tracing.testinfra.JdbcProbe;
import ru.aasmc.tracing.testinfra.TestIds;
import ru.aasmc.tracing.testinfra.TestPartitions;

/**
 * The changelogs build schema {@code tc} on an empty PostgreSQL in which infrastructure-as-code created only the
 * roles and the empty schema. The context runs the migrator's real Liquibase configuration (logged in as
 * {@code tc_migrator}, acting as {@code tc_owner}); assertions run on an independent superuser connection.
 */
@MigratorIT
class MigrationsIT {

    private final JdbcProbe db = JdbcProbe.superuser();

    @Autowired
    SpringLiquibase liquibase;

    @Autowired
    DataSource migratorDataSource;

    @Autowired
    LiquibaseProperties liquibaseProperties;

    @Test
    @DisplayName("""
            the migrator passes the retention and the initial compaction exclusions to Liquibase as changelog
            parameters, by default no exclusion; it starts although the schema was installed with other exclusions,
            because seeds that use parameters accept any checksum""")
    void given_migratorConfiguration_when_bound_then_changelogParameters() {
        assertThat(liquibaseProperties.getParameters())
                .isEqualTo(Map.of("fullDataDays", "7", "digestDays", "90", "initialCompactionExclusions", ""));
    }

    @Test
    @DisplayName("""
            every changeset of the master changelog is applied once, in order""")
    void given_emptySchema_when_migrated_then_everyChangesetApplied() {
        List<String> applied = db.jdbc()
                .queryForList(
                        "SELECT id FROM tc.databasechangelog WHERE filename LIKE 'db/changelog/%' ORDER BY orderexecuted",
                        String.class);

        assertThat(applied)
                .containsExactly(
                        "001-01-kafka-connections-and-enrichment-sources",
                        "001-02-attribute-keys",
                        "001-03-business-operation-definitions",
                        "001-04-retention-and-compaction-configuration",
                        "001-05-services-span-names-resources-scopes",
                        "001-06-correlation-value",
                        "001-07-operational-state",
                        "001-20-try-float8",
                        "002-01-span",
                        "002-02-trace-summary",
                        "002-03-trace-attribute",
                        "002-04-span-link",
                        "002-05-trace-enrichment",
                        "002-06-trace-correlation",
                        "002-07-business-operation",
                        "002-08-trace-digest",
                        "002-09-business-operation-digest",
                        "002-20-operation-root",
                        "003-01-singleton-rows",
                        "003-02-retention-policy",
                        "003-03-built-in-indexed-attributes",
                        "003-04-initial-compaction-exclusions",
                        "004-01-grants");
    }

    @Test
    @DisplayName("""
            running the migration again on a migrated schema
            changes nothing""")
    void given_migratedSchema_when_migratedAgain_then_nothingExecuted() throws LiquibaseException {
        List<Map<String, Object>> before = changelogRows();

        liquibase.afterPropertiesSet();

        assertThat(changelogRows()).isEqualTo(before);
    }

    @Test
    @DisplayName("""
            every table, index, sequence and function of schema tc,
            including Liquibase's own tables, is owned by tc_owner""")
    void given_migratedSchema_when_ownersListed_then_allOwnedByTcOwner() {
        assertThat(db.rows("""
                        SELECT c.relname, c.relowner::regrole::text AS owner
                        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'tc' AND c.relowner <> 'tc_owner'::regrole
                        """)).isEmpty();
        assertThat(db.rows("""
                        SELECT p.proname, p.proowner::regrole::text AS owner
                        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                        WHERE n.nspname = 'tc' AND p.proowner <> 'tc_owner'::regrole
                        """)).isEmpty();
        assertThat(db.tableOwner("tc", "databasechangelog")).isEqualTo("tc_owner");
    }

    @Test
    @DisplayName("""
            a changeset that was edited after it was applied
            fails Liquibase's checksum validation""")
    void given_appliedChangeset_when_editedAndMigrated_then_validationFails() throws LiquibaseException {
        migration("classpath:db/checksum/applied.sql").afterPropertiesSet();

        assertThatThrownBy(() -> migration("classpath:db/checksum/edited.sql").afterPropertiesSet())
                .isInstanceOf(LiquibaseException.class)
                .hasMessageContaining("db/checksum/changelog.sql::checksum-probe::tc");
    }

    @Test
    @DisplayName("""
            the tables and their columns, types and NOT NULL constraints
            are exactly the reviewed list in golden/tc-columns.txt""")
    void given_migratedSchema_when_columnsListed_then_matchGoldenList() throws IOException {
        List<String> columns = db.jdbc().queryForList("""
                SELECT c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
                       || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'tc' AND c.relkind IN ('r', 'p') AND NOT c.relispartition
                  AND c.relname NOT LIKE 'databasechangelog%' AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY c.relname, a.attnum
                """, String.class);

        assertThat(columns).containsExactlyElementsOf(golden("golden/tc-columns.txt"));
    }

    @Test
    @DisplayName("""
            the data tables are range-partitioned on their time column
            and none has a DEFAULT partition, so a row without a partition is rejected""")
    void given_migratedSchema_when_partitionedTablesListed_then_rangeKeysAndNoDefaultPartition() {
        assertThat(db.rows("""
                        SELECT c.relname AS table, pg_get_partkeydef(c.oid) AS key, p.partdefid <> 0 AS has_default
                        FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'tc' AND NOT c.relispartition
                        ORDER BY c.relname
                        """))
                .containsExactly(
                        partitioned("business_operation", "RANGE (first_seen)"),
                        partitioned("business_operation_digest", "RANGE (op_day)"),
                        partitioned("span", "RANGE (start_time)"),
                        partitioned("span_link", "RANGE (start_time)"),
                        partitioned("trace_attribute", "RANGE (ts)"),
                        partitioned("trace_correlation", "RANGE (observed_at)"),
                        partitioned("trace_digest", "RANGE (trace_day)"),
                        partitioned("trace_enrichment", "RANGE (observed_at)"),
                        partitioned("trace_summary", "RANGE (trace_day)"));
    }

    @Test
    @DisplayName("""
            every variable-length column of span is compressed inline with lz4 (STORAGE MAIN),
            so partitions created LIKE span inherit it""")
    void given_spanTable_when_columnStorageRead_then_mainAndLz4() {
        assertThat(db.rows("""
                        SELECT attname AS column, attstorage AS storage, attcompression AS compression
                        FROM pg_attribute
                        WHERE attrelid = 'tc.span'::regclass AND attnum > 0 AND attlen = -1
                        ORDER BY attnum
                        """))
                .extracting(row -> row.get("column") + " " + row.get("storage") + " " + row.get("compression"))
                .containsExactly(
                        "name_overflow m l",
                        "status_message m l",
                        "trace_state m l",
                        "dropped_counts m l",
                        "attributes m l",
                        "events m l",
                        "links m l");
    }

    @Test
    @DisplayName("""
            the seed sets the singleton rows and the built-in indexed keys, and from the changelog parameters
            the retention of both tiers (7 full and 90 digest days) and the initial compaction exclusions,
            each pattern once and without blanks""")
    void given_migratedSchema_when_seedRead_then_defaults() {
        assertThat(db.rows("SELECT id, revision FROM tc.config_revision"))
                .containsExactly(Map.of("id", 1, "revision", 0L));
        assertThat(db.rows("SELECT id, last_processed_id FROM tc.correlation_resolver_state"))
                .containsExactly(Map.of("id", 1, "last_processed_id", 0L));
        assertThat(db.rows("""
                        SELECT table_name, tier, extract(day FROM retention)::int AS days, partition_granularity
                        FROM tc.retention_policy ORDER BY table_name
                        """))
                .extracting(row -> row.get("table_name") + " " + row.get("tier") + " " + row.get("days") + " "
                        + row.get("partition_granularity"))
                .containsExactly(
                        "business_operation FULL 7 MONTH",
                        "business_operation_digest DIGEST 97 DAY",
                        "span FULL 7 HOUR",
                        "span_link FULL 7 DAY",
                        "trace_attribute FULL 7 HOUR",
                        "trace_correlation FULL 7 DAY",
                        "trace_digest DIGEST 97 DAY",
                        "trace_enrichment FULL 7 DAY",
                        "trace_summary FULL 7 DAY");
        assertThat(db.rows("""
                        SELECT k.key, i.origin_scope, i.status
                        FROM tc.indexed_attribute i JOIN tc.attribute_key k ON k.id = i.key_id
                        WHERE i.created_by = 'system' ORDER BY k.key
                        """))
                .containsExactly(
                        Map.of("key", "service.name", "origin_scope", "RESOURCE", "status", "ACTIVE"),
                        Map.of("key", "span.name", "origin_scope", "SPAN", "status", "ACTIVE"),
                        Map.of("key", "status", "origin_scope", "SPAN", "status", "ACTIVE"));
        assertThat(db.jdbc()
                        .queryForList(
                                "SELECT root_name_pattern FROM tc.compaction_exclusion WHERE created_by = 'system'",
                                String.class))
                .containsExactlyInAnyOrder("http get /actuator/%", "GET /internal/health");
    }

    @Test
    @DisplayName("""
            the JSON checks reject a Kafka connection with a secret among its client properties,
            a source revision without a trace context, and an outcome rule field of the wrong type""")
    void given_invalidJsonConfiguration_when_inserted_then_checkViolation() {
        assertThat(insertConnection("{\"client.id\":\"tc\"}")).isEqualTo(1);
        assertThat(sqlStateOf(() -> insertConnection("{\"sasl.jaas.config\":\"secret\"}")))
                .isEqualTo(CHECK_VIOLATION);

        assertThat(sqlStateOf(() -> db.jdbc().update("""
                        INSERT INTO tc.enrichment_source_revision (source_id, revision, definition, created_by)
                        VALUES (uuidv7(), 1, '{"fieldMappings":[]}', 'test')
                        """))).isEqualTo(CHECK_VIOLATION);

        assertThat(insertDefinition(null)).isEqualTo(1);
        assertThat(insertDefinition("{}")).isEqualTo(1);
        assertThat(insertDefinition("{\"outcomeKey\":\"business.outcome\",\"successValues\":[\"OK\"]}"))
                .isEqualTo(1);
        assertThat(sqlStateOf(() -> insertDefinition("{\"successValues\":\"OK\"}")))
                .isEqualTo(CHECK_VIOLATION);
        assertThat(sqlStateOf(() -> insertDefinition("[]"))).isEqualTo(CHECK_VIOLATION);
    }

    @Test
    @DisplayName("""
            the configuration tables edited by admins have UUID ids that default to a new version-7 UUID,
            so a row inserted without an id (like the seeded compaction exclusion) still gets one""")
    void given_configurationTables_when_idsInspected_then_uuidV7ByDefault() {
        assertThat(db.rows("""
                        SELECT table_name, data_type, column_default FROM information_schema.columns
                        WHERE table_schema = 'tc' AND column_name = 'id' AND column_default = 'uuidv7()'
                          AND table_name NOT IN (SELECT relname FROM pg_class WHERE relispartition)
                        ORDER BY table_name
                        """))
                .extracting(row -> row.get("table_name") + " " + row.get("data_type"))
                .containsExactly(
                        "business_operation uuid",
                        "business_operation_definition uuid",
                        "compaction_exclusion uuid",
                        "correlation_key uuid",
                        "enrichment_source uuid",
                        "kafka_connection uuid");
        assertThat(
                        db.scalar(
                                Integer.class,
                                "SELECT DISTINCT uuid_extract_version(id) FROM tc.compaction_exclusion WHERE created_by = 'system'"))
                .isEqualTo(7);
    }

    @Test
    @DisplayName("""
            tc.try_float8 reads a number from text,
            and returns NULL instead of failing for text that is not a float8""")
    void given_texts_when_tryFloat8_then_numberOrNull() {
        assertThat(db.scalar(Double.class, "SELECT tc.try_float8('1.5')")).isEqualTo(1.5);
        assertThat(db.scalar(Double.class, "SELECT tc.try_float8('-2e3')")).isEqualTo(-2000.0);
        assertThat(db.scalar(Double.class, "SELECT tc.try_float8('abc')")).isNull();
        assertThat(db.scalar(Double.class, "SELECT tc.try_float8('1e999')")).isNull();
    }

    @Test
    @DisplayName("""
            tc.operation_root follows merged_into to the operation that was not merged,
            and returns the given id for an operation that is not stored""")
    void given_mergeChain_when_operationRoot_then_survivor() {
        TestPartitions.onTcPostgres()
                .ensureMonthly("business_operation", YearMonth.of(2026, 11), YearMonth.of(2026, 12));
        UUID survivor = UUID.randomUUID();
        UUID merged = UUID.randomUUID();
        UUID mergedIntoMerged = UUID.randomUUID();
        insertOperation(survivor, null);
        insertOperation(merged, survivor);
        insertOperation(mergedIntoMerged, merged);
        UUID unknown = UUID.randomUUID();

        assertThat(operationRoot(survivor)).isEqualTo(survivor);
        assertThat(operationRoot(merged)).isEqualTo(survivor);
        assertThat(operationRoot(mergedIntoMerged)).isEqualTo(survivor);
        assertThat(operationRoot(unknown)).isEqualTo(unknown);
    }

    private List<Map<String, Object>> changelogRows() {
        return db.rows(
                "SELECT id, filename, md5sum, orderexecuted, dateexecuted FROM tc.databasechangelog ORDER BY id");
    }

    private int insertConnection(String clientProperties) {
        return db.jdbc().update("""
                INSERT INTO tc.kafka_connection (name, bootstrap_servers, security_protocol, client_properties)
                VALUES (?, 'kafka:9092', 'PLAINTEXT', ?::jsonb)
                """, TestIds.name("connection"), clientProperties);
    }

    private int insertDefinition(String outcomeRule) {
        return db.jdbc().update("""
                INSERT INTO tc.business_operation_definition (name, outcome_rule) VALUES (?, ?::jsonb)
                """, TestIds.name("definition"), outcomeRule);
    }

    private SpringLiquibase migration(String changeLog) {
        SpringLiquibase migration = new SpringLiquibase();
        migration.setDataSource(migratorDataSource);
        migration.setResourceLoader(new DefaultResourceLoader());
        migration.setChangeLog(changeLog);
        migration.setDefaultSchema("tc");
        migration.setLiquibaseSchema("tc");
        migration.setAnalyticsEnabled(false);
        return migration;
    }

    private static Map<String, Object> partitioned(String table, String key) {
        return Map.of("table", table, "key", key, "has_default", false);
    }

    private void insertOperation(UUID id, UUID mergedInto) {
        db.jdbc().update("""
                INSERT INTO tc.business_operation (id, first_seen, last_seen, merged_into, operation_definition_id,
                                                   display_value)
                VALUES (?, '2026-11-21T10:00:00Z', '2026-11-21T10:00:00Z', ?, uuidv7(), 'value')
                """, id, mergedInto);
    }

    private UUID operationRoot(UUID id) {
        return db.scalar(UUID.class, "SELECT tc.operation_root(?)", id);
    }

    private static List<String> golden(String resource) throws IOException {
        try (InputStream in = MigrationsIT.class.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as(resource).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
        }
    }
}
