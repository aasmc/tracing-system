package ru.aasmc.tracing.collector.migrator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.liquibase.autoconfigure.LiquibaseProperties;
import ru.aasmc.tracing.testinfra.JdbcProbe;

/**
 * Checks the Liquibase conventions the changelogs rely on (object ownership, functions, concurrent indexes,
 * changelog parameters, grants), running through Spring Boot's auto-configuration with the migrator's real
 * {@code application.yaml}. Assertions run on an independent superuser connection ({@link JdbcProbe}).
 */
@MigratorIT
class LiquibaseSpikeIT {

    private final JdbcProbe db = JdbcProbe.superuser();

    @Autowired
    LiquibaseProperties liquibase;

    @Test
    @DisplayName("the spring.liquibase.* properties bind")
    void given_migratorYaml_when_bound_then_liquibasePropertiesAsDesigned() {
        assertThat(liquibase.isEnabled()).isTrue();
        assertThat(liquibase.getDefaultSchema()).isEqualTo("tc");
        assertThat(liquibase.getLiquibaseSchema()).isEqualTo("tc");
        assertThat(liquibase.getParameters())
                .containsExactlyInAnyOrderEntriesOf(Map.of("fullDays", "7", "digestDays", "90"));
        assertThat(liquibase.getAnalyticsEnabled()).isFalse();
    }

    @Test
    @DisplayName("Hikari connection-init-sql SET ROLE tc_owner applies to Liquibase's connection")
    void given_migratorLogsInAsTcMigrator_when_changesetCreatesTable_then_ownedByTcOwner() {
        assertThat(db.tableOwner("tc", "spike_owned")).isEqualTo("tc_owner");
        assertThat(db.tableOwner("tc", "spike_param")).isEqualTo("tc_owner");
        assertThat(db.scalar(
                        String.class,
                        "SELECT proowner::regrole::text FROM pg_proc WHERE oid = 'tc.spike_fn()'::regprocedure"))
                .isEqualTo("tc_owner");
        assertThat(db.rows("SELECT DISTINCT author FROM tc.databasechangelog WHERE id LIKE 'spike-%'"))
                .containsExactly(Map.of("author", "tc"));
    }

    @Test
    @DisplayName("splitStatements:false keeps a $$ function body intact")
    void given_splitStatementsFalse_when_applied_then_functionWorks() {
        assertThat(db.scalar(Integer.class, "SELECT tc.spike_fn()")).isEqualTo(42);
    }

    @Test
    @DisplayName("runInTransaction:false allows CREATE INDEX CONCURRENTLY; the index is valid")
    void given_runInTransactionFalse_when_applied_then_concurrentIndexValid() {
        assertThat(db.scalar(
                        Boolean.class,
                        "SELECT indisvalid FROM pg_index WHERE indexrelid = 'tc.spike_owned_id_idx'::regclass"))
                .isTrue();
    }

    @Test
    @DisplayName("${fullDays}/${digestDays} are substituted from spring.liquibase.parameters.*")
    void given_changelogParameters_when_applied_then_substituted() {
        assertThat(db.rows("SELECT full_days, digest_days FROM tc.spike_param"))
                .containsExactly(Map.of("full_days", 7, "digest_days", 90));
    }

    @Test
    @DisplayName("changesets are recorded in tc.databasechangelog, which the SchemaVersionGuard query reads")
    void given_appliedChangesets_when_queryingLikeSchemaVersionGuard_then_found() {
        List<String> ids = db.jdbc()
                .queryForList(
                        "SELECT id FROM tc.databasechangelog WHERE author = 'tc' AND id LIKE 'spike-%' ORDER BY orderexecuted",
                        String.class);

        assertThat(ids)
                .containsExactly(
                        "spike-01-ownership",
                        "spike-02-function",
                        "spike-03-concurrent-index",
                        "spike-04-parameters",
                        "spike-05-grants");
        assertThat(db.scalar(
                        Integer.class,
                        "SELECT count(*) FROM tc.databasechangelog WHERE author = 'tc' AND id = ?",
                        "spike-05-grants"))
                .isEqualTo(1);
        assertThat(db.scalar(Boolean.class, "SELECT has_table_privilege('tc_query', 'tc.spike_owned', 'SELECT')"))
                .isTrue();
    }
}
