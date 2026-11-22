package ru.aasmc.tracing.testinfra.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.aasmc.tracing.testinfra.JdbcProbe;
import ru.aasmc.tracing.testinfra.LiquibaseMigrations;
import ru.aasmc.tracing.testinfra.TcContainers;

@InfraIT
class TcContainersIT {

    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    @DisplayName("0-T1 the context's DataSource is connected to the PostgreSQL 18 container via @ServiceConnection")
    void given_context_when_queryingServerVersion_then_postgres18() throws Exception {
        try (var connection = dataSource.getConnection()) {
            assertThat(connection.getMetaData().getURL())
                    .isEqualTo(TcContainers.postgres().getJdbcUrl());
        }
        assertThat(jdbc.queryForObject("SHOW server_version", String.class)).startsWith("18.");
        assertThat(jdbc.queryForObject("SELECT current_database()", String.class))
                .isEqualTo(TcContainers.DATABASE);
    }

    @Test
    @DisplayName("0-T1 uuidv7() exists and returns version-7 UUIDs")
    void given_postgres18_when_callingUuidv7_then_version7() {
        UUID id = jdbc.queryForObject("SELECT uuidv7()", UUID.class);

        assertThat(id).isNotNull();
        assertThat(id.version()).isEqualTo(7);
    }

    @Test
    @DisplayName("0-T1 schema tc and the Liquibase tracking tables exist and are owned by tc_owner")
    void given_liquibaseMigrations_when_inspectingCatalog_then_trackingTablesOwnedByTcOwner() {
        JdbcProbe probe = JdbcProbe.superuser();

        assertThat(probe.scalar(String.class, "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'tc'"))
                .isEqualTo("tc_owner");
        assertThat(probe.tableOwner(LiquibaseMigrations.SCHEMA, "databasechangelog"))
                .isEqualTo("tc_owner");
        assertThat(probe.tableOwner(LiquibaseMigrations.SCHEMA, "databasechangeloglock"))
                .isEqualTo("tc_owner");
        assertThat(probe.tableOwner("public", "databasechangelog")).isNull();
    }

    @Test
    @DisplayName("0-T1 the infrastructure roles exist; tc_migrator and tc_maintenance are members of tc_owner")
    void given_iacBootstrap_when_inspectingRoles_then_rolesAsIn04Section13() {
        JdbcProbe probe = JdbcProbe.superuser();

        assertThat(probe.jdbc()
                        .queryForList(
                                "SELECT rolname FROM pg_roles WHERE rolname LIKE 'tc\\_%' ORDER BY rolname",
                                String.class))
                .containsExactly(
                        "tc_admin",
                        "tc_enricher",
                        "tc_ingester",
                        "tc_maintenance",
                        "tc_migrator",
                        "tc_owner",
                        "tc_query");
        assertThat(probe.scalar(Boolean.class, "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'tc_owner'"))
                .isFalse();
        assertThat(probe.jdbc().queryForList("""
                        SELECT m.rolname FROM pg_auth_members am
                        JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles m ON m.oid = am.member
                        WHERE r.rolname = 'tc_owner' ORDER BY m.rolname
                        """, String.class)).containsExactly("tc_maintenance", "tc_migrator");
    }
}
