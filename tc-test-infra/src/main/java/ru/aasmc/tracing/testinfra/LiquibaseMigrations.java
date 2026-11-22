package ru.aasmc.tracing.testinfra;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.springframework.core.io.DefaultResourceLoader;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Builds the schema exactly as production does:
 *
 * <ol>
 *   <li>runs {@code testinfra/iac-bootstrap.sql} as superuser: roles and the empty schema {@code tc} (owner
 *       {@code tc_owner}), which infrastructure-as-code creates in production;
 *   <li>runs the real changelog {@value #CHANGE_LOG} from {@code tc-schema-changelog} as {@code tc_migrator}, with
 *       the same Hikari {@code connection-init-sql: SET ROLE tc_owner} and the same Liquibase settings as
 *       {@code tc-schema-migrator}'s {@code application.yaml}.
 * </ol>
 *
 * Runs at most once per JVM. Test contexts set {@code spring.liquibase.enabled=false}, as the services do.
 */
public final class LiquibaseMigrations {

    public static final String CHANGE_LOG = "classpath:db/changelog/db.changelog-master.yaml";
    public static final String SCHEMA = "tc";
    public static final String MIGRATOR_USER = "tc_migrator";
    /** Changelog parameters; production values come from {@code tc.retention.*}. */
    public static final Map<String, String> PARAMETERS = Map.of("fullDays", "7", "digestDays", "90");

    private static final String BOOTSTRAP_SCRIPT = "testinfra/iac-bootstrap.sql";

    private static boolean applied;

    private LiquibaseMigrations() {}

    static synchronized void applyOnce(PostgreSQLContainer postgres) {
        if (applied) {
            return;
        }
        runBootstrap(postgres);
        runLiquibase(postgres);
        applied = true;
    }

    /** Password of a test role created by the bootstrap script (role name = password). */
    public static String passwordOf(String role) {
        return role;
    }

    private static void runBootstrap(PostgreSQLContainer postgres) {
        String sql = readClasspath(BOOTSTRAP_SCRIPT);
        try (Connection connection = DriverManager.getConnection(
                        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql); // PgJDBC runs a multi-statement string in one simple-query round trip
        } catch (SQLException e) {
            throw new IllegalStateException("Infrastructure bootstrap failed", e);
        }
    }

    private static void runLiquibase(PostgreSQLContainer postgres) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(MIGRATOR_USER);
        config.setPassword(passwordOf(MIGRATOR_USER));
        config.setMaximumPoolSize(2);
        config.setConnectionInitSql("SET ROLE tc_owner");
        config.setPoolName("tc-test-migrator");
        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            SpringLiquibase liquibase = new SpringLiquibase();
            liquibase.setDataSource(dataSource);
            liquibase.setResourceLoader(new DefaultResourceLoader(LiquibaseMigrations.class.getClassLoader()));
            liquibase.setChangeLog(CHANGE_LOG);
            liquibase.setDefaultSchema(SCHEMA);
            liquibase.setLiquibaseSchema(SCHEMA);
            liquibase.setChangeLogParameters(PARAMETERS);
            liquibase.setAnalyticsEnabled(false);
            liquibase.afterPropertiesSet();
        } catch (LiquibaseException e) {
            throw new IllegalStateException("Liquibase migration failed", e);
        }
    }

    private static String readClasspath(String path) {
        try (InputStream in = LiquibaseMigrations.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
