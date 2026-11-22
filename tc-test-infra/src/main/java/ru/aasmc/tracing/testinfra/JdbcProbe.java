package ru.aasmc.tracing.testinfra;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Read-mostly helper queries for assertions (09 §1.3), executed on a superuser connection that is independent of
 * the application's {@code DataSource}. Sessions run with {@code TimeZone = UTC}, so rendered timestamps
 * ({@code pg_get_expr} of partition bounds, {@code ::date} casts) do not depend on the JVM's default zone. Domain-specific helpers ({@code countSpans(traceId)},
 * {@code summary(traceId)}) are added in Phase 2, once the tables exist.
 */
public final class JdbcProbe {

    private final JdbcTemplate jdbc;

    private JdbcProbe(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public static JdbcProbe superuser() {
        return Holder.INSTANCE;
    }

    public JdbcTemplate jdbc() {
        return jdbc;
    }

    public DataSource dataSource() {
        return jdbc.getDataSource();
    }

    public <T> T scalar(Class<T> type, String sql, Object... args) {
        return jdbc.queryForObject(sql, type, args);
    }

    public List<Map<String, Object>> rows(String sql, Object... args) {
        return jdbc.queryForList(sql, args);
    }

    /** {@code SHOW server_version}, e.g. {@code 18.7 (Debian 18.7-1.pgdg13+1)}. */
    public String serverVersion() {
        return scalar(String.class, "SHOW server_version");
    }

    /** Owner role of {@code schema.table}, or {@code null} if the table does not exist. */
    public String tableOwner(String schema, String table) {
        List<String> owners = jdbc.queryForList(
                "SELECT tableowner FROM pg_tables WHERE schemaname = ? AND tablename = ?", String.class, schema, table);
        return owners.isEmpty() ? null : owners.getFirst();
    }

    /** Text plan of {@code EXPLAIN (COSTS OFF) <sql>}, one line per plan row. */
    public String explain(String sql, Object... args) {
        return jdbc.queryForList("EXPLAIN (COSTS OFF) " + sql, String.class, args).stream()
                .collect(Collectors.joining("\n"));
    }

    private static final class Holder {
        static final JdbcProbe INSTANCE = new JdbcProbe(dataSource(TcContainers.postgres()));

        private static DataSource dataSource(PostgreSQLContainer postgres) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
            config.setMaximumPoolSize(4);
            config.setPoolName("tc-test-probe");
            // PgJDBC sets the session TimeZone to the JVM default zone; TCS reasons in UTC (04 §8.1)
            config.setConnectionInitSql("SET TIME ZONE 'UTC'");
            HikariDataSource dataSource = new HikariDataSource(config);
            Runtime.getRuntime().addShutdownHook(new Thread(dataSource::close, "tc-test-probe-close"));
            return dataSource;
        }
    }
}
