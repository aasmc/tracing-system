package ru.aasmc.tracing.testinfra;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Helper queries for assertions, on connections that are independent of the application's {@code DataSource}:
 * as the container superuser, or logged in as one of the database roles to check what that role may do. Sessions
 * run with {@code TimeZone = UTC}, so rendered timestamps ({@code pg_get_expr} of partition bounds, {@code ::date}
 * casts) do not depend on the JVM's default zone.
 */
public final class JdbcProbe {

    private static final Map<String, JdbcProbe> ROLE_PROBES = new ConcurrentHashMap<>();

    private final JdbcTemplate jdbc;

    private JdbcProbe(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public static JdbcProbe superuser() {
        return Holder.INSTANCE;
    }

    /** Logged in as {@code role}, one of the roles that the infrastructure bootstrap creates (e.g. {@code tc_query}). */
    public static JdbcProbe loggedInAs(String role) {
        return ROLE_PROBES.computeIfAbsent(
                role, name -> new JdbcProbe(dataSource(name, LiquibaseMigrations.passwordOf(name))));
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

    private static DataSource dataSource(String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(TcContainers.postgres().getJdbcUrl());
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(4);
        config.setPoolName("tc-test-probe-" + user);
        // PgJDBC sets the session TimeZone to the JVM default zone; TCS works in UTC
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        HikariDataSource dataSource = new HikariDataSource(config);
        Runtime.getRuntime().addShutdownHook(new Thread(dataSource::close, "tc-test-probe-close-" + user));
        return dataSource;
    }

    private static final class Holder {
        static final PostgreSQLContainer POSTGRES = TcContainers.postgres();
        static final JdbcProbe INSTANCE = new JdbcProbe(dataSource(POSTGRES.getUsername(), POSTGRES.getPassword()));
    }
}
