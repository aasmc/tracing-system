package ru.aasmc.tracing.testinfra;

import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Creates the partitions a test needs (09 §0.3 T10). Partitions use the production naming convention and bounds of
 * 04 §8.1 and the CREATE + ATTACH pattern of 04 §8.2, and are owned by {@code tc_owner}.
 *
 * <p>Phase-0 fallback: the SQL is built here. From Phase 2 on this class delegates to the production
 * {@code PartitionDdl} (09 item 2d), so tests and production share one implementation.
 */
public final class TestPartitions {

    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,40}");
    private static final DateTimeFormatter HOUR =
            DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyyMM");
    private static final String SCHEMA = "tc";

    private final JdbcTemplate jdbc;

    private TestPartitions(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static TestPartitions onTcPostgres() {
        return new TestPartitions(JdbcProbe.superuser().jdbc());
    }

    /** Hourly partitions {@code <table>_pYYYYMMDDHH} covering {@code [from, to)}, truncated to whole hours. */
    public void ensureHourly(String table, Instant from, Instant to) {
        requireIdentifier(table);
        for (Instant h = from.truncatedTo(ChronoUnit.HOURS); h.isBefore(to); h = h.plus(1, ChronoUnit.HOURS)) {
            ensure(table, table + "_p" + HOUR.format(h), timestamptz(h), timestamptz(h.plus(1, ChronoUnit.HOURS)));
        }
    }

    /**
     * Daily partitions {@code <table>_pYYYYMMDD} covering {@code [from, toExclusive)}. The bound literal follows the
     * partition key type: {@code date} for {@code trace_summary}, UTC midnight for {@code timestamptz} keys.
     */
    public void ensureDaily(String table, LocalDate from, LocalDate toExclusive) {
        requireIdentifier(table);
        boolean dateKey = "date".equals(partitionKeyType(table));
        for (LocalDate d = from; d.isBefore(toExclusive); d = d.plusDays(1)) {
            String lower = dateKey
                    ? d.toString()
                    : timestamptz(d.atStartOfDay(ZoneOffset.UTC).toInstant());
            String upper = dateKey
                    ? d.plusDays(1).toString()
                    : timestamptz(d.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant());
            ensure(table, table + "_p" + DAY.format(d), lower, upper);
        }
    }

    /** Monthly partitions {@code <table>_pYYYYMM} covering {@code [from, toExclusive)}. */
    public void ensureMonthly(String table, YearMonth from, YearMonth toExclusive) {
        requireIdentifier(table);
        for (YearMonth m = from; m.isBefore(toExclusive); m = m.plusMonths(1)) {
            ensure(
                    table,
                    table + "_p" + MONTH.format(m),
                    timestamptz(m.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant()),
                    timestamptz(m.plusMonths(1)
                            .atDay(1)
                            .atStartOfDay(ZoneOffset.UTC)
                            .toInstant()));
        }
    }

    /** Names of the partitions currently attached to {@code tc.<table>}, sorted. */
    public List<String> attached(String table) {
        requireIdentifier(table);
        return jdbc.queryForList("""
                SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                WHERE i.inhparent = to_regclass(?) ORDER BY c.relname
                """, String.class, SCHEMA + "." + table);
    }

    private void ensure(String parent, String partition, String lower, String upper) {
        requireIdentifier(parent);
        requireIdentifier(partition);
        if (attached(parent).contains(partition)) {
            return;
        }
        String create = "CREATE TABLE IF NOT EXISTS %1$s.%2$s (LIKE %1$s.%3$s INCLUDING DEFAULTS INCLUDING CONSTRAINTS"
                        .formatted(SCHEMA, partition, parent)
                + " INCLUDING STORAGE INCLUDING COMPRESSION)";
        String attach = "ALTER TABLE %1$s.%2$s ATTACH PARTITION %1$s.%3$s FOR VALUES FROM ('%4$s') TO ('%5$s')"
                .formatted(SCHEMA, parent, partition, lower, upper);
        jdbc.execute((ConnectionCallback<Void>) connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET ROLE tc_owner");
                try {
                    statement.execute(create);
                    statement.execute(attach);
                } finally {
                    statement.execute("RESET ROLE");
                }
            }
            return null;
        });
    }

    private String partitionKeyType(String table) {
        requireIdentifier(table);
        List<String> types = jdbc.queryForList("""
                SELECT format_type(a.atttypid, a.atttypmod)
                FROM pg_partitioned_table p
                JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0]
                WHERE p.partrelid = to_regclass(?)
                """, String.class, SCHEMA + "." + table);
        if (types.isEmpty()) {
            throw new IllegalArgumentException(SCHEMA + "." + table + " is not a partitioned table");
        }
        return types.getFirst();
    }

    private static String timestamptz(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDateTime() + "+00";
    }

    private static void requireIdentifier(String name) {
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Not a plain identifier: " + name);
        }
    }
}
