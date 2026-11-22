package ru.aasmc.tracing.testinfra.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.aasmc.tracing.testinfra.JdbcProbe;
import ru.aasmc.tracing.testinfra.TestIds;
import ru.aasmc.tracing.testinfra.TestPartitions;

/** The Phase-0 fallback of {@link TestPartitions} against throw-away partitioned tables (04 §8.1 naming). */
@InfraIT
class TestPartitionsIT {

    private final JdbcTemplate superuser = JdbcProbe.superuser().jdbc();
    private final TestPartitions partitions = TestPartitions.onTcPostgres();

    @Test
    @DisplayName("hourly partitions follow <table>_pYYYYMMDDHH, are owned by tc_owner, and creation is idempotent")
    void given_hourlyParent_when_ensureHourlyTwice_then_threePartitionsOwnedByTcOwner() {
        String table = parent("timestamptz");
        Instant from = Instant.parse("2026-11-21T22:15:00Z");
        Instant to = Instant.parse("2026-11-22T01:00:00Z");

        partitions.ensureHourly(table, from, to);
        partitions.ensureHourly(table, from, to);

        assertThat(partitions.attached(table))
                .containsExactly(table + "_p2026112122", table + "_p2026112123", table + "_p2026112200");
        assertThat(JdbcProbe.superuser().tableOwner("tc", table + "_p2026112123"))
                .isEqualTo("tc_owner");
        superuser.update("INSERT INTO tc." + table + " (k) VALUES ('2026-11-21T23:59:59.999999Z')");
        assertThat(JdbcProbe.superuser().scalar(Long.class, "SELECT count(*) FROM tc." + table + "_p2026112123"))
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("daily partitions use date bounds for a date key and UTC midnight for a timestamptz key")
    void given_dateAndTimestamptzParents_when_ensureDaily_then_boundsMatchKeyType() {
        String dateTable = parent("date");
        String tsTable = parent("timestamptz");

        partitions.ensureDaily(dateTable, LocalDate.parse("2026-11-21"), LocalDate.parse("2026-11-23"));
        partitions.ensureDaily(tsTable, LocalDate.parse("2026-11-21"), LocalDate.parse("2026-11-22"));

        assertThat(partitions.attached(dateTable)).containsExactly(dateTable + "_p20261121", dateTable + "_p20261122");
        assertThat(bound(dateTable + "_p20261121")).isEqualTo("FOR VALUES FROM ('2026-11-21') TO ('2026-11-22')");
        assertThat(bound(tsTable + "_p20261121"))
                .isEqualTo("FOR VALUES FROM ('2026-11-21 00:00:00+00') TO ('2026-11-22 00:00:00+00')");
    }

    @Test
    @DisplayName("monthly partitions follow <table>_pYYYYMM")
    void given_monthlyParent_when_ensureMonthly_then_calendarMonthBounds() {
        String table = parent("timestamptz");

        partitions.ensureMonthly(table, YearMonth.parse("2026-12"), YearMonth.parse("2027-02"));

        assertThat(partitions.attached(table)).containsExactly(table + "_p202612", table + "_p202701");
        assertThat(bound(table + "_p202612"))
                .isEqualTo("FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')");
    }

    @Test
    @DisplayName("names that are not plain identifiers are rejected before any SQL is built")
    void given_injectionAttempt_when_ensureHourly_then_rejected() {
        assertThatThrownBy(() -> partitions.ensureHourly("span; DROP SCHEMA tc", Instant.EPOCH, Instant.EPOCH))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private String parent(String keyType) {
        String table = TestIds.name("tp").replace('-', '_').toLowerCase(Locale.ROOT);
        superuser.execute("CREATE TABLE tc." + table + " (k " + keyType + " NOT NULL) PARTITION BY RANGE (k)");
        superuser.execute("ALTER TABLE tc." + table + " OWNER TO tc_owner");
        return table;
    }

    private String bound(String partition) {
        return JdbcProbe.superuser()
                .scalar(
                        String.class,
                        "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = to_regclass(?)",
                        "tc." + partition);
    }
}
