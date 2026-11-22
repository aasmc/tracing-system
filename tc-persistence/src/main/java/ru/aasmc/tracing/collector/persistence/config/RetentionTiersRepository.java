package ru.aasmc.tracing.collector.persistence.config;

import java.util.Map;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.aasmc.tracing.collector.domain.config.RetentionTiers;

/**
 * How long trace data is kept, stored as the retention of each data table. Every FULL table (the full trace data)
 * keeps its rows {@code fullDataDays}. Every DIGEST table keeps its rows {@code fullDataDays + digestDays}: its
 * retention is the total age of the data, because the digest is written when the full data reaches
 * {@code fullDataDays}.
 */
@Repository
public class RetentionTiersRepository {

    private static final String SELECT_TIERS = """
            SELECT
                (SELECT extract(epoch FROM max(retention)) / 86400 FROM tc.retention_policy WHERE tier = 'FULL')
                    AS full_data_days,
                (SELECT extract(epoch FROM max(retention)) / 86400 FROM tc.retention_policy WHERE tier = 'DIGEST')
                    AS total_days
            """;
    private static final String UPDATE_TIER = """
            UPDATE tc.retention_policy SET retention = make_interval(days => :days)
            WHERE tier = :tier
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public RetentionTiersRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The current tiers. All tables of a tier have the same retention, because {@link #update} sets them together. */
    public RetentionTiers current() {
        return jdbc.queryForObject(SELECT_TIERS, Map.of(), (row, rowNumber) -> {
            int fullDataDays = row.getInt("full_data_days");
            return new RetentionTiers(fullDataDays, row.getInt("total_days") - fullDataDays);
        });
    }

    /**
     * Sets the retention of every table to {@code tiers}. Like every configuration change it must run in a
     * transaction that also increments the configuration revision.
     *
     * @throws org.springframework.transaction.IllegalTransactionStateException if no transaction is active
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void update(RetentionTiers tiers) {
        jdbc.update(UPDATE_TIER, Map.of("tier", "FULL", "days", tiers.fullDataDays()));
        jdbc.update(UPDATE_TIER, Map.of("tier", "DIGEST", "days", tiers.fullDataDays() + tiers.digestDays()));
    }
}
