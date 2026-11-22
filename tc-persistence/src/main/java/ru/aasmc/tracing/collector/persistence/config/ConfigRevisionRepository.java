package ru.aasmc.tracing.collector.persistence.config;

import java.util.Map;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The configuration revision: one number that every configuration change increments. The services compare it with
 * the revision of the configuration they loaded and reload only when it has changed, so noticing a change costs one
 * single-row query.
 */
@Repository
public class ConfigRevisionRepository {

    private static final String SELECT_REVISION = "SELECT revision FROM tc.config_revision WHERE id = 1";
    private static final String INCREMENT_REVISION = """
            UPDATE tc.config_revision SET revision = revision + 1, updated_at = now()
            WHERE id = 1
            RETURNING revision
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public ConfigRevisionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The current revision. */
    public long current() {
        return jdbc.queryForObject(SELECT_REVISION, Map.of(), Long.class);
    }

    /**
     * Increments the revision and returns the new one. It must run in the transaction that changes the
     * configuration: a service that sees the new revision then also sees the whole change, and a change that is
     * rolled back leaves the revision as it was. The row stays locked until that transaction ends, so concurrent
     * configuration changes run one after the other.
     *
     * @throws org.springframework.transaction.IllegalTransactionStateException if no transaction is active
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public long increment() {
        return jdbc.queryForObject(INCREMENT_REVISION, Map.of(), Long.class);
    }
}
