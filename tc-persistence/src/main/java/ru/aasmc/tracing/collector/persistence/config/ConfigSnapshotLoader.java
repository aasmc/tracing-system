package ru.aasmc.tracing.collector.persistence.config;

import java.util.Map;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Loads the whole configuration as one {@link ConfigSnapshot}.
 *
 * <p>The parts are read by several statements. A configuration change committed between two of them would leave the
 * snapshot half old and half new, so all statements run in one {@code REPEATABLE READ} transaction: each of them sees
 * the database as it was when the first one started. The isolation level is set by a statement inside the
 * transaction rather than on the JDBC connection, because the connection setting lasts for the whole database
 * session, which PgBouncer in transaction mode shares between clients.
 */
@Component
public class ConfigSnapshotLoader {

    private static final String REPEATABLE_READ = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";

    private final NamedParameterJdbcTemplate jdbc;
    private final ConfigRevisionRepository revisions;
    private final RetentionTiersRepository retentionTiers;

    public ConfigSnapshotLoader(
            NamedParameterJdbcTemplate jdbc,
            ConfigRevisionRepository revisions,
            RetentionTiersRepository retentionTiers) {
        this.jdbc = jdbc;
        this.revisions = revisions;
        this.retentionTiers = retentionTiers;
    }

    /** The configuration at the latest committed revision. */
    @Transactional(readOnly = true)
    public ConfigSnapshot load() {
        jdbc.update(REPEATABLE_READ, Map.of());
        return new ConfigSnapshot(revisions.current(), retentionTiers.current());
    }
}
