package ru.aasmc.tracing.collector.persistence.config;

import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.aasmc.tracing.collector.domain.config.RetentionTiers;

/** What the configuration tests share. */
final class ConfigTestSupport {

    /** The tiers the test schema is installed with. Tests that change them restore them afterwards. */
    static final RetentionTiers INSTALLED_TIERS = new RetentionTiers(7, 90);

    private ConfigTestSupport() {}

    /** Transactions on the connections of {@code dataSource}, e.g. one logged in as a service's role. */
    static TransactionTemplate transactionsOf(DataSource dataSource) {
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }
}
