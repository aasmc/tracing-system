package ru.aasmc.tracing.collector.persistence.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.INSTALLED_TIERS;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.transactionsOf;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import ru.aasmc.tracing.collector.domain.config.RetentionTiers;
import ru.aasmc.tracing.collector.persistence.PersistenceIT;
import ru.aasmc.tracing.testinfra.JdbcProbe;

@PersistenceIT
class ConfigRevisionRepositoryIT {

    @Autowired
    ConfigRevisionRepository revisions;

    @Autowired
    RetentionTiersRepository retentionTiers;

    @Autowired
    TransactionTemplate transaction;

    @Test
    @DisplayName("""
            Given a transaction that increments the revision twice,
            when it commits,
            then the two increments return consecutive revisions and the last one is current""")
    void given_twoIncrements_when_committed_then_consecutiveAndCurrent() {
        long before = revisions.current();

        List<Long> incremented = transaction.execute(_ -> List.of(revisions.increment(), revisions.increment()));

        assertThat(incremented).containsExactly(before + 1, before + 2);
        assertThat(revisions.current()).isEqualTo(before + 2);
    }

    @Test
    @DisplayName("""
            Given a transaction that changes the retention tiers and increments the revision,
            when it is rolled back,
            then neither the tiers nor the revision have changed""")
    void given_changeWithIncrement_when_rolledBack_then_bothUnchanged() {
        long before = revisions.current();

        transaction.executeWithoutResult(status -> {
            retentionTiers.update(new RetentionTiers(3, 30));
            revisions.increment();
            status.setRollbackOnly();
        });

        assertThat(revisions.current()).isEqualTo(before);
        assertThat(retentionTiers.current()).isEqualTo(INSTALLED_TIERS);
    }

    @Test
    @DisplayName("""
            Given no active transaction,
            when the revision is incremented,
            then it is refused and the revision is unchanged""")
    void given_noTransaction_when_incremented_then_refused() {
        long before = revisions.current();

        assertThatExceptionOfType(IllegalTransactionStateException.class).isThrownBy(revisions::increment);

        assertThat(revisions.current()).isEqualTo(before);
    }

    @Test
    @DisplayName("""
            Given the control plane's database role,
            when it increments the revision,
            then its privileges are enough""")
    void given_adminRole_when_incremented_then_allowed() {
        DataSource admin = JdbcProbe.loggedInAs("tc_admin").dataSource();
        ConfigRevisionRepository asAdmin = new ConfigRevisionRepository(new NamedParameterJdbcTemplate(admin));

        Long incremented = transactionsOf(admin).execute(status -> {
            status.setRollbackOnly();
            return asAdmin.increment();
        });

        assertThat(incremented).isPositive();
    }
}
