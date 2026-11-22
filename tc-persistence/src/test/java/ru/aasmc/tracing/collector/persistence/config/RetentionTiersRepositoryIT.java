package ru.aasmc.tracing.collector.persistence.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.INSTALLED_TIERS;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.transactionsOf;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
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
class RetentionTiersRepositoryIT {

    private final JdbcProbe db = JdbcProbe.superuser();

    @Autowired
    RetentionTiersRepository retentionTiers;

    @Autowired
    TransactionTemplate transaction;

    @AfterEach
    void restoreInstalledTiers() {
        transaction.executeWithoutResult(_ -> retentionTiers.update(INSTALLED_TIERS));
    }

    @Test
    @DisplayName("""
            Given the installed schema,
            when the tiers are read,
            then they are the full-data and digest days it was installed with""")
    void given_installedSchema_when_read_then_installedTiers() {
        assertThat(retentionTiers.current()).isEqualTo(INSTALLED_TIERS);
    }

    @Test
    @DisplayName("""
            Given new tiers of 3 full-data days and 30 digest days,
            when they are stored,
            then every FULL table keeps 3 days, every DIGEST table 33 days (the total age),
            and the tiers read back unchanged""")
    void given_newTiers_when_updated_then_fullAndTotalRetentionStored() {
        RetentionTiers changed = new RetentionTiers(3, 30);

        transaction.executeWithoutResult(_ -> retentionTiers.update(changed));

        assertThat(db.rows("""
                        SELECT tier, extract(day FROM retention)::int AS days, count(*)::int AS tables
                        FROM tc.retention_policy GROUP BY tier, retention ORDER BY tier"""))
                .extracting(row -> List.of(row.get("tier"), row.get("days"), row.get("tables")))
                .containsExactly(List.of("DIGEST", 33, 2), List.of("FULL", 3, 7));
        assertThat(retentionTiers.current()).isEqualTo(changed);
    }

    @Test
    @DisplayName("""
            Given no active transaction,
            when the tiers are changed,
            then it is refused and the tiers are unchanged,
            so that no change can be committed without incrementing the configuration revision""")
    void given_noTransaction_when_updated_then_refused() {
        assertThatExceptionOfType(IllegalTransactionStateException.class)
                .isThrownBy(() -> retentionTiers.update(new RetentionTiers(3, 30)));

        assertThat(retentionTiers.current()).isEqualTo(INSTALLED_TIERS);
    }

    @Test
    @DisplayName("""
            Given the control plane's database role,
            when it changes the tiers,
            then its privileges are enough""")
    void given_adminRole_when_updated_then_allowed() {
        DataSource admin = JdbcProbe.loggedInAs("tc_admin").dataSource();
        RetentionTiersRepository asAdmin = new RetentionTiersRepository(new NamedParameterJdbcTemplate(admin));
        RetentionTiers changed = new RetentionTiers(3, 30);

        RetentionTiers seenInTransaction = transactionsOf(admin).execute(status -> {
            status.setRollbackOnly();
            asAdmin.update(changed);
            return asAdmin.current();
        });

        assertThat(seenInTransaction).isEqualTo(changed);
    }
}
