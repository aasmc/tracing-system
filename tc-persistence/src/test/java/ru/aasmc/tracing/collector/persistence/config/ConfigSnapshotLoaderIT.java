package ru.aasmc.tracing.collector.persistence.config;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.INSTALLED_TIERS;
import static ru.aasmc.tracing.collector.persistence.config.ConfigTestSupport.transactionsOf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import ru.aasmc.tracing.collector.domain.config.RetentionTiers;
import ru.aasmc.tracing.collector.persistence.PersistenceIT;
import ru.aasmc.tracing.testinfra.JdbcProbe;

@PersistenceIT
class ConfigSnapshotLoaderIT {

    private static final int CONCURRENT_CHANGES = 200;

    @Autowired
    ConfigSnapshotLoader snapshots;

    @Autowired
    ConfigRevisionRepository revisions;

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
            when a snapshot is loaded,
            then it holds the current revision and the installed tiers""")
    void given_installedSchema_when_loaded_then_currentRevisionAndTiers() {
        assertThat(snapshots.load()).isEqualTo(new ConfigSnapshot(revisions.current(), INSTALLED_TIERS));
    }

    @Test
    @DisplayName("""
            Given a writer that commits 200 tier changes, each with its revision increment,
            when snapshots are loaded while it runs,
            then every snapshot holds exactly the tiers of its own revision, never those of another""")
    void given_concurrentChanges_when_loaded_then_tiersMatchRevision() {
        long start = revisions.current();
        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            for (int change = 0; change < CONCURRENT_CHANGES; change++) {
                transaction.executeWithoutResult(_ -> retentionTiers.update(tiersOf(revisions.increment())));
            }
        });

        List<ConfigSnapshot> loaded = new ArrayList<>();
        while (!writer.isDone()) {
            loaded.add(snapshots.load());
        }
        writer.join();

        assertThat(loaded)
                .allSatisfy(snapshot -> assertThat(snapshot.retentionTiers())
                        .isEqualTo(snapshot.revision() == start ? INSTALLED_TIERS : tiersOf(snapshot.revision())));
        assertThat(loaded)
                .extracting(ConfigSnapshot::revision)
                .as("the snapshots were loaded while the writer changed the configuration")
                .anyMatch(revision -> revision > start && revision < start + CONCURRENT_CHANGES);
    }

    @Test
    @DisplayName("""
            Given the database role of each service that reads the configuration,
            when it loads a snapshot,
            then its privileges are enough""")
    void given_readerRoles_when_loaded_then_allowed() {
        ConfigSnapshot expected = snapshots.load();

        for (String role : List.of("tc_ingester", "tc_enricher", "tc_query", "tc_maintenance")) {
            DataSource reader = JdbcProbe.loggedInAs(role).dataSource();
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(reader);
            ConfigSnapshotLoader loader = new ConfigSnapshotLoader(
                    jdbc, new ConfigRevisionRepository(jdbc), new RetentionTiersRepository(jdbc));

            ConfigSnapshot loadedByRole = transactionsOf(reader).execute(_ -> loader.load());

            assertThat(loadedByRole).as(role).isEqualTo(expected);
        }
    }

    /** Different tiers for consecutive revisions. */
    private static RetentionTiers tiersOf(long revision) {
        return new RetentionTiers(2 + (int) (revision % 10), (int) (revision % 7));
    }
}
