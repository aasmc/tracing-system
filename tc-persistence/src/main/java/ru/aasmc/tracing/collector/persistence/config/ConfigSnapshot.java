package ru.aasmc.tracing.collector.persistence.config;

import java.util.Objects;
import ru.aasmc.tracing.collector.domain.config.RetentionTiers;

/**
 * The configuration as it was at one configuration revision. Every part belongs to that revision: no part reflects a
 * change that another part does not.
 *
 * @param revision the configuration revision
 * @param retentionTiers how long trace data is kept
 */
public record ConfigSnapshot(long revision, RetentionTiers retentionTiers) {

    public ConfigSnapshot {
        Objects.requireNonNull(retentionTiers, "retentionTiers");
    }
}
