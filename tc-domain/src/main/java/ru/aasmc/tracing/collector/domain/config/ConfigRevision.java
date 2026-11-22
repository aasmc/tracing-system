package ru.aasmc.tracing.collector.domain.config;

/**
 * Monotonic counter bumped by the control plane in the same transaction as any config change (05 §1, §4.12).
 * Invariant: {@code value >= 0}.
 *
 * @param value the revision
 */
public record ConfigRevision(long value) implements Comparable<ConfigRevision> {

    public ConfigRevision next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(ConfigRevision other) {
        throw new UnsupportedOperationException();
    }
}
