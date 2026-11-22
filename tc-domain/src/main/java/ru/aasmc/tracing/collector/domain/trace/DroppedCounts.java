package ru.aasmc.tracing.collector.domain.trace;

/**
 * Counts of attributes, events and links the producer dropped (05 §3.6, §6.2).
 *
 * <p>Invariants: all counts {@code >= 0}. Stored as {@code span.dropped_counts} jsonb only if {@link #any()}.
 *
 * @param attributes dropped attributes
 * @param events dropped events
 * @param links dropped links
 */
public record DroppedCounts(int attributes, int events, int links) {

    public static final DroppedCounts NONE = new DroppedCounts(0, 0, 0);

    /** {@code true} if any count is positive; otherwise the column is NULL. */
    public boolean any() {
        throw new UnsupportedOperationException();
    }
}
