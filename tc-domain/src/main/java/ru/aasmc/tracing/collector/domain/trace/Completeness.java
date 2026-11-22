package ru.aasmc.tracing.collector.domain.trace;

import java.time.Duration;
import java.time.Instant;

/**
 * Completeness hint for the UI and search consumers; never a gate and never persisted (05 §4.5).
 *
 * @param state RECEIVING or SETTLED
 * @param rootless {@code true} while no root span is stored
 * @param orphanSpans spans whose parent is not stored
 */
public record Completeness(CompletenessState state, boolean rootless, int orphanSpans) {

    /** Default of {@code tc.query.trace-settle-after}. */
    public static final Duration DEFAULT_SETTLE_AFTER = Duration.ofMinutes(2);

    /** SETTLED iff {@code summary.lastIngestedAt + settleAfter < now}. */
    public static Completeness of(TraceSummary summary, int orphanSpans, Instant now, Duration settleAfter) {
        throw new UnsupportedOperationException();
    }
}
