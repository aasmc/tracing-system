package ru.aasmc.tracing.collector.domain.trace;

import java.time.Duration;
import java.time.Instant;

/**
 * Half-open interval {@code [from, to)} of UTC instants (05 §3.3).
 *
 * <p>Invariants: both bounds are non-null and {@code from < to} ({@code INVALID_TIME_RANGE}). Deviation from 05,
 * which allowed an empty range: no caller needs one, and a search window or partition is never empty.
 *
 * @param from inclusive lower bound
 * @param to exclusive upper bound
 */
public record TimeRange(Instant from, Instant to) {

    /** {@code to - from}, always positive. */
    public Duration length() {
        throw new UnsupportedOperationException();
    }

    /** {@code from <= instant < to}. */
    public boolean contains(Instant instant) {
        throw new UnsupportedOperationException();
    }

    /** {@code true} if the two half-open ranges share at least one instant. */
    public boolean overlaps(TimeRange other) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns {@code this} if {@code length() <= max}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code WINDOW_TOO_LARGE} otherwise
     */
    public TimeRange requireMaxLength(Duration max) {
        throw new UnsupportedOperationException();
    }
}
