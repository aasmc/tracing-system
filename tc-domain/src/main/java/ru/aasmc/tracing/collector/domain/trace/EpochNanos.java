package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;

/**
 * Conversions of wire times (05 §3.3). The wire sends epoch nanoseconds as decimal strings; PostgreSQL
 * {@code timestamptz} has microsecond precision (ADR-10).
 */
public final class EpochNanos {

    private EpochNanos() {}

    /**
     * Parses the decimal string with {@link Long#parseLong(String)}, never through {@code double}.
     *
     * @throws NumberFormatException if the text is not a decimal long
     */
    public static long parse(String text) {
        throw new UnsupportedOperationException();
    }

    /** Exact instant, including negative epoch values (floor division). */
    public static Instant toInstant(long epochNanos) {
        throw new UnsupportedOperationException();
    }

    /** Instant truncated (not rounded) to microseconds: the value written to {@code timestamptz}. */
    public static Instant toMicros(long epochNanos) {
        throw new UnsupportedOperationException();
    }

    /**
     * Epoch nanoseconds of an instant.
     *
     * @throws ArithmeticException if the instant is outside the {@code long} nanosecond range (years 1677–2262)
     */
    public static long of(Instant instant) {
        throw new UnsupportedOperationException();
    }
}
