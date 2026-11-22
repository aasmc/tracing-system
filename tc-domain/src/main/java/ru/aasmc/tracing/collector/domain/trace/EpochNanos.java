package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;

/**
 * Conversions of span times. Producers send nanoseconds since the Unix epoch as decimal strings; PostgreSQL
 * {@code timestamptz} keeps microseconds.
 */
public final class EpochNanos {

    private EpochNanos() {}

    /**
     * Parses a decimal string exactly, never through {@code double} (which would lose the last digits).
     *
     * @throws NumberFormatException if the text is not a decimal {@code long}
     */
    public static long parse(String text) {
        throw new UnsupportedOperationException();
    }

    /** The exact instant; negative values are before 1970. */
    public static Instant toInstant(long epochNanos) {
        throw new UnsupportedOperationException();
    }

    /** The instant cut (not rounded) down to whole microseconds: the value PostgreSQL stores. */
    public static Instant toInstantTruncatedToMicros(long epochNanos) {
        throw new UnsupportedOperationException();
    }
}
