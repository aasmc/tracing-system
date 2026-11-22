package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Conversions of span times. Producers send nanoseconds since the Unix epoch as decimal strings; PostgreSQL
 * {@code timestamptz} keeps microseconds.
 */
public final class EpochNanos {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private EpochNanos() {}

    /**
     * Parses a decimal string exactly, never through {@code double} (which would lose the last digits).
     *
     * @throws NumberFormatException if the text is not a decimal {@code long}
     */
    public static long parse(String text) {
        return Long.parseLong(text);
    }

    /** The exact instant; negative values are before 1970. */
    public static Instant toInstant(long epochNanos) {
        return Instant.ofEpochSecond(
                Math.floorDiv(epochNanos, NANOS_PER_SECOND), Math.floorMod(epochNanos, NANOS_PER_SECOND));
    }

    /** The instant cut (not rounded) down to whole microseconds: the value PostgreSQL stores. */
    public static Instant toInstantTruncatedToMicros(long epochNanos) {
        return toInstant(epochNanos).truncatedTo(ChronoUnit.MICROS);
    }
}
