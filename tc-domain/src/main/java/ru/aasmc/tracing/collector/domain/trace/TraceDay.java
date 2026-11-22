package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.time.LocalDate;

/**
 * UTC calendar day under which a trace's summary row is filed: the partition key of {@code trace_summary} and
 * {@code trace_digest} (05 §3.3). The day is fixed when the trace is first seen ({@code TraceDayResolver}).
 *
 * <p>Invariants: {@code value} is not null. Ordered by date.
 *
 * @param value the UTC date
 */
public record TraceDay(LocalDate value) implements Comparable<TraceDay> {

    /** UTC date of an epoch-nanosecond timestamp. */
    public static TraceDay ofEpochNanos(long epochNanos) {
        throw new UnsupportedOperationException();
    }

    /** UTC date of an instant. */
    public static TraceDay of(Instant instant) {
        throw new UnsupportedOperationException();
    }

    /** {@code [value 00:00Z, value + 1 day 00:00Z)}. */
    public TimeRange range() {
        throw new UnsupportedOperationException();
    }

    public TraceDay plusDays(long days) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(TraceDay other) {
        throw new UnsupportedOperationException();
    }
}
