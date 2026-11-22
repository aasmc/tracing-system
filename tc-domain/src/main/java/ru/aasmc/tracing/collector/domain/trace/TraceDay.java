package ru.aasmc.tracing.collector.domain.trace;

import java.time.LocalDate;

/**
 * The UTC day a trace belongs to: the day its earliest span started. Trace tables are partitioned by it.
 *
 * @param value the UTC date
 */
public record TraceDay(LocalDate value) implements Comparable<TraceDay> {

    /** The UTC day of an epoch-nanosecond time. */
    public static TraceDay ofEpochNanos(long epochNanos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(TraceDay other) {
        throw new UnsupportedOperationException();
    }
}
