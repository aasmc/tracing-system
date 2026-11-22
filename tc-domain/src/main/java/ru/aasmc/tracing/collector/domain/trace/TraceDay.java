package ru.aasmc.tracing.collector.domain.trace;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * The UTC day a trace belongs to: the day its earliest span started. Trace tables are partitioned by it.
 *
 * @param value the UTC date
 */
public record TraceDay(LocalDate value) implements Comparable<TraceDay> {

    /** The UTC day of an epoch-nanosecond time. */
    public static TraceDay ofEpochNanos(long epochNanos) {
        return new TraceDay(LocalDate.ofInstant(EpochNanos.toInstant(epochNanos), ZoneOffset.UTC));
    }

    @Override
    public int compareTo(TraceDay other) {
        return value.compareTo(other.value);
    }
}
