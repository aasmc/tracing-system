package ru.aasmc.tracing.collector.domain.config;

import java.time.Duration;

/** Partition granularity of a partitioned table (05 §4.12). */
public enum Granularity {
    HOUR,
    DAY,
    MONTH;

    /** Upper bound of one partition's length: 1 h, 1 d, 31 d. */
    public Duration maxPartitionLength() {
        throw new UnsupportedOperationException();
    }
}
