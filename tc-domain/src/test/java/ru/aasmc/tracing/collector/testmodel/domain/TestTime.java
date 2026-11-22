package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Instant;

/** Epoch-nanosecond conversions for tests, independent of the production {@code EpochNanos}. */
public final class TestTime {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private TestTime() {}

    public static long epochNanos(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), NANOS_PER_SECOND), instant.getNano());
    }

    public static long epochNanos(String isoInstant) {
        return epochNanos(Instant.parse(isoInstant));
    }

    public static Instant instantOf(long epochNanos) {
        return Instant.ofEpochSecond(
                Math.floorDiv(epochNanos, NANOS_PER_SECOND), Math.floorMod(epochNanos, NANOS_PER_SECOND));
    }
}
