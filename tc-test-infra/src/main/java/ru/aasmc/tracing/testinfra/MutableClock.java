package ru.aasmc.tracing.testinfra;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A {@link Clock} that tests set and advance (09 §0.3 T10). Production code reads time only from the {@code Clock}
 * bean; {@link ClockTestConfiguration} replaces it with this one. Thread-safe. Always UTC, like the system (04 §8.1).
 */
public final class MutableClock extends Clock {

    private volatile Instant instant;

    public MutableClock(Instant initial) {
        this.instant = Objects.requireNonNull(initial, "initial");
    }

    /** A clock frozen at the current system time. */
    public static MutableClock startingNow() {
        return new MutableClock(Instant.now());
    }

    public void setInstant(Instant newInstant) {
        this.instant = Objects.requireNonNull(newInstant, "newInstant");
    }

    public synchronized Instant advance(Duration duration) {
        instant = instant.plus(Objects.requireNonNull(duration, "duration"));
        return instant;
    }

    /** Back to the current system time; for {@code @BeforeEach} of tests that move the clock. */
    public void reset() {
        setInstant(Instant.now());
    }

    @Override
    public Instant instant() {
        return instant;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (ZoneOffset.UTC.equals(zone)) {
            return this;
        }
        throw new UnsupportedOperationException("TCS works in UTC only");
    }
}
