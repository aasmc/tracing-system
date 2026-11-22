package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/** Ids for tests, built from hex without the production parsers (which are under test). */
public final class DomainIds {

    private DomainIds() {}

    public static TraceId traceId(String hex32) {
        return new TraceId(
                Long.parseUnsignedLong(hex32.substring(0, 16), 16), Long.parseUnsignedLong(hex32.substring(16), 16));
    }

    public static SpanId spanId(String hex16) {
        return new SpanId(Long.parseUnsignedLong(hex16, 16));
    }

    /** Version-7 UUIDs with {@code timestamp} and an increasing counter, so they are distinct and ordered. */
    public static Supplier<UUID> uuidV7Sequence(Instant timestamp) {
        AtomicLong counter = new AtomicLong();
        long msb = (timestamp.toEpochMilli() << 16) | 0x7000L;
        return () -> new UUID(msb, 0x8000_0000_0000_0000L | counter.incrementAndGet());
    }
}
