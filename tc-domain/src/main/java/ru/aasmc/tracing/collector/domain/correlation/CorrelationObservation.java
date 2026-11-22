package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.attribute.AttributeOrigin;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * One fact "trace X carries value V for correlation key K": a row of the append-only {@code tc.trace_correlation}
 * (05 §4.11).
 *
 * <p>Invariants ({@code INVALID_CORRELATION_VALUE}): {@code value} 1..{@link #MAX_VALUE_LENGTH} characters, not
 * blank; trace id, time and origin not null.
 *
 * @param id identity value ({@code 0} before insert)
 * @param observedAt span start (SPAN/RESOURCE origin) or Kafka timestamp (ENRICHMENT)
 * @param correlationTypeId the type
 * @param correlationKeyId the {@code correlation_key.id}
 * @param value the correlation value
 * @param traceId the trace
 * @param origin where the value came from
 */
public record CorrelationObservation(
        long id,
        Instant observedAt,
        int correlationTypeId,
        int correlationKeyId,
        String value,
        TraceId traceId,
        AttributeOrigin origin) {

    public static final int MAX_VALUE_LENGTH = 512;
}
