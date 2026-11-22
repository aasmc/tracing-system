package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * The fact "trace T carries value V of correlation key K".
 *
 * <p>{@code value} is 1..{@value #MAX_VALUE_LENGTH} characters and not blank; trace id and time are set
 * ({@code INVALID_CORRELATION_VALUE}).
 *
 * @param observedAt when the value was seen: the span start, or the event time for enrichment
 * @param correlationTypeId the type
 * @param correlationKeyId the key
 * @param value the value
 * @param traceId the trace
 */
public record CorrelationObservation(
        Instant observedAt, int correlationTypeId, int correlationKeyId, String value, TraceId traceId) {

    public static final int MAX_VALUE_LENGTH = 512;
}
