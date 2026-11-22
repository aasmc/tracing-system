package ru.aasmc.tracing.collector.domain.enrichment;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.trace.KafkaCoordinates;
import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * One attribute contributed to a trace by one enrichment event; append-only, never touches {@code tc.span} (05
 * §4.8, §7.1). Idempotency key: {@code (source_id, kafka_partition, kafka_offset, key_id, observed_at)}.
 *
 * <p>Invariants: trace id, key, value and coordinates not null; {@code sourceRevision >= 1} ({@code
 * INVALID_REVISION}).
 *
 * @param traceId the trace
 * @param spanId the span for SPAN scope, {@code null} for TRACE scope
 * @param sourceId the source
 * @param sourceRevision revision that produced the value
 * @param key attribute key
 * @param value attribute value
 * @param source record coordinates
 */
public record EnrichmentRecord(
        TraceId traceId,
        SpanId spanId,
        EnrichmentSourceId sourceId,
        int sourceRevision,
        AttributeKey key,
        AttributeValue value,
        KafkaCoordinates source) {

    /** The Kafka record timestamp: deterministic on replay, part of the unique key. */
    public Instant observedAt() {
        throw new UnsupportedOperationException();
    }
}
