package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.List;

/**
 * Content of one source revision, stored as {@code enrichment_source_revision.definition} jsonb (05 §4.7.1).
 *
 * <p>Invariants: 1..{@link #MAX_MAPPINGS} field mappings ({@code INVALID_MAPPING}; deviation from 05, which allowed
 * 64, to match V5); target keys unique ({@code DUPLICATE_TARGET_KEY}); SPAN scope requires a strategy that
 * {@link TraceContextExtraction#yieldsSpanId() yields a span id} ({@code INVALID_SCOPE}); {@code traceContext} and
 * {@code scope} not null; nulls default to JSON, LATEST, DLQ and {@link ConsumerOptions#DEFAULT}; mappings are an
 * immutable copy.
 *
 * @param payloadFormat payload format
 * @param traceContext trace-context extraction
 * @param scope TRACE or SPAN
 * @param filter optional event filter, or {@code null}
 * @param startFrom start position for a new consumer group
 * @param onMissingTraceContext DLQ or DROP
 * @param consumer consumer overrides
 * @param fieldMappings field mappings in declaration order
 */
public record SourceDefinition(
        PayloadFormat payloadFormat,
        TraceContextExtraction traceContext,
        EnrichmentScope scope,
        EventFilter filter,
        StartFrom startFrom,
        OnMissingTraceContext onMissingTraceContext,
        ConsumerOptions consumer,
        List<FieldMapping> fieldMappings) {

    /** 03 §5.4 V5. */
    public static final int MAX_MAPPINGS = 50;
}
