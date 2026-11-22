package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.List;

/**
 * What an enrichment source does with each event: find the trace, then map fields to attributes.
 *
 * <p>Rules: {@code traceContextSource} and {@code scope} are set; SPAN scope needs a trace-context source that
 * {@link TraceContextSource#providesSpanId() yields a span id} ({@code INVALID_SCOPE}); 1..{@value #MAX_MAPPINGS}
 * mappings ({@code INVALID_MAPPING}) with distinct target keys ({@code DUPLICATE_TARGET_KEY}); the list is an
 * immutable copy.
 *
 * @param traceContextSource where the trace is found in an event
 * @param scope trace or span
 * @param fieldMappings the mappings, in order
 */
public record EnrichmentSourceDefinition(
        TraceContextSource traceContextSource, EnrichmentScope scope, List<FieldMapping> fieldMappings) {

    public static final int MAX_MAPPINGS = 50;
}
