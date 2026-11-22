package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * What {@link EnrichmentSourceDefinition#extract} made of one enrichment event. The caller handles every case with an
 * exhaustive {@code switch}, so a new kind of outcome cannot be ignored silently.
 */
public sealed interface Extraction {

    /**
     * The event adds attributes to a trace or one of its spans.
     *
     * @param target the trace, and the span for SPAN scope; the span is {@code null} for TRACE scope
     * @param attributes the mapped values in mapping order; mappings whose optional value was missing are absent
     */
    record Enriched(TraceContext target, Attributes attributes) implements Extraction {}

    /** The event names no trace; the source's settings decide whether it is dropped or dead-lettered. */
    record NoTraceContext() implements Extraction {}

    /**
     * The event cannot be applied and goes to the dead-letter topic.
     *
     * @param reason why
     * @param targetKey the attribute whose mapping failed
     */
    record Rejected(ExtractionFailure reason, AttributeKey targetKey) implements Extraction {}
}
