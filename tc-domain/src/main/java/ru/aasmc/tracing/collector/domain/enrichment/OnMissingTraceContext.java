package ru.aasmc.tracing.collector.domain.enrichment;

/** What to do with an event that carries no usable trace context (05 §4.7.1). */
public enum OnMissingTraceContext {
    /** Copy to {@code tc-enrichment.dlq} with reason {@code NO_TRACE_CONTEXT}. */
    DLQ,
    /** Count and skip. */
    DROP
}
