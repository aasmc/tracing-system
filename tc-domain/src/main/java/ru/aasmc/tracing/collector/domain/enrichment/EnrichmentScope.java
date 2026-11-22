package ru.aasmc.tracing.collector.domain.enrichment;

/** Whether an enrichment attaches to the trace or to one span (05 §4.7.1, V10). */
public enum EnrichmentScope {
    TRACE,
    SPAN
}
