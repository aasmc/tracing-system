package ru.aasmc.tracing.collector.domain.enrichment;

/** What an enrichment event adds attributes to. */
public enum EnrichmentScope {
    /** The whole trace. */
    TRACE,
    /** One span; needs a trace context that carries a span id. */
    SPAN
}
