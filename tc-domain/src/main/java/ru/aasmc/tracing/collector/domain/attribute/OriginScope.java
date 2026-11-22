package ru.aasmc.tracing.collector.domain.attribute;

/** Which origins an indexed attribute accepts (05 §4.9). */
public enum OriginScope {
    /** Any origin except DERIVED (DERIVED is accepted only for built-in keys). */
    ANY,
    SPAN,
    RESOURCE,
    ENRICHMENT
}
