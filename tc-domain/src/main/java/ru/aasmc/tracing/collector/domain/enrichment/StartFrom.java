package ru.aasmc.tracing.collector.domain.enrichment;

/** {@code auto.offset.reset} for a consumer group without committed offsets (05 §4.7.1). */
public enum StartFrom {
    LATEST,
    EARLIEST
}
